package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/connectivity/config"
	"simulator/connectivity/db"
)

// Integration tests run against a live TEST database (migrations 0001–0003
// applied). Each test:
//   - Postgres: opens a pool via TEST_DATABASE_URL; seeds its own account
//     rows with a per-test owner_id namespace; cleans those rows up on
//     test end. The Store under test is given the real pool (not a tx)
//     because its production surface is pool-backed — swapping in a tx
//     interface here would hide a bug that only surfaces through the
//     real pool.
//   - Isolation: owner_id is prefixed with the test name + a timestamp,
//     so parallel tests never collide, and cleanup is a single DELETE
//     filtered on that prefix.
//
// If TEST_DATABASE_URL is unset, individual tests skip cleanly.

const (
	envTestDBURL = "TEST_DATABASE_URL"
)

// requirePostgresPool returns a live pgxpool.Pool against TEST_DATABASE_URL
// or skips the test. The pool is closed on test cleanup. Shared across
// seeds within one test; one pool per test is cheap for integration scale.
func requirePostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(envTestDBURL)
	if dsn == "" {
		t.Skipf("%s not set — integration test skipped", envTestDBURL)
	}
	pool, err := db.NewPostgres(context.Background(), config.PostgresConfig{
		DSN:         dsn,
		PingTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open test postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// testOwnerPrefix returns a unique owner_id prefix for this test. Used by
// seedAccount for ownership, and by cleanupOwnersWithPrefix as the DELETE
// filter. Test name + nanoseconds → distinct across parallel + repeat runs.
func testOwnerPrefix(t *testing.T) string {
	t.Helper()
	safeName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("test-%s-%d", safeName, time.Now().UnixNano())
}

// seedAccount INSERTs one accounts row with the given fields. account_id,
// owner_type, owner_id are explicit (the UNIQUE on (owner_type, owner_id)
// is what AccountHandle probes). Cleanup is handled by the matching
// cleanupOwnersWithPrefix in the test's t.Cleanup.
func seedAccount(t *testing.T, pool *pgxpool.Pool, accountID, ownerType, ownerID string, tradingCash, bankCash int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO accounts (account_id, owner_type, owner_id, trading_cash, bank_cash)
		 VALUES ($1, $2, $3, $4, $5)`,
		accountID, ownerType, ownerID, tradingCash, bankCash,
	)
	if err != nil {
		t.Fatalf("seed account %s (%s, %s): %v", accountID, ownerType, ownerID, err)
	}
}

// cleanupOwnersWithPrefix schedules a DELETE on test end that removes
// every accounts row whose owner_id starts with ownerPrefix. Scoped to
// the test's prefix so parallel tests never clobber each other's seeds.
func cleanupOwnersWithPrefix(t *testing.T, pool *pgxpool.Pool, ownerPrefix string) {
	t.Helper()
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(),
			`DELETE FROM accounts WHERE owner_id LIKE $1`,
			ownerPrefix+"%",
		)
		if err != nil {
			t.Logf("cleanup accounts (prefix=%s): %v", ownerPrefix, err)
		}
	})
}
