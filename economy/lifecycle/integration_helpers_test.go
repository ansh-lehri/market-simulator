package lifecycle

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"simulator/connectivity/config"
	"simulator/connectivity/db"
	"simulator/economy/persistence"
)

// Integration tests here exercise lifecycle against the REAL
// persistence.Component (live Postgres + Redis). The point is to prove the
// contract lifecycle depends on end-to-end:
//   - Postgres signed trading_cash round-trip (Option B — negative
//     Available survives OpenAccount → SessionSave → LoadAccount).
//   - Redis snapshot is seeded once at SessionLoad and read back by
//     ReadBalanceSnapshot (the item-16 boundary read).
//   - On-demand leaves NO Postgres footprint at teardown.
//   - The don't-discard-on-failed-save rule: a failed SessionSave RETAINS
//     the Redis snapshot as the last surviving copy.
//   - CrashRecoveryReload restores an evolved snapshot verbatim.
//
// Skipping and cleanup follow the persistence package's conventions
// (integration_helpers_test.go there): missing TEST_DATABASE_URL /
// TEST_REDIS_URL skip cleanly; Redis keys are namespaced per test and
// SCAN+DEL'd on cleanup; Postgres accounts rows are DELETE'd on cleanup by
// account_id (no transaction rollback — the smoke pattern — because we use
// the full pool for symmetry with production, and lifecycle flows do not
// need concurrent goroutines the way the drainer smoke does).
//
// Bring-up matters: the test DB must have Stage-2 migrations applied.

const (
	envTestDBURL    = "TEST_DATABASE_URL"
	envTestRedisURL = "TEST_REDIS_URL"
)

// requirePostgresPool returns a live pgxpool.Pool against TEST_DATABASE_URL
// or skips the test. Pool is closed on test cleanup.
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

// requireRedis returns a live redis.Client and a unique KeyNamespace scoped
// to this test. All keys under that namespace are SCAN+DEL'd on cleanup so
// no key leaks across tests.
func requireRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	url := os.Getenv(envTestRedisURL)
	if url == "" {
		t.Skipf("%s not set — integration test skipped", envTestRedisURL)
	}
	client, err := db.NewRedis(context.Background(), config.RedisConfig{
		URL:         url,
		PingTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open test redis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	safeName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	namespace := fmt.Sprintf("test_life_%s_%d", safeName, time.Now().UnixNano())

	t.Cleanup(func() {
		ctx := context.Background()
		var cursor uint64
		for {
			batch, next, err := client.Scan(ctx, cursor, namespace+":*", 100).Result()
			if err != nil {
				return
			}
			if len(batch) > 0 {
				_ = client.Del(ctx, batch...).Err()
			}
			if next == 0 {
				return
			}
			cursor = next
		}
	})
	return client, namespace
}

// buildLifecycleForIntegration wires a lifecycle.Service against a real
// persistence.Component sharing the given pool + namespaced Redis client.
// Returns the service and the underlying Component so tests can also make
// out-of-band calls (e.g. WriteSnapshot for crash-recovery seeding, direct
// DELETE via the pool for forced-save-failure).
func buildLifecycleForIntegration(pool *pgxpool.Pool, client *redis.Client, namespace string) (*Service, *persistence.Component) {
	cfg := persistence.Config{
		KeyNamespace: namespace,
	}
	component := persistence.NewComponent(pool, client, persistence.IndiaCostAdapter{}, cfg)
	return NewService(component), component
}

// registerAccountCleanup schedules a DELETE FROM accounts on test end so
// each test self-cleans its seeded rows. Runs even if the test fails, so
// no residue is left in l5_test.
func registerAccountCleanup(t *testing.T, pool *pgxpool.Pool, accountIDs ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, accountID := range accountIDs {
			_, _ = pool.Exec(ctx, `DELETE FROM accounts WHERE account_id = $1`, accountID)
		}
	})
}

// uniqueOwnerID builds a test-scoped owner_id that will not collide with
// any other test's OpenAccount call (the uq_accounts_owner constraint
// means two tests using the same owner_id would 23505 the second one).
func uniqueOwnerID(t *testing.T, suffix string) string {
	t.Helper()
	safeName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("%s_%s_%d", safeName, suffix, time.Now().UnixNano())
}
