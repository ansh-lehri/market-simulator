package persistence

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"simulator/connectivity/config"
	"simulator/connectivity/db"
	"simulator/country/india"
)

// Integration tests run against a live TEST database + Redis. Each test:
//   - Postgres: opens ONE conn from the pool, begins an outer tx, and passes
//     that tx as the store's dbConn. Rollback on test end wipes everything.
//     store.withTx internally uses SAVEPOINT (via pgx.Tx.Begin) so nested
//     commits still roll back cleanly with the outer tx.
//   - Redis: allocates a unique KeyNamespace per test; SCAN+DEL on that
//     namespace at test end so no key leaks across tests.
//
// If TEST_DATABASE_URL / TEST_REDIS_URL is unset, individual tests skip
// cleanly. Bring-up matters: the test DB must have the Stage-2 migrations
// applied (schema doc §6 "provisioned by running these migrations against
// the test DATABASE_URL").

const (
	envTestDBURL    = "TEST_DATABASE_URL"
	envTestRedisURL = "TEST_REDIS_URL"

	// envCommitTestData, when set to a truthy value ("1"/"true"/"yes"/"on"),
	// makes beginTestTx COMMIT the outer test transaction on cleanup instead
	// of rolling it back. Rows land in Postgres permanently so a developer
	// can psql in and eyeball them.
	//
	// Off by default (rollback), because the standard tx-rollback pattern
	// keeps CI runs self-cleaning per economy/CLAUDE.md. Turn it on for
	// manual "does this actually work" debugging on a fresh environment.
	//
	// Caution: this LEAVES DATA in the DB. Run against l5_test (never dev
	// data) and clean up manually before the next commit-mode run.
	envCommitTestData = "PERSISTENCE_COMMIT_TEST_DATA"
)

// isTruthy returns true for the standard set of positive env values. Empty
// / unset / "0" / "false" all yield false.
func isTruthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// requirePostgresPool returns a live pgxpool.Pool against TEST_DATABASE_URL
// or skips the test. The pool is closed on test cleanup.
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

// beginTestTx acquires a connection from the pool, begins an OUTER
// transaction on it, and schedules cleanup on test end. The returned tx
// satisfies the store's dbConn interface, so any state the store writes
// through it — including nested savepoints from store.withTx — is
// discarded (default) or committed (PERSISTENCE_COMMIT_TEST_DATA=1) when
// the test finishes.
//
// Default cleanup path: Rollback. The DB is left pristine — the standard
// pattern per economy/CLAUDE.md.
//
// Commit mode (PERSISTENCE_COMMIT_TEST_DATA=1): Commit. Rows LAND in the
// DB so the developer can inspect them via psql. Constraint-violation
// tests leave the tx aborted; the helper falls back to Rollback silently
// there so cleanup never panics.
func beginTestTx(t *testing.T) pgx.Tx {
	t.Helper()
	pool := requirePostgresPool(t)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire test conn: %v", err)
	}
	t.Cleanup(conn.Release)

	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin test tx: %v", err)
	}

	commitMode := isTruthy(os.Getenv(envCommitTestData))
	t.Cleanup(func() {
		ctx := context.Background()
		if !commitMode {
			// Rollback may fail if the tx is in an unusual state; ignore
			// because we just want a clean baseline for the next test.
			_ = tx.Rollback(ctx)
			return
		}
		// Commit mode: try to commit; fall back to rollback if the tx was
		// aborted (constraint-violation tests do this deliberately). Log
		// the outcome so the developer knows what to expect in psql.
		if commitErr := tx.Commit(ctx); commitErr != nil {
			_ = tx.Rollback(ctx)
			t.Logf("%s: commit failed (%v) — rolled back. Expected for tests that trigger constraint violations.",
				envCommitTestData, commitErr)
			return
		}
		t.Logf("%s: outer tx COMMITTED — rows persist in DB for inspection. Clean up before the next run.",
			envCommitTestData)
	})
	return tx
}

// requireRedis returns a live redis.Client and a unique KeyNamespace scoped
// to this test. All keys under that namespace are removed on cleanup.
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

	// Unique namespace: test name (path-safe) + a timestamp. Two tests
	// running back-to-back with the same name still get distinct namespaces.
	safeName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	namespace := fmt.Sprintf("test_%s_%d", safeName, time.Now().UnixNano())

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

// seedAccount INSERTs one accounts row inside the given tx. Persistence
// never inserts into accounts (creation is out of scope, LAYER_5_PERSISTENCE
// §3), so integration tests seed manually. account_id doubles as owner_id
// for uniqueness across parallel tests.
func seedAccount(t *testing.T, tx dbConn, accountID string, tradingCash, bankCash int64) {
	t.Helper()
	_, err := tx.Exec(context.Background(),
		`INSERT INTO accounts (account_id, owner_type, owner_id, trading_cash, bank_cash)
		 VALUES ($1, 'agent', $1, $2, $3)`,
		accountID, tradingCash, bankCash)
	if err != nil {
		t.Fatalf("seed account %s: %v", accountID, err)
	}
}

// baseTradeInsert returns a fully-populated TradeInsert for a given trade
// and account. Used by trades_sql_test to keep test cases readable.
func baseTradeInsert(tradeID, accountID string) TradeInsert {
	entry := time.Date(2026, 3, 15, 9, 30, 0, 0, time.UTC)
	exit := entry.Add(4 * time.Hour)
	return TradeInsert{
		TradeID:      tradeID,
		AccountID:    accountID,
		SessionID:    "sess-" + tradeID,
		InstrumentID: "ACME",
		Direction:    "long",
		Quantity:     100,
		EntryPrice:   50000,
		ExitPrice:    51000,
		EntryAt:      entry,
		ExitAt:       exit,
		GrossPnl:     100000,
		TotalCosts:   543,
		NetPnl:       99457,
		PnlPct:       1.98914,
		ExitReason:   "voluntary",
		CostLines: map[string]int64{
			india.LineSTT:      25,
			india.LineExchange: 3,
			india.LineSEBI:     1,
			india.LineGST:      5,
		},
		CostTotal: 543,
	}
}
