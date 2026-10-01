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
	"simulator/economy/persistence"
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

// cleanupOwnersWithPrefix schedules a FK-ordered DELETE on test end:
// trade_costs → participant_trades → accounts, scoped to the test's
// prefix. trade_costs has FK to participant_trades (migration 0003),
// and participant_trades has FK to accounts (migration 0002), so the
// order matters.
func cleanupOwnersWithPrefix(t *testing.T, pool *pgxpool.Pool, ownerPrefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		likePattern := ownerPrefix + "%"
		// 1) trade_costs rows whose parent trade belongs to a seeded account.
		if _, err := pool.Exec(ctx,
			`DELETE FROM trade_costs
			 WHERE trade_id IN (
			     SELECT pt.trade_id FROM participant_trades pt
			     JOIN accounts acc ON acc.account_id = pt.account_id
			     WHERE acc.owner_id LIKE $1
			 )`, likePattern); err != nil {
			t.Logf("cleanup trade_costs (prefix=%s): %v", ownerPrefix, err)
		}
		// 2) participant_trades for seeded accounts.
		if _, err := pool.Exec(ctx,
			`DELETE FROM participant_trades
			 WHERE account_id IN (
			     SELECT account_id FROM accounts WHERE owner_id LIKE $1
			 )`, likePattern); err != nil {
			t.Logf("cleanup participant_trades (prefix=%s): %v", ownerPrefix, err)
		}
		// 3) the accounts themselves.
		if _, err := pool.Exec(ctx,
			`DELETE FROM accounts WHERE owner_id LIKE $1`,
			likePattern); err != nil {
			t.Logf("cleanup accounts (prefix=%s): %v", ownerPrefix, err)
		}
	})
}

// tradeSeed carries every field for one hand-crafted participant_trades
// row. Zero values mean "use the test's default for that field" when
// baseTradeSeed builds one — see tests for the override pattern.
type tradeSeed struct {
	TradeID      string
	AccountID    string
	SessionID    string
	InstrumentID string
	Direction    string
	Quantity     int64
	EntryPrice   int64
	ExitPrice    int64
	EntryAt      time.Time
	ExitAt       time.Time
	GrossPnl     int64
	TotalCosts   int64
	NetPnl       int64
	PnlPct       float32
	ExitReason   string
}

// seedTrade INSERTs one participant_trades row with the given fields.
// Not the drainer's SQL — this is test-only, hand-written to decouple
// the Store's integration tests from economy/persistence's drainer
// implementation. 15 columns match migration 0002 verbatim.
func seedTrade(t *testing.T, pool *pgxpool.Pool, seed tradeSeed) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO participant_trades (
			trade_id, account_id, session_id, instrument_id,
			direction, quantity, entry_price, exit_price,
			entry_at, exit_at,
			gross_pnl, total_costs, net_pnl, pnl_pct, exit_reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		seed.TradeID, seed.AccountID, seed.SessionID, seed.InstrumentID,
		seed.Direction, seed.Quantity, seed.EntryPrice, seed.ExitPrice,
		seed.EntryAt, seed.ExitAt,
		seed.GrossPnl, seed.TotalCosts, seed.NetPnl, seed.PnlPct, seed.ExitReason,
	)
	if err != nil {
		t.Fatalf("seed trade %s: %v", seed.TradeID, err)
	}
}

// seedIndiaCostRow writes one trade_costs row via the REAL
// IndiaCostAdapter (economy/persistence). This is deliberate: the Store
// under test reads what IndiaCostAdapter.InsertCostRow writes, so using
// it here exercises the exact write-side column layout.
func seedIndiaCostRow(t *testing.T, pool *pgxpool.Pool, tradeID string, lines map[string]int64, total int64) {
	t.Helper()
	adapter := persistence.IndiaCostAdapter{}
	if err := adapter.InsertCostRow(context.Background(), pool, tradeID, lines, total); err != nil {
		t.Fatalf("seed trade_costs for %s: %v", tradeID, err)
	}
}
