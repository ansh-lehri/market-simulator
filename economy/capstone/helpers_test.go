package capstone

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
	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
	"simulator/economy/persistence"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// Capstone integration-test harness.
//
// This file is test-only (compiles under `go test` only). It provides:
//
//   - newBundle(t)         — Wires a live *Bundle against the test DB + Redis
//                            or skips if env is unset. Registers full
//                            teardown via t.Cleanup.
//   - defaultIndiaRates()  — the pinned India rate fixture used across
//                            capstone tests.
//   - buildLimitOrder(...) — a compact reserve.Order builder for limit legs.
//   - waitForBufferDrained — polls the closed-trade buffer LLEN to 0.
//   - Time fixtures        — a fixed clock so replays are deterministic.
//
// Convention (matches economy/persistence/integration_helpers_test.go):
// TEST_DATABASE_URL and TEST_REDIS_URL are the env vars; either unset → the
// test skips. Never runs against a real DATABASE_URL to prevent touching
// non-test data.
// -----------------------------------------------------------------------------

const (
	envTestDBURL    = "TEST_DATABASE_URL"
	envTestRedisURL = "TEST_REDIS_URL"

	testPingTimeout = 5 * time.Second
)

// testExitAt is the fixed timestamp handed to every Convert / lot as
// EntryAt / exitAt so replays are bit-identical. Mirrors the pattern in
// economy/reserve/convert_test.go so hand-computed goldens carry over.
var testExitAt = time.Date(2026, 9, 22, 14, 30, 0, 0, time.UTC)

// newBundle spins up a real Bundle wired to TEST_DATABASE_URL /
// TEST_REDIS_URL. Skips the test cleanly if either env var is unset —
// consistent with persistence/integration_helpers_test.go, so `go test ./...`
// stays green on a fresh checkout without live infra.
//
// The returned bundle is fully wired: pool + redis pinged, drainer
// goroutine running, all compute services ready. The second return value
// is the per-test Redis KEY NAMESPACE (no trailing colon); callers pass
// it to waitForBufferDrained and to any direct Redis probing they need.
//
// Teardown is registered via t.Cleanup and runs in a single pass at test
// end, in this order:
//
//  1. Persist.Stop  — drainer goroutine exits (still touches the handles).
//  2. TRUNCATE      — the three tables, so the next test starts clean.
//  3. FLUSH         — Redis keys under this test's namespace, so per-test
//                     snapshot / buffer state does not leak.
//  4. Redis close   — client handle released.
//  5. Pool close    — pgxpool handle released.
//
// TRUNCATE + FLUSH before closing handles is deliberate — the drainer runs
// on its own goroutine + its own pool connection, so a per-test tx-rollback
// pattern would not compose. Whole-table cleanup is the only reliable
// reset. -race stays green because the drainer stops before either
// cleanup step touches state.
func newBundle(t *testing.T) (*Bundle, string) {
	t.Helper()

	pgDSN := os.Getenv(envTestDBURL)
	if pgDSN == "" {
		t.Skipf("%s not set — capstone integration test skipped", envTestDBURL)
	}
	redisURL := os.Getenv(envTestRedisURL)
	if redisURL == "" {
		t.Skipf("%s not set — capstone integration test skipped", envTestRedisURL)
	}

	// Per-test Redis namespace: sanitize t.Name() to a path-safe form so
	// subtests (which include a "/" in the name) still yield a clean key
	// prefix. Namespace has NO trailing colon — persistence's nsPrefix
	// appends one internally.
	safeName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	namespace := fmt.Sprintf("l5cap_%s_%d", safeName, time.Now().UnixNano())

	opts := Options{
		ConnConfig: config.Config{
			Postgres: config.PostgresConfig{DSN: pgDSN, PingTimeout: testPingTimeout},
			Redis:    config.RedisConfig{URL: redisURL, PingTimeout: testPingTimeout},
		},
		RedisNamespace: namespace,
		Cost:           cost.NewIndiaCostModule(defaultIndiaRates()),
		CostAdapter:    persistence.IndiaCostAdapter{},
	}

	ctx := context.Background()
	bundle, err := Wire(ctx, opts)
	if err != nil {
		t.Fatalf("capstone.Wire: %v", err)
	}

	// Single teardown closure — sequential steps in the correct order.
	// Errors are logged (not fatal) so a later step still runs and the
	// test does not leave residue in the shared test DB.
	t.Cleanup(func() {
		// 1. Stop the drainer goroutine while its handles are still live.
		if bundle.Persist != nil {
			bundle.Persist.Stop()
		}
		// 2. TRUNCATE the three tables. Child → parent order handles the
		//    FK from trade_costs → participant_trades cleanly.
		if bundle.Pool != nil {
			truncateAll(t, bundle.Pool)
		}
		// 3. FLUSH this test's Redis namespace.
		if bundle.Redis != nil {
			flushNamespace(t, bundle.Redis, namespace)
		}
		// 4. Close Redis, then Pool. bundle.Close would combine 1 + 4 + 5
		//    into one call, but splitting lets 2 + 3 run against live
		//    handles between the drainer stop and the handle close.
		if bundle.Redis != nil {
			if err := bundle.Redis.Close(); err != nil {
				t.Logf("capstone teardown: redis close: %v", err)
			}
		}
		if bundle.Pool != nil {
			bundle.Pool.Close()
		}
	})

	return bundle, namespace
}

// defaultIndiaRates returns the pinned India rate fixture matching
// [[project_india_rate_locks]] and economy/reserve/convert_test.go:26.
// Kept here (duplicated) rather than referenced across packages because
// the rulebook home is undecided (see
// [[project_broker_rulebook_injection_tbd]]) — the capstone owns its
// own fixture until that decision lands.
func defaultIndiaRates() cost.IndiaRates {
	return cost.IndiaRates{
		STT:           cost.Rate{Num: 25, Den: 100_000},     // 0.025%
		Exchange:      cost.Rate{Num: 297, Den: 10_000_000}, // 0.00297% NSE
		SEBI:          cost.Rate{Num: 1, Den: 1_000_000},    // 0.0001%
		Stamp:         cost.Rate{Num: 3, Den: 100_000},      // 0.003%
		GST:           cost.Rate{Num: 18, Den: 100},         // 18%
		Brokerage:     2000,                                 // ₹20 = 2000 paise per executed order
		AutosquareFee: 5000,                                 // ₹50 = 5000 paise (CST-5)
	}
}

// buildLimitOrder assembles a reserve.Order for a limit leg. The market-
// specific fields (BestAsk, SlippageBuffer) are ignored on limits, so
// they stay zero. When a capstone test needs a market order, add a
// buildMarketOrder helper alongside this one — do not overload this
// builder.
func buildLimitOrder(
	orderID string,
	accountID account.AccountID,
	sessionID string,
	instrumentID contract.InstrumentID,
	side contract.Side,
	qty int64,
	limitPrice int64,
) reserve.Order {
	return reserve.Order{
		OrderID:      orderID,
		AccountID:    accountID,
		SessionID:    sessionID,
		InstrumentID: instrumentID,
		Side:         side,
		Qty:          qty,
		IsMarket:     false,
		LimitPrice:   limitPrice,
	}
}

// waitForBufferDrained polls the closed-trade buffer LLEN until it hits
// zero or `timeout` elapses. Returns true on drain, false on timeout.
//
// The buffer key shape matches economy/persistence/keys.go:70 (bufKey):
// {namespace}:l5:buf:trades. Duplicated here rather than reaching into
// persistence internals — the shape is fixed by LAYER_5_PERSISTENCE §7
// and cannot silently drift.
func waitForBufferDrained(t *testing.T, client *redis.Client, namespace string, timeout time.Duration) bool {
	t.Helper()
	bufferKey := namespace + ":l5:buf:trades"
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		length, err := client.LLen(context.Background(), bufferKey).Result()
		if err != nil {
			t.Logf("waitForBufferDrained: LLEN %s: %v", bufferKey, err)
			return false
		}
		if length == 0 {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// truncateAll wipes the three durable tables (child → parent order so
// the FK from trade_costs.trade_id to participant_trades.trade_id is
// happy). Runs against the shared TEST_DATABASE_URL — never a real DB —
// which is why the newBundle helper is gated on env-var presence.
//
// Called only from the newBundle teardown closure. Errors are logged
// (not fatal) so later teardown steps still run.
func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	// Explicit table list (not TRUNCATE ... CASCADE) so the intent is
	// visible and the FK order is documented in code.
	statements := []string{
		`TRUNCATE TABLE trade_costs`,
		`TRUNCATE TABLE participant_trades`,
		`TRUNCATE TABLE accounts`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Logf("capstone teardown: %s: %v", statement, err)
		}
	}
}

// flushNamespace SCANs and DELs every Redis key beginning with the given
// namespace — the closed-trade buffer, snapshot keys, everything the
// component wrote during this test. Modelled on
// economy/persistence/integration_helpers_test.go's requireRedis cleanup.
//
// Called only from the newBundle teardown closure. Silent on SCAN error
// (matches persistence's pattern) so later teardown steps still run.
func flushNamespace(t *testing.T, client *redis.Client, namespace string) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	pattern := namespace + ":*"
	for {
		batch, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			t.Logf("capstone teardown: SCAN %s: %v", pattern, err)
			return
		}
		if len(batch) > 0 {
			if err := client.Del(ctx, batch...).Err(); err != nil {
				t.Logf("capstone teardown: DEL batch: %v", err)
			}
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}
