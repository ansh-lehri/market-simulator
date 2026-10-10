package bringup

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"simulator/broker/store"
	"simulator/connectivity/config"
	"simulator/country/india"
	"simulator/economy/lifecycle"
	"simulator/economy/realize"
)

// ---------- helpers ----------

const (
	envTestDBURL    = "TEST_DATABASE_URL"
	envTestRedisURL = "TEST_REDIS_URL"
)

// testInfraCfg builds a connectivity.Config from the TEST_* env vars.
// Skips the test cleanly if either is unset — same pattern as the
// Stage-A/B/C integration helpers.
func testInfraCfg(t *testing.T) config.Config {
	t.Helper()
	dbDSN := os.Getenv(envTestDBURL)
	redisURL := os.Getenv(envTestRedisURL)
	if dbDSN == "" || redisURL == "" {
		t.Skipf("%s / %s not set — integration test skipped", envTestDBURL, envTestRedisURL)
	}
	return config.Config{
		Postgres: config.PostgresConfig{
			DSN:         dbDSN,
			PingTimeout: 5 * time.Second,
			MaxConns:    4,
		},
		Redis: config.RedisConfig{
			URL:         redisURL,
			PingTimeout: 5 * time.Second,
		},
	}
}

// defaultCfg is a valid BrokerConfig variants modify. Keeps the test
// file terse and highlights what each case actually probes.
func defaultCfg() BrokerConfig {
	return BrokerConfig{
		BrokerID:   "B-test",
		ShardCount: 1,
		InboxDepth: 4,
		Country:    "IN",
	}
}

// ---------- fast-path tests (NO DB required) ----------

// ErrBadConfig surfaces for every invalid required field. These short-
// circuit before any infra touch, so they run without TEST env vars.

func TestBringUp_BadConfig_EmptyBrokerID(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.BrokerID = ""
	runtime, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
	if runtime != nil {
		t.Error("partial Runtime returned alongside config error")
	}
}

func TestBringUp_BadConfig_ZeroShardCount(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.ShardCount = 0
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}

func TestBringUp_BadConfig_ZeroInboxDepth(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.InboxDepth = 0
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}

func TestBringUp_BadConfig_EmptyCountry(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.Country = ""
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}

// Phase-2 money dials: validate rejects negatives BEFORE any infra opens.
// The infra config is empty — if validate didn't short-circuit, we'd see
// a Postgres-dial failure instead of ErrBadConfig.
func TestBringUp_BadConfig_NegativeStartingCapital(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.StartingCapital = -1
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}

func TestBringUp_BadConfig_NegativeStartingBank(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.StartingBank = -1
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}

func TestBringUp_BadConfig_NegativeOnDemandMinimum(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.OnDemandMinimum = -1
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}

// Negative HTTP timeouts are a programming error at the caller — surface
// them as ErrBadConfig alongside the other config checks. Zero stays
// legal (it falls through to the package default); only negative is
// rejected.
func TestBringUp_BadConfig_NegativeReadTimeout(t *testing.T) {
	badCfg := defaultCfg()
	badCfg.ReadTimeout = -1 * time.Millisecond
	_, err := bringUp(context.Background(), badCfg, config.Config{})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("err: got %v, want ErrBadConfig", err)
	}
}


// Country allowlist: "US" is in the allowlist but returns
// ErrCountryUnavailable (slot defined, body not built). Fires AFTER
// validate() but BEFORE any pool opens — observable because the error
// is clean (no wrapped Postgres/Redis failure).
func TestBringUp_CountryUS_Rejected(t *testing.T) {
	usCfg := defaultCfg()
	usCfg.Country = "US"
	runtime, err := bringUp(context.Background(), usCfg, config.Config{})
	if !errors.Is(err, ErrCountryUnavailable) {
		t.Fatalf("err: got %v, want ErrCountryUnavailable", err)
	}
	if runtime != nil {
		t.Error("partial Runtime returned on country rejection")
	}
}

// Country NOT in the allowlist.
func TestBringUp_CountryUnknown_Rejected(t *testing.T) {
	unknownCfg := defaultCfg()
	unknownCfg.Country = "XX"
	_, err := bringUp(context.Background(), unknownCfg, config.Config{})
	if !errors.Is(err, ErrCountryUnavailable) {
		t.Fatalf("err: got %v, want ErrCountryUnavailable", err)
	}
}

// Nil-safe Shutdown — a nil Runtime is a no-op. Guards callers who defer
// Shutdown on a BringUp that returned (nil, err).
func TestShutdown_NilRuntimeIsNoOp(t *testing.T) {
	var runtime *Runtime
	runtime.Shutdown() // must not panic
}

// ---------- DB-backed tests ----------

// Happy path: bring up with ShardCount=2, assert both Broker and Store
// are non-nil, then Shutdown. Primary assertion is "no panic" — the
// Shutdown-ordering guarantee lives in funnel.Broker.Stop's shardWG and
// in Bundle.Close's own LIFO. We ride on those two.
func TestBringUp_And_Shutdown_HappyPath_ShardCount2(t *testing.T) {
	infraCfg := testInfraCfg(t)
	brokerCfg := defaultCfg()
	brokerCfg.ShardCount = 2

	runtime, err := bringUp(context.Background(), brokerCfg, infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	if runtime == nil {
		t.Fatal("Runtime nil on success")
	}
	if runtime.Broker == nil {
		t.Error("Runtime.Broker nil")
	}
	if runtime.Store == nil {
		t.Error("Runtime.Store nil")
	}
	if got := len(runtime.bundles); got != 2 {
		t.Errorf("bundles: got %d, want 2", got)
	}
	// Phase-2 wiring assertion (P2-1·D3): Store exposes a non-nil
	// *lifecycle.Service built over an UNSTARTED persistence.Component.
	// We can't directly observe "unstarted" without reaching into
	// persistence internals, but the whole process stays race-clean under
	// `-race` which it would not if a second (Store-owned) drainer were
	// racing the per-shard bundles' drainers over the same Redis buffer.
	if runtime.Store.Lifecycle() == nil {
		t.Error("Store.Lifecycle() nil — Phase-2 EnsureAccount/StartSession would nil-deref")
	}
	runtime.Shutdown()
}

// Phase-2 smoke: the Store's lifecycle service can OpenAccount end-to-end
// against the real wiring (unstarted Component, shared pool + redis). This
// is the minimum proof that bringup's Phase-2 additions are plumbed
// correctly before Stage B's EnsureAccount lands on top.
func TestBringUp_StoreLifecycle_OpenAccountSmoke(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)

	// Unique owner so parallel runs don't collide. Delete the row on
	// cleanup — bringup_test.go doesn't share the broker/store integration
	// helpers' prefix-based cleanup, so clean up by exact owner_id.
	ownerID := fmt.Sprintf("bringup-store-lifecycle-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = runtime.Store.Pool().Exec(context.Background(),
			`DELETE FROM accounts WHERE owner_id = $1`, ownerID)
	})

	accountID, err := runtime.Store.Lifecycle().OpenAccount(
		context.Background(),
		lifecycle.OwnerHuman, ownerID,
		100_000 /*startingCapital*/, 50_000 /*startingBank*/,
	)
	if err != nil {
		t.Fatalf("Store.Lifecycle().OpenAccount: %v", err)
	}
	if accountID == "" {
		t.Error("OpenAccount returned empty AccountID")
	}

	// Round-trip via the pool to prove the row actually landed.
	var tradingCash, bankCash int64
	if err := runtime.Store.Pool().QueryRow(context.Background(),
		`SELECT trading_cash, bank_cash FROM accounts WHERE account_id = $1`,
		string(accountID),
	).Scan(&tradingCash, &bankCash); err != nil {
		t.Fatalf("round-trip SELECT: %v", err)
	}
	if tradingCash != 100_000 || bankCash != 50_000 {
		t.Errorf("round-trip cash: got trading=%d bank=%d, want 100000/50000", tradingCash, bankCash)
	}
}

// ShardCount=1 (smallest legal value).
func TestBringUp_And_Shutdown_HappyPath_ShardCount1(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	runtime.Shutdown()
}

// Every read route the runtime is responsible for must be reachable
// on Runtime.Mux. A regression that drops one of the NewXHandler
// registrations would route to the mux's default 404 handler instead
// of our handler. We confirm registration by issuing a GET with no
// account_id — every reads handler responds with 400 "account_id
// required" (its own first check), never a mux 404. A 404 here would
// mean the path wasn't registered.
//
// Transfer is NOT registered by bringup (see buildMux doc for the
// cycle rationale), so it is not asserted here.
func TestBringUp_Mux_RegistersAllReadRoutes(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)

	if runtime.Mux == nil {
		t.Fatal("Runtime.Mux is nil")
	}

	paths := []string{
		"/v1/account/balance",
		"/v1/account/positions",
		"/v1/account/orders",
		"/v1/account/observation",
		"/v1/account/trades",
		"/v1/account/pnl",
		"/v1/account/costs",
		"/v1/account/costs/summary",
		// Phase 7: GET /v1/candles (market-data history). Same probe
		// shape — a bare GET without the required ?market=/?instrument=
		// params hits the handler's own validation and returns 400,
		// NOT a 404 from the mux. A 404 means the route was dropped.
		"/v1/candles",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			recorder := httptest.NewRecorder()
			runtime.Mux.ServeHTTP(recorder, request)
			if recorder.Code == http.StatusNotFound {
				t.Fatalf("mux returned 404 — route not registered")
			}
		})
	}
}

// Phase 7: Runtime.Market is populated by BringUp — L7 (when it
// exists) reaches into it to call WireInstance per exchange instance.
// Also smoke-tests that the market-data registry is non-nil + empty
// at bringup (no instances wired yet), which is also exactly what
// got injected as funnel.NewBroker's mktCache (replaces Phase-6 nil).
func TestBringUp_MarketData_Available(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)

	if runtime.Market == nil {
		t.Fatal("Runtime.Market: got nil — bringup did not wire MarketData")
	}
	registry := runtime.Market.MarketPriceCache()
	if registry == nil {
		t.Fatal("Runtime.Market.MarketPriceCache(): got nil")
	}
	if got := registry.InstanceCount(); got != 0 {
		t.Errorf("InstanceCount at bringup: got %d, want 0 (no instances yet — L7 drives WireInstance)", got)
	}
	// Account-side price seam reads miss cleanly on an empty registry
	// (no exch wired). This mirrors the Phase-6 nopMarketCache
	// behavior, but now flowing through the real registry — a BestAsk
	// hit becomes possible once L7 calls WireInstance.
	if _, ok := registry.BestAsk("E1", "ACME"); ok {
		t.Error("BestAsk at bringup: unexpected hit on empty registry")
	}
}

// WS is NOT registered by bringup today — the per-session FanOut
// dispatcher is L7's seam. This test proves /v1/ws is 404 at bringup
// so a future change that adds a stub /v1/ws here (without the L7
// dispatcher) breaks loudly rather than silently serving dead
// connections.
func TestBringUp_Mux_DoesNotRegisterWSYet(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)

	request := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
	recorder := httptest.NewRecorder()
	runtime.Mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("/v1/ws unexpectedly registered: status=%d (want 404 — L7 owns this route)", recorder.Code)
	}
}

// Transfer is NOT registered by bringup — the caller wires it onto
// runtime.Mux themselves. This test proves the current behaviour so
// a future change that adds transfer back here breaks loudly.
func TestBringUp_Mux_DoesNotRegisterTransfer(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)

	request := httptest.NewRequest(http.MethodPost, "/v1/account/transfer", nil)
	recorder := httptest.NewRecorder()
	runtime.Mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("transfer route unexpectedly registered: status=%d (want 404)", recorder.Code)
	}
}

// Idempotent Shutdown: a second call is a safe no-op.
func TestShutdown_Idempotent_SafeToCallTwice(t *testing.T) {
	infraCfg := testInfraCfg(t)
	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	runtime.Shutdown()
	runtime.Shutdown() // must not panic / double-close pool
}

// Country=IN wires the India stack end-to-end: enqueue a closed trade
// through bundle[0]'s persistence seam, let the drainer land it, read
// it back via the Store, and verify the India cost line names surface.
// This is the proof that:
//   - the per-shard bundle's drainer is actually running,
//   - the IndiaCostAdapter is wired (write path through india.Column*),
//   - the Store reads what the drainer writes (round-trip),
//   - broker/store/costs_in.go maps India columns → India line names.
func TestBringUp_CountryIN_DrainerLandsClosedTradeRoundTrip(t *testing.T) {
	infraCfg := testInfraCfg(t)
	brokerCfg := defaultCfg()
	brokerCfg.ShardCount = 1 // one bundle keeps the test's reasoning simple

	runtime, err := bringUp(context.Background(), brokerCfg, infraCfg)
	if err != nil {
		t.Fatalf("BringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)

	// Seed an accounts row the trade's FK can bind to. The bringup test
	// does not share the broker/store integration-helper file, so inline
	// the one-off INSERT + cleanup here.
	accountID := fmt.Sprintf("bringup-acc-%d", time.Now().UnixNano())
	ownerID := accountID + "-owner"
	tradeID := accountID + "-trd"
	ctx := context.Background()
	storePool := runtime.Store.Pool()
	if _, err := storePool.Exec(ctx,
		`INSERT INTO accounts (account_id, owner_type, owner_id, trading_cash, bank_cash)
		 VALUES ($1, 'agent', $2, 0, 0)`,
		accountID, ownerID,
	); err != nil {
		t.Fatalf("seed accounts: %v", err)
	}
	t.Cleanup(func() {
		_, _ = storePool.Exec(context.Background(),
			`DELETE FROM trade_costs WHERE trade_id = $1`, tradeID)
		_, _ = storePool.Exec(context.Background(),
			`DELETE FROM participant_trades WHERE trade_id = $1`, tradeID)
		_, _ = storePool.Exec(context.Background(),
			`DELETE FROM accounts WHERE account_id = $1`, accountID)
	})

	// Enqueue one closed trade via the first bundle's persistence seam.
	// EnqueueClosedTrade writes to the Redis buffer; the drainer picks
	// it up on its next tick (default 100ms).
	entryAt := time.Date(2026, 10, 1, 9, 30, 0, 0, time.UTC)
	exitAt := entryAt.Add(2 * time.Hour)
	runtime.bundles[0].Persist.EnqueueClosedTrade(realize.ClosedTradeRow{
		Summary: realize.ParticipantTradeRow{
			TradeID:      realize.TradeID(tradeID),
			AccountID:    accountID,
			SessionID:    "sess-bringup",
			InstrumentID: "ACME",
			Direction:    "long",
			Quantity:     10,
			EntryPrice:   50_000,
			ExitPrice:    51_000,
			EntryAt:      entryAt,
			ExitAt:       exitAt,
			GrossPnl:     10_000,
			TotalCosts:   34,
			NetPnl:       9_966,
			PnlPct:       1.99,
			ExitReason:   realize.ExitVoluntary,
		},
		Cost: realize.TradeCostRow{
			TradeID: realize.TradeID(tradeID),
			Lines: map[string]int64{
				india.LineSTT:      25,
				india.LineExchange: 3,
				india.LineSEBI:     1,
				india.LineGST:      5,
			},
			Total: 34,
		},
	})

	// Poll Store.RecentTrades until the drainer has landed it.
	deadline := time.Now().Add(3 * time.Second)
	var tradeRows []store.TradeRow
	for time.Now().Before(deadline) {
		tradeRows, err = runtime.Store.RecentTrades(ctx, accountID, 10)
		if err != nil {
			t.Fatalf("Store.RecentTrades: %v", err)
		}
		if len(tradeRows) >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(tradeRows) != 1 {
		t.Fatalf("drainer did not land the trade within budget: got %d rows", len(tradeRows))
	}
	if tradeRows[0].TradeID != tradeID {
		t.Errorf("TradeID: got %q, want %q", tradeRows[0].TradeID, tradeID)
	}

	// And the India cost row — proves the IndiaCostAdapter write +
	// Store.CostsPerTradeIN read agree on column layout.
	costRows, err := runtime.Store.CostsPerTradeIN(ctx, accountID, 10, nil)
	if err != nil {
		t.Fatalf("Store.CostsPerTradeIN: %v", err)
	}
	if len(costRows) != 1 || costRows[0].TradeID != tradeID || costRows[0].Total != 34 {
		t.Fatalf("cost round-trip: got %+v, want one row {tradeID=%s, total=34}", costRows, tradeID)
	}
	// The seeded India lines must come back as named lines.
	wantLines := map[string]int64{
		india.LineSTT: 25, india.LineExchange: 3, india.LineSEBI: 1, india.LineGST: 5,
	}
	for lineName, wantAmount := range wantLines {
		var found bool
		for _, line := range costRows[0].Lines {
			if line.Name == lineName {
				if line.Amount != wantAmount {
					t.Errorf("line %q: got %d, want %d", lineName, line.Amount, wantAmount)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf("line %q missing from cost row: %+v", lineName, costRows[0].Lines)
		}
	}
}

// Partial-failure rollback: a bad Redis URL makes the first
// capstone.Wire fail; bringUp must return an error and no Runtime. We
// cannot cleanly observe "the bundles already built were Close'd" from
// outside the package, but the error must trace back to the bad infra
// (wrapped by bringUp's own prefix). ShardCount=1 is enough to exercise
// the rollback path (shard 0 fails on Redis after its Postgres opens;
// capstone.Wire's own rollback closes the pool before returning).
func TestBringUp_PartialFailure_BadRedis(t *testing.T) {
	// Postgres still needs to be valid so the Wire gets past its ping
	// — otherwise we'd be testing Postgres failure instead of Redis.
	infraCfg := testInfraCfg(t)
	infraCfg.Redis.URL = "redis://127.0.0.1:1" // unreachable port

	runtime, err := bringUp(context.Background(), defaultCfg(), infraCfg)
	if err == nil {
		if runtime != nil {
			runtime.Shutdown()
		}
		t.Fatal("BringUp: got nil err, want infra failure")
	}
	if runtime != nil {
		t.Error("partial Runtime returned alongside Wire error")
	}
	// Sanity: the error carries bringup's prefix and the Wire failure.
	if !strings.Contains(err.Error(), "capstone.Wire shard 0") {
		t.Errorf("error should carry bringup wrap + shard index, got: %v", err)
	}
}
