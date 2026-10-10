package marketdata

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/exchange/emit"
)

// --- helpers ---------------------------------------------------------

// defaultTestCandleCfg returns a valid Config for construction tests.
func defaultTestCandleCfg() Config {
	return Config{ReadTimeout: 2 * time.Second}
}

// defaultTestWSCfg returns a valid WSConfig for construction tests.
func defaultTestWSCfg() WSConfig {
	return WSConfig{
		ConnBuffer:           4,
		LagEvictionThreshold: 3,
		PingInterval:         1 * time.Second,
		ReadTimeout:          3 * time.Second,
		WriteTimeout:         1 * time.Second,
	}
}

// --- NewMarketData construction panics (pool-free) ------------------
// A nil pool is NOT checked at construction (mirrors broker/store.NewStore
// — see NewMarketData doc). These panic-paths run without a DB.

func TestNewMarketData_PanicsOnInvalidCandleCfg(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on invalid candleCfg (zero ReadTimeout)")
		}
	}()
	// nil pool is fine — invalid cfg panics first (cfg validation runs
	// before any pool use).
	NewMarketData(nil, Config{ReadTimeout: 0}, defaultTestWSCfg())
}

func TestNewMarketData_PanicsOnInvalidWSCfg(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on invalid wsCfg (zero ConnBuffer)")
		}
	}()
	NewMarketData(nil, defaultTestCandleCfg(), WSConfig{})
}

// --- MarketPriceCache accessor + RegisterCandlesRoute (pool-free) ---

func TestNewMarketData_MarketPriceCache_NonNilEmpty(t *testing.T) {
	// Pool-free: this test probes only the registry accessor, no Insert.
	marketData := NewMarketData(nil, defaultTestCandleCfg(), defaultTestWSCfg())

	cache := marketData.MarketPriceCache()
	if cache == nil {
		t.Fatal("MarketPriceCache(): got nil, want *CacheRegistry")
	}
	if got := cache.InstanceCount(); got != 0 {
		t.Errorf("InstanceCount on fresh registry: got %d, want 0", got)
	}
	// Misses on everything (no instances wired yet).
	if _, ok := cache.BestAsk("E1", "ACME"); ok {
		t.Error("BestAsk on empty registry: unexpected hit")
	}
}

// Registering the route puts a handler on the mux at PathCandles. A
// bare GET (no params) returns 400 from the handler's own validation,
// NOT a 404 from the mux. Mirrors the broker/bringup mux-registration
// smoke test. Pool-free: this exercise never hits the engine's Store.
func TestRegisterCandlesRoute_InstallsHandlerAtPath(t *testing.T) {
	marketData := NewMarketData(nil, defaultTestCandleCfg(), defaultTestWSCfg())

	mux := http.NewServeMux()
	marketData.RegisterCandlesRoute(mux)

	request := httptest.NewRequest(http.MethodGet, PathCandles, nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code == http.StatusNotFound {
		t.Fatalf("mux returned 404 — route %s not registered", PathCandles)
	}
	if recorder.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (handler's own validation)", recorder.Code)
	}
}

// --- WSConfig accessor (pool-free) -----------------------------------

func TestNewMarketData_WSConfig_ReturnedVerbatim(t *testing.T) {
	wsCfg := defaultTestWSCfg()
	wsCfg.ConnBuffer = 123 // distinctive value to prove no mutation
	marketData := NewMarketData(nil, defaultTestCandleCfg(), wsCfg)

	if got := marketData.WSConfig(); got.ConnBuffer != 123 {
		t.Errorf("WSConfig.ConnBuffer: got %d, want 123 (held verbatim for L7)", got.ConnBuffer)
	}
}

// --- WireInstance (pool-free in on-demand mode) ----------------------

func TestWireInstance_PanicsOnUnknownMarket(t *testing.T) {
	marketData := NewMarketData(nil, defaultTestCandleCfg(), defaultTestWSCfg())
	tape := make(chan emit.TapeEvent)
	defer close(tape)

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unknown market")
		}
	}()
	_ = marketData.WireInstance("E1", tape, "ZZ", ModeOnDemand)
}

// WireInstance registers the snapshot in the shared CacheRegistry so
// the account-side price seam can find it. Observable: before = empty,
// after = 1 instance. Then: closing the tape → ingestor teardown →
// Remove → registry empty again. Full lifecycle.
//
// Pool-free: on-demand mode skips Insert, so the ingestor never calls
// the pool. No TEST_DATABASE_URL required.
func TestWireInstance_RegistersSnapshotAndRemovesOnTeardown(t *testing.T) {
	marketData := NewMarketData(nil, defaultTestCandleCfg(), defaultTestWSCfg())

	if got := marketData.MarketPriceCache().InstanceCount(); got != 0 {
		t.Fatalf("precondition: InstanceCount=%d, want 0", got)
	}

	tape := make(chan emit.TapeEvent, 4)
	liveFanOut := marketData.WireInstance("E1", tape, "IN", ModeOnDemand)
	if liveFanOut == nil {
		t.Fatal("WireInstance returned nil FanOut")
	}

	// Snapshot registered → cache knows E1.
	if got := marketData.MarketPriceCache().InstanceCount(); got != 1 {
		t.Errorf("post-WireInstance: InstanceCount=%d, want 1", got)
	}

	// Teardown: close tape → ingestor exits → Remove(E1).
	close(tape)
	// Wait up to 500ms for the ingestor to run the Remove on exit.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if marketData.MarketPriceCache().InstanceCount() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := marketData.MarketPriceCache().InstanceCount(); got != 0 {
		t.Errorf("after teardown: InstanceCount=%d, want 0 (ingestor must Remove on exit)", got)
	}
}

// End-to-end: WireInstance spawns an ingestor; an inbound Trade on the
// tape updates the registered MarketSnapshot (observable via the
// CacheRegistry's LastPrice). Proves the whole stage-7 chain is wired
// correctly: WireInstance → registry.Put(snap) → ingestor processes
// event → snap updates → CacheRegistry.LastPrice sees it.
//
// Pool-free: Trade events don't touch the store, and we use on-demand
// mode so even hypothetical Candle events would skip persistence.
func TestWireInstance_EndToEnd_TradeFlowsToCacheRegistry(t *testing.T) {
	marketData := NewMarketData(nil, defaultTestCandleCfg(), defaultTestWSCfg())

	tape := make(chan emit.TapeEvent, 4)
	_ = marketData.WireInstance("E1", tape, "IN", ModeOnDemand)

	// Ingestor is running. Send a Trade — it should land on the
	// registered MarketSnapshot and surface via the shared registry.
	tape <- emit.TapeEvent{
		Kind:       emit.KindTrade,
		Instrument: "ACME",
		Trade:      &emit.PublicTrade{Price: 10_050, Qty: 100},
	}
	// Wait briefly for the ingestor to process the event.
	deadline := time.Now().Add(500 * time.Millisecond)
	var lastPrice int64
	var ok bool
	for time.Now().Before(deadline) {
		price, hit := marketData.MarketPriceCache().LastPrice("E1", "ACME")
		if hit {
			lastPrice, ok = int64(price), true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(tape)

	if !ok {
		t.Fatal("CacheRegistry.LastPrice(E1, ACME): never hit after Trade event")
	}
	if lastPrice != 10_050 {
		t.Errorf("LastPrice: got %d, want 10050", lastPrice)
	}
}
