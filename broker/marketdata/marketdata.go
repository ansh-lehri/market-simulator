package marketdata

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/exchange/emit"
)

// Mode selects whether an exchange instance's candles are persisted.
// Set once at WireInstance time per instance; the ingestor reads it on
// every KindCandle event to decide persist-or-skip.
//
// Canonical mode persists every closed 1-min candle to the durable
// store so HTTP /candles history can serve them later; on-demand mode
// skips persistence (DESIGN §1.2 / BROKER_MARKET_DATA_L7_EXPECTATIONS
// §3 — on-demand sessions never write to the shared history table).
type Mode int

const (
	ModeCanonical Mode = iota
	ModeOnDemand
)

// String returns the stable wire/log form for a Mode.
func (mode Mode) String() string {
	switch mode {
	case ModeCanonical:
		return "canonical"
	case ModeOnDemand:
		return "ondemand"
	}
	return "invalid"
}

// MarketData is the broker-side holder for every process-lifetime piece
// of the market-data component (BROKER_MARKETDATA_IMPL_WIRING §2):
//
//   - candleStore + readEngine: shared durable layer + its HTTP read engine
//   - cacheRegistry: the account-side price seam (funnel.MarketPriceCache)
//   - candleCfg: the HTTP /candles handler's per-request deadline
//   - wsCfg: the WS connection tunables (held for L7 to pass into
//     NewWSHandler when it registers /v1/ws — bringup does NOT register
//     /ws in Phase 7 because the per-session FanOut dispatch is L7's)
//
// Built ONCE at broker bootstrap by bringup. WireInstance is called
// per exchange instance by L7 (or by tests via a fake tape) and spawns
// the per-instance ingestor goroutine.
//
// Zero value is NOT usable — construct via NewMarketData.
type MarketData struct {
	candleStore   *CandleStore
	readEngine    *ReadEngine
	cacheRegistry *CacheRegistry
	candleCfg     Config
	wsCfg         WSConfig
}

// NewMarketData builds the shared process-lifetime pieces. pgxPool is
// the broker Store's shared pool (via store.Pool()) — borrowed, not
// owned; store.Close handles pool teardown. candleCfg + wsCfg are
// validated at construction (fail-fast — a zero timeout would collapse
// every request).
//
// Panics on invalid config — bringup programming error per root
// CLAUDE.md invariant rule. A nil pool is NOT checked here (mirrors
// broker/store.NewStore + broker/marketdata.NewCandleStore): a nil
// pool nil-derefs on first Insert, surfacing the bringup bug at the
// first ingestor event rather than at construction. Keeps NewMarketData
// usable in unit tests that never touch Insert (e.g. accessor + route
// registration tests).
func NewMarketData(pgxPool *pgxpool.Pool, candleCfg Config, wsCfg WSConfig) *MarketData {
	if err := candleCfg.Validate(); err != nil {
		panic("broker/marketdata: NewMarketData: " + err.Error())
	}
	if err := wsCfg.Validate(); err != nil {
		panic("broker/marketdata: NewMarketData: " + err.Error())
	}
	candleStore := NewCandleStore(pgxPool)
	readEngine := NewReadEngine(candleStore)
	cacheRegistry := NewCacheRegistry()
	slog.Info("broker/marketdata: MarketData built",
		"candle_read_timeout", candleCfg.ReadTimeout,
		"ws_conn_buffer", wsCfg.ConnBuffer,
		"ws_lag_eviction_threshold", wsCfg.LagEvictionThreshold,
	)
	return &MarketData{
		candleStore:   candleStore,
		readEngine:    readEngine,
		cacheRegistry: cacheRegistry,
		candleCfg:     candleCfg,
		wsCfg:         wsCfg,
	}
}

// MarketPriceCache returns the shared CacheRegistry — bringup injects
// this as funnel.NewBroker's mktCache argument, replacing the nil
// stand-in that bringup passes today. Structural interface match: the
// returned type has BestAsk/LastPrice with signatures matching
// funnel.MarketPriceCache, so Go accepts it at the bringup call site
// without this package needing to import broker/funnel.
func (marketData *MarketData) MarketPriceCache() *CacheRegistry {
	return marketData.cacheRegistry
}

// RegisterCandlesRoute installs GET /v1/candles on the shared mux.
// Called by bringup during buildMux. Not an http.Handler because we
// want to keep bringup's mux-wiring style (register, don't return a
// router) — same pattern as broker/reads's handler constructors + mux
// registrations.
func (marketData *MarketData) RegisterCandlesRoute(mux *http.ServeMux) {
	mux.Handle(PathCandles, NewCandleHandler(marketData.readEngine, marketData.candleCfg))
	slog.Info("broker/marketdata: /v1/candles route registered")
}

// WSConfig returns the WS tunables so L7 (when it exists) can build
// per-instance WS handlers via NewWSHandler at the time it registers
// /v1/ws against its own session→FanOut dispatcher. Not called today
// (Phase 7 does not register /v1/ws because the dispatcher is L7's).
func (marketData *MarketData) WSConfig() WSConfig {
	return marketData.wsCfg
}

// WireInstance is the one entry point L7 calls per exchange instance
// (BROKER_MARKETDATA_IMPL_WIRING §2). Builds the per-instance trio
// (MarketSnapshot + streamRollup + FanOut), registers the snapshot in
// the shared CacheRegistry so the account side can read prices against
// this exch, and spawns the ingestor goroutine.
//
// Returns the FanOut so L7's per-session WS dispatcher can route live
// subscriptions to the right instance.
//
// Market must be in the tz allowlist (tzFor); an unknown market is a
// programming error at the L7 call site — panic (invariant per root
// CLAUDE.md). Same discipline as the HTTP handler's handler-side
// market validation + engine-side panic.
//
// NOT called by bringup today — L7 drives per-instance wiring. Tests
// call WireInstance with a fake tape to exercise the end-to-end path.
func (marketData *MarketData) WireInstance(
	exchangeID ExchangeID,
	tape <-chan emit.TapeEvent,
	market string,
	mode Mode,
) *FanOut {
	location, locationOK := tzFor(market)
	if !locationOK {
		panic(fmt.Sprintf("broker/marketdata: WireInstance called with unknown market %q", market))
	}
	marketSnapshot := NewMarketSnapshot(exchangeID)
	marketData.cacheRegistry.Put(exchangeID, marketSnapshot)
	rollup := newStreamRollup(location)
	liveFanOut := NewFanOut(marketSnapshot, market, location)

	go runMarketDataIngestor(
		tape,
		exchangeID,
		market,
		mode,
		marketData.candleStore,
		marketSnapshot,
		rollup,
		liveFanOut,
		marketData.cacheRegistry,
	)
	slog.Info("broker/marketdata: WireInstance spawned ingestor",
		"exch", exchangeID,
		"market", market,
		"mode", mode,
	)
	return liveFanOut
}
