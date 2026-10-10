package marketdata

import (
	"log/slog"
	"sync"

	"simulator/contract"
)

// CacheRegistry holds the per-ExchangeID MarketSnapshot that the
// account-side price seam reads through. ONE instance exists per
// broker (process-lifetime, built by bringup); WireInstance (Stage 7)
// calls Put at per-instance bring-up and the ingestor's teardown calls
// Remove when Public closes (BODIES §1.8 — else the account side would
// read a dead snapshot).
//
// Satisfies funnel.MarketPriceCache structurally — this package
// deliberately does NOT import broker/funnel so marketdata stays
// self-contained for a possible future service split. Interface
// matching happens at the bringup call site:
//
//	cache := marketdata.NewCacheRegistry()
//	brokerInstance := funnel.NewBroker(..., cache)   // ← compile-time check lands here
//
// If the funnel interface drifts, bringup's build breaks noisily. The
// test file carries a local marketPriceCacheShape fence that catches
// drift inside this package too.
//
// Concurrency: one RWMutex on the map. Readers (BestAsk/LastPrice on
// the account shard hot path) RLock briefly then dispatch to the
// MarketSnapshot's lock-free Load. Writers (Put/Remove — once per
// instance lifetime) take the write-lock, rare. The MarketSnapshot
// itself handles its own concurrency (atomic CoW) once resolved.
//
// Zero value is NOT usable — construct via NewCacheRegistry.
type CacheRegistry struct {
	mu     sync.RWMutex
	byExch map[ExchangeID]*MarketSnapshot
}

// NewCacheRegistry builds an empty registry. ONE lives for the
// broker's lifetime; bringup injects it as funnel.NewBroker's mktCache
// argument (Stage 7 — replaces the nil that bringup passes today, so
// market-buy stops rejecting on ReasonNoMarketPrice and A9/A14 marks
// stop degrading to MarkAvailable:false).
func NewCacheRegistry() *CacheRegistry {
	return &CacheRegistry{
		byExch: make(map[ExchangeID]*MarketSnapshot),
	}
}

// Put registers a MarketSnapshot for an exchange instance. Called by
// WireInstance (Stage 7) once per instance, at bring-up — specifically
// BEFORE Start spawns the ingestor, so account-side lookups never see
// an exch key with a nil snapshot.
//
// A second Put under the same exch overwrites and logs at warn-level
// — a programming error at the bringup layer (one instance = one
// snapshot) but we don't panic because that would blow up the broker
// over a benign re-wire.
//
// A nil marketSnapshot IS a programming error — panic (invariant per
// root CLAUDE.md).
func (registry *CacheRegistry) Put(exchangeID ExchangeID, marketSnapshot *MarketSnapshot) {
	if marketSnapshot == nil {
		panic("broker/marketdata: CacheRegistry.Put requires non-nil MarketSnapshot")
	}
	registry.mu.Lock()
	if _, existed := registry.byExch[exchangeID]; existed {
		slog.Warn("broker/marketdata: CacheRegistry.Put overwriting existing snapshot",
			"exch", exchangeID,
		)
	}
	registry.byExch[exchangeID] = marketSnapshot
	count := len(registry.byExch)
	registry.mu.Unlock()
	slog.Info("broker/marketdata: CacheRegistry registered snapshot",
		"exch", exchangeID,
		"instance_count", count,
	)
}

// Remove deregisters a MarketSnapshot. Called by the ingestor's
// teardown path when the exchange closes Public (BODIES §1.8). After
// Remove, subsequent BestAsk/LastPrice lookups on this exch miss.
//
// Idempotent: a Remove on an unknown exch is a silent no-op (shutdown
// ordering doesn't guarantee Put ran — e.g. a Wire that failed between
// NewExchange and Start).
func (registry *CacheRegistry) Remove(exchangeID ExchangeID) {
	registry.mu.Lock()
	_, existed := registry.byExch[exchangeID]
	delete(registry.byExch, exchangeID)
	count := len(registry.byExch)
	registry.mu.Unlock()
	if existed {
		slog.Info("broker/marketdata: CacheRegistry removed snapshot",
			"exch", exchangeID,
			"instance_count", count,
		)
	} else {
		slog.Debug("broker/marketdata: CacheRegistry.Remove no-op (unknown exch)",
			"exch", exchangeID,
		)
	}
}

// BestAsk implements the "best ask" half of funnel.MarketPriceCache.
// Dispatches to the registered MarketSnapshot for exch; miss on any of:
//   - exch not registered (unknown instance)
//   - instrument never seen on that exch's tape
//   - quote on that instrument has no ask side (HasAsk=false, including
//     the empty-ask-book case where BestAsk field is a stale 0 — the
//     HasAsk gate is load-bearing because 0 is a valid tick price)
//
// Returns (0, false) on any miss. The account path's semantic on miss
// (funnel/mkt_cache.go doc): market-buy place rejects with
// ReasonNoMarketPrice; limit orders don't call BestAsk.
func (registry *CacheRegistry) BestAsk(exchangeID ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool) {
	registry.mu.RLock()
	candidateSnapshot, ok := registry.byExch[exchangeID]
	registry.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return candidateSnapshot.BestAsk(instrument)
}

// LastPrice implements the "last price" half of funnel.MarketPriceCache.
// Same miss shape as BestAsk (unknown exch / unknown instrument /
// HasLast=false). On miss the account side's A9/A14 position mark
// degrades to MarkAvailable:false; the position row still ships.
func (registry *CacheRegistry) LastPrice(exchangeID ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool) {
	registry.mu.RLock()
	candidateSnapshot, ok := registry.byExch[exchangeID]
	registry.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return candidateSnapshot.LastPrice(instrument)
}

// InstanceCount returns the number of currently-registered exchange
// instances. For logs + tests; not a hot path.
func (registry *CacheRegistry) InstanceCount() int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.byExch)
}
