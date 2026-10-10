package marketdata

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
)

// marketPriceCacheShape mirrors funnel.MarketPriceCache without
// importing broker/funnel. Both funnel.ExchangeID and marketdata.ExchangeID
// are `= string` (true aliases), so a type that matches this shape
// matches the real funnel interface. If the funnel interface ever
// drifts (field name, extra method), bringup's build will break — this
// local fence catches drift inside the package even without the import.
type marketPriceCacheShape interface {
	BestAsk(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)
	LastPrice(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)
}

var _ marketPriceCacheShape = (*CacheRegistry)(nil)

// --- empty registry --------------------------------------------------

func TestCacheRegistry_Empty_BestAskMiss(t *testing.T) {
	registry := NewCacheRegistry()
	if _, ok := registry.BestAsk("E1", "ACME"); ok {
		t.Error("BestAsk on empty registry: got ok=true, want false")
	}
}

func TestCacheRegistry_Empty_LastPriceMiss(t *testing.T) {
	registry := NewCacheRegistry()
	if _, ok := registry.LastPrice("E1", "ACME"); ok {
		t.Error("LastPrice on empty registry: got ok=true, want false")
	}
}

func TestCacheRegistry_Empty_InstanceCountZero(t *testing.T) {
	registry := NewCacheRegistry()
	if got := registry.InstanceCount(); got != 0 {
		t.Errorf("InstanceCount on empty: got %d, want 0", got)
	}
}

// --- unknown exch ----------------------------------------------------

func TestCacheRegistry_UnknownExch_BestAskMiss(t *testing.T) {
	// Known exch E1 is registered; look up E2 (unknown).
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})
	registry.Put("E1", snap)

	if _, ok := registry.BestAsk("E2", "ACME"); ok {
		t.Error("BestAsk on unknown exch E2: got ok=true, want false")
	}
}

func TestCacheRegistry_UnknownExch_LastPriceMiss(t *testing.T) {
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050)
	registry.Put("E1", snap)

	if _, ok := registry.LastPrice("E2", "ACME"); ok {
		t.Error("LastPrice on unknown exch E2: got ok=true, want false")
	}
}

// --- known exch, unknown instrument ----------------------------------

func TestCacheRegistry_KnownExch_UnknownInstrument_BestAskMiss(t *testing.T) {
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})
	registry.Put("E1", snap)

	if _, ok := registry.BestAsk("E1", "BETA"); ok {
		t.Error("BestAsk E1/BETA (BETA never seen on tape): got ok=true, want false")
	}
}

func TestCacheRegistry_KnownExch_UnknownInstrument_LastPriceMiss(t *testing.T) {
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050)
	registry.Put("E1", snap)

	if _, ok := registry.LastPrice("E1", "BETA"); ok {
		t.Error("LastPrice E1/BETA: got ok=true, want false")
	}
}

// --- happy-path hits -------------------------------------------------

func TestCacheRegistry_BestAsk_HitAfterPutAndSetQuote(t *testing.T) {
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{
		BestBid: 10_040, BidQty: 100, HasBid: true,
		BestAsk: 10_060, AskQty: 200, HasAsk: true,
	})
	registry.Put("E1", snap)

	ask, ok := registry.BestAsk("E1", "ACME")
	if !ok {
		t.Fatal("BestAsk E1/ACME: got ok=false, want true")
	}
	if ask != 10_060 {
		t.Errorf("BestAsk: got %d, want 10060", ask)
	}
}

func TestCacheRegistry_LastPrice_HitAfterPutAndSetLast(t *testing.T) {
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050)
	registry.Put("E1", snap)

	price, ok := registry.LastPrice("E1", "ACME")
	if !ok {
		t.Fatal("LastPrice E1/ACME: got ok=false, want true")
	}
	if price != 10_050 {
		t.Errorf("LastPrice: got %d, want 10050", price)
	}
}

// --- Has* false → miss (gate load-bearing: 0 is a valid tick) --------

func TestCacheRegistry_HasAskFalse_BestAskMiss(t *testing.T) {
	// Snapshot registered + instrument tape-learned (via SetLast), but
	// the quote has no ask side. BestAsk must miss — not return (0,true).
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050) // tape-learn + sets HasLast
	snap.SetQuote("ACME", book.QuoteState{
		BestBid: 10_040, BidQty: 100, HasBid: true,
		HasAsk: false,
	})
	registry.Put("E1", snap)

	if _, ok := registry.BestAsk("E1", "ACME"); ok {
		t.Error("BestAsk with HasAsk=false: got ok=true, want false (HasAsk gate)")
	}
}

func TestCacheRegistry_HasLastFalse_LastPriceMiss(t *testing.T) {
	// Snapshot registered + instrument tape-learned (via SetQuote), but
	// no trade yet so HasLast is false. LastPrice must miss.
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})
	registry.Put("E1", snap)

	if _, ok := registry.LastPrice("E1", "ACME"); ok {
		t.Error("LastPrice with HasLast=false: got ok=true, want false (HasLast gate)")
	}
}

// --- Remove + instance count -----------------------------------------

func TestCacheRegistry_Remove_SubsequentLookupsMiss(t *testing.T) {
	registry := NewCacheRegistry()
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})
	snap.SetLast("ACME", 10_050)
	registry.Put("E1", snap)

	// Preconditions: both hit before Remove.
	if _, ok := registry.BestAsk("E1", "ACME"); !ok {
		t.Fatal("precondition: BestAsk miss before Remove")
	}
	if _, ok := registry.LastPrice("E1", "ACME"); !ok {
		t.Fatal("precondition: LastPrice miss before Remove")
	}

	registry.Remove("E1")

	if _, ok := registry.BestAsk("E1", "ACME"); ok {
		t.Error("BestAsk after Remove: got ok=true, want false")
	}
	if _, ok := registry.LastPrice("E1", "ACME"); ok {
		t.Error("LastPrice after Remove: got ok=true, want false")
	}
	if got := registry.InstanceCount(); got != 0 {
		t.Errorf("InstanceCount after Remove: got %d, want 0", got)
	}
}

func TestCacheRegistry_RemoveUnknownExch_IsSilentNoOp(t *testing.T) {
	// Shutdown ordering doesn't guarantee Put ran — e.g. a Wire that
	// failed between NewExchange and Start would still call Remove on
	// cleanup. Must not panic.
	registry := NewCacheRegistry()
	registry.Remove("E-never-registered")
	if got := registry.InstanceCount(); got != 0 {
		t.Errorf("InstanceCount: got %d, want 0", got)
	}
}

// --- Multi-instance isolation ----------------------------------------

func TestCacheRegistry_MultipleExchanges_IsolatedByKey(t *testing.T) {
	// Two exchanges, both carrying instrument "ACME" at different
	// prices. Lookup by exch must return the right instance's price.
	registry := NewCacheRegistry()
	snapE1 := NewMarketSnapshot("E1")
	snapE1.SetLast("ACME", 10_050)
	registry.Put("E1", snapE1)

	snapE2 := NewMarketSnapshot("E2")
	snapE2.SetLast("ACME", 20_050)
	registry.Put("E2", snapE2)

	if price, _ := registry.LastPrice("E1", "ACME"); price != 10_050 {
		t.Errorf("E1 LastPrice: got %d, want 10050", price)
	}
	if price, _ := registry.LastPrice("E2", "ACME"); price != 20_050 {
		t.Errorf("E2 LastPrice: got %d, want 20050", price)
	}
	if got := registry.InstanceCount(); got != 2 {
		t.Errorf("InstanceCount: got %d, want 2", got)
	}
}

// --- Put semantics ---------------------------------------------------

func TestCacheRegistry_PutOverwrites_WarnLogged_LastWriterWins(t *testing.T) {
	// A re-Put under the same exch replaces the previous snapshot. In
	// production this is a programming error (one instance = one
	// snapshot) and we log a warn, but we do NOT panic — swallowing
	// one wrong wire is better than taking the broker down.
	registry := NewCacheRegistry()

	snapOld := NewMarketSnapshot("E1")
	snapOld.SetLast("ACME", 10_050)
	registry.Put("E1", snapOld)

	snapNew := NewMarketSnapshot("E1")
	snapNew.SetLast("ACME", 99_999)
	registry.Put("E1", snapNew)

	price, _ := registry.LastPrice("E1", "ACME")
	if price != 99_999 {
		t.Errorf("LastPrice after Put-overwrite: got %d, want 99999 (last-writer-wins)", price)
	}
	if got := registry.InstanceCount(); got != 1 {
		t.Errorf("InstanceCount after Put-overwrite: got %d, want 1", got)
	}
}

func TestCacheRegistry_PutNilSnapshot_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on Put(exch, nil)")
		}
	}()
	NewCacheRegistry().Put("E1", nil)
}

// --- -race gate ------------------------------------------------------

// Concurrent Put/Remove vs BestAsk/LastPrice. Account hot path hits
// BestAsk/LastPrice continuously; WireInstance/teardown Put/Remove
// rarely. The RWMutex path must stay race-free under both.
func TestCacheRegistry_ConcurrentPutRemoveVsReaders_RaceFree(t *testing.T) {
	registry := NewCacheRegistry()

	const exchCount = 5
	const iterations = 500
	const readerCount = 4

	// Pre-register snapshots so readers always have SOMETHING to hit
	// (reduces test flakiness — we're probing the registry's map
	// concurrency, not the "lookup during gap" semantic).
	snaps := make([]*MarketSnapshot, exchCount)
	for exchIndex := 0; exchIndex < exchCount; exchIndex++ {
		snap := NewMarketSnapshot(ExchangeID(fmt.Sprintf("E%d", exchIndex)))
		snap.SetLast("ACME", contract.Ticks(1000+exchIndex))
		snap.SetQuote("ACME", book.QuoteState{
			BestAsk: contract.Ticks(1001 + exchIndex), HasAsk: true,
		})
		snaps[exchIndex] = snap
		registry.Put(snap.Exch(), snap)
	}

	var writesDone atomic.Bool
	var wg sync.WaitGroup

	// Writer: churn Put/Remove on random exchanges.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for iter := 0; iter < iterations; iter++ {
			exchIndex := iter % exchCount
			snap := snaps[exchIndex]
			registry.Remove(snap.Exch())
			registry.Put(snap.Exch(), snap)
		}
		writesDone.Store(true)
	}()

	// Readers: hammer BestAsk/LastPrice in a loop.
	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !writesDone.Load() {
				for exchIndex := 0; exchIndex < exchCount; exchIndex++ {
					exch := ExchangeID(fmt.Sprintf("E%d", exchIndex))
					_, _ = registry.BestAsk(exch, "ACME")
					_, _ = registry.LastPrice(exch, "ACME")
				}
			}
		}()
	}

	wg.Wait()
	// If -race flagged nothing, we pass.
}
