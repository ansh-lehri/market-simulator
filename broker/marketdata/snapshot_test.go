package marketdata

import (
	"fmt"
	"sync/atomic"
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/emit"
)

// --- enum sanity -------------------------------------------------------

func TestSessionPhase_StringForm(t *testing.T) {
	cases := map[SessionPhase]string{
		PhaseUnknown: "unknown",
		PhaseOpen:    "open",
		PhaseAuction: "auction",
		PhaseClosed:  "closed",
	}
	for phase, want := range cases {
		if got := phase.String(); got != want {
			t.Errorf("SessionPhase(%d).String(): got %q, want %q", phase, got, want)
		}
	}
}

func TestSessionPhase_UnknownForOutOfRange(t *testing.T) {
	if got := SessionPhase(99).String(); got != "invalid" {
		t.Errorf("String() for out-of-range: got %q, want invalid", got)
	}
}

func TestMapSessionKindToPhase_KnownKinds(t *testing.T) {
	cases := map[emit.SessionEventKind]SessionPhase{
		emit.SessionOpened:       PhaseOpen,
		emit.AuctionWindowOpened: PhaseAuction,
		emit.SessionClosed:       PhaseClosed,
	}
	for kind, want := range cases {
		if got := mapSessionKindToPhase(kind); got != want {
			t.Errorf("mapSessionKindToPhase(%d): got %d, want %d", kind, got, want)
		}
	}
}

func TestMapSessionKindToPhase_UnknownFallsBackToUnknown(t *testing.T) {
	// Defensive: an out-of-range kind must not panic — skip-and-log,
	// phase stays unchanged in practice (the writer stores Unknown).
	if got := mapSessionKindToPhase(emit.SessionEventKind(99)); got != PhaseUnknown {
		t.Errorf("unknown kind: got %d, want PhaseUnknown", got)
	}
}

// --- NewMarketSnapshot ------------------------------------------------

func TestNewMarketSnapshot_InitialState(t *testing.T) {
	snap := NewMarketSnapshot("E1")

	if snap.Exch() != "E1" {
		t.Errorf("Exch: got %q, want E1", snap.Exch())
	}
	if snap.Phase() != PhaseUnknown {
		t.Errorf("Phase: got %d, want PhaseUnknown", snap.Phase())
	}

	// Reader miss on any instrument before any writes.
	if _, ok := snap.BestAsk("ACME"); ok {
		t.Error("BestAsk on empty snap: got ok=true, want false")
	}
	if _, ok := snap.LastPrice("ACME"); ok {
		t.Error("LastPrice on empty snap: got ok=true, want false")
	}
	if got := snap.Get("ACME"); got != nil {
		t.Errorf("Get on empty snap: got %+v, want nil", got)
	}
}

// --- SetLast ----------------------------------------------------------

func TestSetLast_FirstCall_StoresPriceAndHasLast(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050)

	price, ok := snap.LastPrice("ACME")
	if !ok {
		t.Fatal("LastPrice: got ok=false, want true")
	}
	if price != 10_050 {
		t.Errorf("LastPrice: got %d, want 10050", price)
	}

	instSnap := snap.Get("ACME")
	if instSnap == nil {
		t.Fatal("Get: got nil")
	}
	if !instSnap.HasLast || instSnap.LastPrice != 10_050 {
		t.Errorf("InstSnap: got HasLast=%v LastPrice=%d, want (true, 10050)",
			instSnap.HasLast, instSnap.LastPrice)
	}
}

func TestSetLast_ZeroPriceStillRegistersHasLast(t *testing.T) {
	// 0 is a valid tick price. HasLast=true distinguishes it from
	// "never traded" (EXCHANGE_11 §7.1).
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 0)
	price, ok := snap.LastPrice("ACME")
	if !ok {
		t.Fatal("LastPrice(0): ok=false — expected has-last on a zero price")
	}
	if price != 0 {
		t.Errorf("LastPrice: got %d, want 0", price)
	}
}

func TestSetLast_DuringOpenPhase_SeedsSessionOpen(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050)

	instSnap := snap.Get("ACME")
	if !instSnap.HasOpen {
		t.Error("HasOpen: got false, want true (seed on first trade in open phase)")
	}
	if instSnap.SessionOpen != 10_050 {
		t.Errorf("SessionOpen: got %d, want 10050 (first trade's price)", instSnap.SessionOpen)
	}
}

func TestSetLast_DuringUnknownPhase_DoesNotSeedSessionOpen(t *testing.T) {
	// Phase is Unknown until the first ApplySession. A trade arriving
	// before any session event does not seed sessionOpen (we have no
	// session base to anchor against).
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050)

	instSnap := snap.Get("ACME")
	if instSnap.HasOpen {
		t.Error("HasOpen: got true, want false (no SessionOpened yet)")
	}
}

func TestSetLast_DuringClosedPhase_DoesNotSeedSessionOpen(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.ApplySession(emit.SessionClosed)
	snap.SetLast("ACME", 10_050)

	instSnap := snap.Get("ACME")
	if instSnap.HasOpen {
		t.Error("HasOpen: got true, want false (phase is closed)")
	}
}

func TestSetLast_SecondCallInSameSession_DoesNotReseedSessionOpen(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050) // first trade — seeds
	snap.SetLast("ACME", 11_000) // second trade — must NOT re-seed

	instSnap := snap.Get("ACME")
	if instSnap.SessionOpen != 10_050 {
		t.Errorf("SessionOpen: got %d, want 10050 (seeded by first trade; subsequent trades must not shift it)",
			instSnap.SessionOpen)
	}
	if instSnap.LastPrice != 11_000 {
		t.Errorf("LastPrice: got %d, want 11000 (latest trade)", instSnap.LastPrice)
	}
}

// --- SetQuote ---------------------------------------------------------

func TestSetQuote_StoresQuote_BestAskExposed(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{
		BestBid: 10_040, BidQty: 100, HasBid: true,
		BestAsk: 10_060, AskQty: 200, HasAsk: true,
	})

	ask, ok := snap.BestAsk("ACME")
	if !ok {
		t.Fatal("BestAsk: ok=false, want true")
	}
	if ask != 10_060 {
		t.Errorf("BestAsk: got %d, want 10060", ask)
	}
}

func TestSetQuote_HasAskFalse_BestAskMiss(t *testing.T) {
	// HasAsk=false means empty ask book — BestAsk must miss even though
	// BestAsk field holds the stale zero-value (0 is a valid price so
	// returning (0,true) would be wrong).
	snap := NewMarketSnapshot("E1")
	snap.SetQuote("ACME", book.QuoteState{
		BestBid: 10_040, BidQty: 100, HasBid: true,
		HasAsk: false,
	})
	if _, ok := snap.BestAsk("ACME"); ok {
		t.Error("BestAsk: ok=true, want false (HasAsk=false)")
	}
}

func TestSetQuote_DoesNotTouchLastPrice(t *testing.T) {
	// Writers are per-field — SetQuote must leave LastPrice/HasLast as-is.
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 10_050)
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})
	if price, ok := snap.LastPrice("ACME"); !ok || price != 10_050 {
		t.Errorf("LastPrice after SetQuote: got (%d, %v), want (10050, true)", price, ok)
	}
}

// --- SetDepth ---------------------------------------------------------

func TestSetDepth_StoresDepth(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	depth := emit.DepthSnapshot{
		Bids: []emit.DepthLevel{{Price: 10_040, TotalQty: 100}},
		Asks: []emit.DepthLevel{{Price: 10_060, TotalQty: 200}, {Price: 10_070, TotalQty: 150}},
	}
	snap.SetDepth("ACME", depth)

	got := snap.Get("ACME")
	if len(got.Depth.Bids) != 1 || len(got.Depth.Asks) != 2 {
		t.Fatalf("Depth sizes: got (bids=%d asks=%d), want (1, 2)",
			len(got.Depth.Bids), len(got.Depth.Asks))
	}
	if got.Depth.Asks[0].Price != 10_060 {
		t.Errorf("Asks[0].Price: got %d, want 10060", got.Depth.Asks[0].Price)
	}
}

func TestSetDepth_ReplacesNotMerges(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.SetDepth("ACME", emit.DepthSnapshot{
		Bids: []emit.DepthLevel{{Price: 10_040, TotalQty: 100}},
		Asks: []emit.DepthLevel{{Price: 10_060, TotalQty: 200}},
	})
	snap.SetDepth("ACME", emit.DepthSnapshot{
		Bids: []emit.DepthLevel{{Price: 10_030, TotalQty: 50}},
		Asks: []emit.DepthLevel{{Price: 10_070, TotalQty: 300}},
	})

	got := snap.Get("ACME")
	if got.Depth.Bids[0].Price != 10_030 {
		t.Errorf("Bids after replacement: got %d, want 10030 (replace, not merge)",
			got.Depth.Bids[0].Price)
	}
}

// --- ApplySession -----------------------------------------------------

func TestApplySession_UpdatesPhase(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	cases := []struct {
		kind      emit.SessionEventKind
		wantPhase SessionPhase
	}{
		{emit.SessionOpened, PhaseOpen},
		{emit.AuctionWindowOpened, PhaseAuction},
		{emit.SessionClosed, PhaseClosed},
	}
	for _, tc := range cases {
		snap.ApplySession(tc.kind)
		if got := snap.Phase(); got != tc.wantPhase {
			t.Errorf("after ApplySession(%d): phase=%d, want %d", tc.kind, got, tc.wantPhase)
		}
	}
}

func TestApplySession_SessionOpened_ResetsAllInstrumentsHasOpen(t *testing.T) {
	// Seed two instruments with HasOpen=true, then re-open the session;
	// both must have HasOpen=false afterwards so the first trade of the
	// new session re-seeds (day-change base).
	snap := NewMarketSnapshot("E1")
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050) // seeds HasOpen for ACME
	snap.SetLast("BETA", 20_050) // seeds HasOpen for BETA

	snap.ApplySession(emit.SessionClosed)
	snap.ApplySession(emit.SessionOpened) // new day

	for _, instrument := range []contract.InstrumentID{"ACME", "BETA"} {
		got := snap.Get(instrument)
		if got.HasOpen {
			t.Errorf("%s HasOpen after re-open: got true, want false (per-instrument reset)", instrument)
		}
		if got.SessionOpen != 0 {
			t.Errorf("%s SessionOpen after re-open: got %d, want 0 (reset)", instrument, got.SessionOpen)
		}
	}
}

func TestApplySession_SessionClosed_DoesNotClearSessionOpen(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050)
	snap.ApplySession(emit.SessionClosed)

	got := snap.Get("ACME")
	if !got.HasOpen || got.SessionOpen != 10_050 {
		t.Errorf("SessionOpen after SessionClosed: got (HasOpen=%v, SessionOpen=%d), want (true, 10050) — close must NOT clear",
			got.HasOpen, got.SessionOpen)
	}
}

// --- tape-learned instrument map -------------------------------------

func TestInstrumentMap_TapeLearned_GrowsOnInsert(t *testing.T) {
	// L7 provides no instrument list (E3). Instruments appear via the
	// first SetX call that names them; the map copy-on-insert grows.
	snap := NewMarketSnapshot("E1")
	for i := 0; i < 5; i++ {
		snap.SetLast(contract.InstrumentID(fmt.Sprintf("ACME-%d", i)), contract.Ticks(1000+i))
	}
	for i := 0; i < 5; i++ {
		instrument := contract.InstrumentID(fmt.Sprintf("ACME-%d", i))
		price, ok := snap.LastPrice(instrument)
		if !ok {
			t.Errorf("%s: LastPrice ok=false after SetLast — instrument not learned", instrument)
			continue
		}
		if price != contract.Ticks(1000+i) {
			t.Errorf("%s: price got %d, want %d", instrument, price, 1000+i)
		}
	}
}

// --- Get: shape of the pointer returned ------------------------------

func TestGet_ReturnsFreshSnapshotAfterWrite(t *testing.T) {
	// CoW: a Get BEFORE a mutation returns the pre-mutation state; a
	// Get AFTER returns the post-mutation state. The earlier pointer
	// retains the earlier state (immutable).
	snap := NewMarketSnapshot("E1")
	snap.SetLast("ACME", 100)
	before := snap.Get("ACME")

	snap.SetLast("ACME", 200)
	after := snap.Get("ACME")

	if before.LastPrice != 100 {
		t.Errorf("earlier Get: LastPrice got %d, want 100 (immutable retention)", before.LastPrice)
	}
	if after.LastPrice != 200 {
		t.Errorf("later Get: LastPrice got %d, want 200", after.LastPrice)
	}
	if before == after {
		t.Error("CoW: before and after Get returned the SAME pointer — writer mutated in place")
	}
}

func TestGet_UnknownInstrument_ReturnsNil(t *testing.T) {
	snap := NewMarketSnapshot("E1")
	if got := snap.Get("NEVER-SEEN"); got != nil {
		t.Errorf("Get on unknown instrument: got %+v, want nil", got)
	}
}

// --- determinism -----------------------------------------------------

// Same sequence of writes on two independent snapshots yields the same
// observable state. Root CLAUDE.md: "add a determinism check (run a
// scenario twice, assert identical output) for stateful parts."
func TestMarketSnapshot_SameSequence_YieldsSameObservableState(t *testing.T) {
	build := func() *MarketSnapshot {
		snap := NewMarketSnapshot("E1")
		snap.ApplySession(emit.SessionOpened)
		for i := 0; i < 10; i++ {
			instrument := contract.InstrumentID(fmt.Sprintf("ACME-%d", i))
			snap.SetLast(instrument, contract.Ticks(1000+i))
			snap.SetQuote(instrument, book.QuoteState{
				BestBid: contract.Ticks(999 + i), HasBid: true,
				BestAsk: contract.Ticks(1001 + i), HasAsk: true,
			})
			snap.SetDepth(instrument, emit.DepthSnapshot{
				Bids: []emit.DepthLevel{{Price: contract.Ticks(999 + i), TotalQty: 100}},
				Asks: []emit.DepthLevel{{Price: contract.Ticks(1001 + i), TotalQty: 200}},
			})
		}
		return snap
	}
	first := build()
	second := build()

	for i := 0; i < 10; i++ {
		instrument := contract.InstrumentID(fmt.Sprintf("ACME-%d", i))
		a := first.Get(instrument)
		b := second.Get(instrument)
		if a == nil || b == nil {
			t.Fatalf("%s: nil InstSnap", instrument)
		}
		if a.LastPrice != b.LastPrice || a.HasLast != b.HasLast {
			t.Errorf("%s LastPrice/HasLast: first=(%d,%v) second=(%d,%v)",
				instrument, a.LastPrice, a.HasLast, b.LastPrice, b.HasLast)
		}
		if a.Quote.BestAsk != b.Quote.BestAsk || a.Quote.HasAsk != b.Quote.HasAsk {
			t.Errorf("%s Quote divergence", instrument)
		}
		if a.SessionOpen != b.SessionOpen || a.HasOpen != b.HasOpen {
			t.Errorf("%s SessionOpen: first=(%d,%v) second=(%d,%v)",
				instrument, a.SessionOpen, a.HasOpen, b.SessionOpen, b.HasOpen)
		}
	}
	if first.Phase() != second.Phase() {
		t.Errorf("Phase: first=%d second=%d", first.Phase(), second.Phase())
	}
}

// --- -race gate ------------------------------------------------------

// The whole point of CoW: 1 writer mutating + many readers Loading
// must be race-free. Spawn N readers calling BestAsk/LastPrice/Get in
// a tight loop while one writer rolls through SetLast/SetQuote/SetDepth
// across M instruments. Readers also read Depth slices (len + indexed
// access) — the design's torn-read concern. If -race fires, something
// is broken.
func TestMarketSnapshot_ConcurrentReadsVsWrites_RaceFree(t *testing.T) {
	snap := NewMarketSnapshot("E1")

	const (
		instrumentCount = 10
		writeIterations = 2_000
		readerCount     = 4
	)

	instruments := make([]contract.InstrumentID, instrumentCount)
	for i := range instruments {
		instruments[i] = contract.InstrumentID(fmt.Sprintf("ACME-%d", i))
	}

	// Signal to readers that writes are done. atomic.Bool is race-safe
	// so the readers' poll loop doesn't race with the writer's final set.
	var writesDone atomic.Bool
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		for iter := 0; iter < writeIterations; iter++ {
			instrument := instruments[iter%instrumentCount]
			snap.SetLast(instrument, contract.Ticks(iter))
			snap.SetQuote(instrument, book.QuoteState{
				BestBid: contract.Ticks(iter), HasBid: true,
				BestAsk: contract.Ticks(iter + 1), HasAsk: true,
			})
			// Depth slice length varies iter-to-iter so a torn read
			// (bids/asks len inconsistent with contents) would be
			// visible as an index-out-of-range if present.
			depthSize := 1 + iter%3
			bids := make([]emit.DepthLevel, depthSize)
			asks := make([]emit.DepthLevel, depthSize)
			for i := 0; i < depthSize; i++ {
				bids[i] = emit.DepthLevel{Price: contract.Ticks(iter - i), TotalQty: 100}
				asks[i] = emit.DepthLevel{Price: contract.Ticks(iter + 1 + i), TotalQty: 100}
			}
			snap.SetDepth(instrument, emit.DepthSnapshot{Bids: bids, Asks: asks})
		}
		writesDone.Store(true)
	}()

	readerSync := make(chan struct{}, readerCount)
	for r := 0; r < readerCount; r++ {
		go func() {
			defer func() { readerSync <- struct{}{} }()
			for !writesDone.Load() {
				for _, instrument := range instruments {
					_, _ = snap.BestAsk(instrument)
					_, _ = snap.LastPrice(instrument)
					got := snap.Get(instrument)
					if got != nil {
						// Walk Depth slices in full — a torn read of
						// len vs contents would panic (OOB) or race.
						for i := range got.Depth.Bids {
							_ = got.Depth.Bids[i].Price
						}
						for i := range got.Depth.Asks {
							_ = got.Depth.Asks[i].Price
						}
					}
				}
			}
		}()
	}

	<-writerDone
	for r := 0; r < readerCount; r++ {
		<-readerSync
	}
	// If -race flagged nothing and no panic fired, we pass.
}
