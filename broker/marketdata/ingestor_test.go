package marketdata

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/emit"
)

// --- fakes + harness --------------------------------------------------

// fakeInserter records every Insert call and can be configured to
// return an error. Thread-safe (ingestor runs on its own goroutine;
// the test asserts from the main goroutine).
type fakeInserter struct {
	mu            sync.Mutex
	calls         []Candle
	errorToReturn error
}

func (fake *fakeInserter) Insert(_ context.Context, candle Candle) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls = append(fake.calls, candle)
	return fake.errorToReturn
}

func (fake *fakeInserter) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return len(fake.calls)
}

func (fake *fakeInserter) copyCalls() []Candle {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	out := make([]Candle, len(fake.calls))
	copy(out, fake.calls)
	return out
}

// ingestorHarness bundles the full per-instance ingestor wiring with
// an in-memory fake inserter and a pre-registered CacheRegistry entry.
// Tests send events on harness.tape, close it, and wait on harness.done
// before asserting state.
type ingestorHarness struct {
	tape           chan emit.TapeEvent
	registry       *CacheRegistry
	marketSnapshot *MarketSnapshot
	rollup         *streamRollup
	fanOut         *FanOut
	inserter       *fakeInserter
	done           chan struct{}
	exchangeID     ExchangeID
}

// newIngestorHarness constructs the trio + registers the snapshot +
// spawns the ingestor. The ingestor runs until the test closes the
// tape (which triggers for-range exit → Remove → return). mode selects
// canonical / on-demand.
func newIngestorHarness(t *testing.T, mode Mode) *ingestorHarness {
	t.Helper()
	tape := make(chan emit.TapeEvent, 32)
	registry := NewCacheRegistry()
	marketSnapshot := NewMarketSnapshot("E1")
	rollup := newStreamRollup(time.UTC)
	fanOut := NewFanOut(marketSnapshot, "IN", time.UTC)
	inserter := &fakeInserter{}
	registry.Put("E1", marketSnapshot)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runMarketDataIngestor(
			tape, "E1", "IN", mode,
			inserter, marketSnapshot, rollup, fanOut, registry,
		)
	}()

	return &ingestorHarness{
		tape:           tape,
		registry:       registry,
		marketSnapshot: marketSnapshot,
		rollup:         rollup,
		fanOut:         fanOut,
		inserter:       inserter,
		done:           done,
		exchangeID:     "E1",
	}
}

// addSubscriber registers a Conn directly in the FanOut's subs/byConn
// maps, bypassing Subscribe's seed path so the test's buffer isn't
// polluted by a seed packet it doesn't care about.
func (harness *ingestorHarness) addSubscriber(subStream stream, instrument contract.InstrumentID, timeframe Timeframe) *Conn {
	conn := &Conn{outbound: make(chan outFrame, 128)}
	key := subKey{Stream: subStream, Instrument: instrument, TF: timeframe}
	harness.fanOut.mu.Lock()
	if harness.fanOut.subs[key] == nil {
		harness.fanOut.subs[key] = make(map[*Conn]struct{})
	}
	harness.fanOut.subs[key][conn] = struct{}{}
	harness.fanOut.byConn[conn] = append(harness.fanOut.byConn[conn], key)
	harness.fanOut.mu.Unlock()
	return conn
}

// closeAndWait signals the ingestor to teardown and blocks until it
// returns. Called at the end of every test (via defer or inline) so
// the subsequent asserts see final state.
func (harness *ingestorHarness) closeAndWait() {
	close(harness.tape)
	<-harness.done
}

// collectBufferedFrames reads out every frame currently queued on
// conn.outbound without blocking. Call AFTER closeAndWait so no new
// frames can arrive mid-drain.
func collectBufferedFrames(conn *Conn) []outFrame {
	queued := len(conn.outbound)
	frames := make([]outFrame, 0, queued)
	for i := 0; i < queued; i++ {
		frames = append(frames, <-conn.outbound)
	}
	return frames
}

// --- lifecycle --------------------------------------------------------

func TestRunMarketDataIngestor_RangeExitsOnTapeClose(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	// No events sent. Just close.
	harness.closeAndWait()
	// If closeAndWait returned, the for-range exited cleanly. Pass.
}

func TestRunMarketDataIngestor_Teardown_RemovesSnapshotFromRegistry(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	if harness.registry.InstanceCount() != 1 {
		t.Fatalf("precondition: InstanceCount=%d, want 1 (WireInstance-like Put ran in harness)", harness.registry.InstanceCount())
	}
	harness.closeAndWait()
	if got := harness.registry.InstanceCount(); got != 0 {
		t.Errorf("after teardown: InstanceCount=%d, want 0 (ingestor must Remove on exit — BODIES §1.8)", got)
	}
}

// --- KindTrade --------------------------------------------------------

func TestRunMarketDataIngestor_KindTrade_UpdatesSnapshotAndPublishesSnapshotFrame(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindTrade,
		Instrument: "ACME",
		Trade:      &emit.PublicTrade{Price: 10_050, Qty: 100},
	}
	harness.closeAndWait()

	if price, ok := harness.marketSnapshot.LastPrice("ACME"); !ok || price != 10_050 {
		t.Errorf("LastPrice after Trade: got (%d, %v), want (10050, true)", price, ok)
	}
	frames := collectBufferedFrames(snapshotSubscriber)
	if len(frames) != 1 {
		t.Fatalf("len(frames): got %d, want 1 (one snapshot push per Trade)", len(frames))
	}
	if _, ok := frames[0].(snapshotFrame); !ok {
		t.Errorf("frame[0]: got %T, want snapshotFrame", frames[0])
	}
}

func TestRunMarketDataIngestor_KindTrade_NilPayload_SkippedNoPanic(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	harness.tape <- emit.TapeEvent{Kind: emit.KindTrade, Instrument: "ACME", Trade: nil}
	harness.closeAndWait()

	if _, ok := harness.marketSnapshot.LastPrice("ACME"); ok {
		t.Error("LastPrice updated despite nil Trade payload — expected skip-and-log")
	}
	if frames := collectBufferedFrames(snapshotSubscriber); len(frames) != 0 {
		t.Errorf("len(frames): got %d, want 0 (nil payload must skip publish)", len(frames))
	}
}

// --- KindQuote --------------------------------------------------------

func TestRunMarketDataIngestor_KindQuote_UpdatesSnapshotAndPublishes(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindQuote,
		Instrument: "ACME",
		Quote: &book.QuoteState{
			BestBid: 10_040, BidQty: 100, HasBid: true,
			BestAsk: 10_060, AskQty: 200, HasAsk: true,
		},
	}
	harness.closeAndWait()

	if ask, ok := harness.marketSnapshot.BestAsk("ACME"); !ok || ask != 10_060 {
		t.Errorf("BestAsk: got (%d, %v), want (10060, true)", ask, ok)
	}
	if frames := collectBufferedFrames(snapshotSubscriber); len(frames) != 1 {
		t.Errorf("len(frames): got %d, want 1", len(frames))
	}
}

// --- KindDepth --------------------------------------------------------

func TestRunMarketDataIngestor_KindDepth_UpdatesSnapshotAndPublishes(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindDepth,
		Instrument: "ACME",
		Depth: &emit.DepthSnapshot{
			Bids: []emit.DepthLevel{{Price: 10_040, TotalQty: 100}},
			Asks: []emit.DepthLevel{{Price: 10_060, TotalQty: 200}},
		},
	}
	harness.closeAndWait()

	snap := harness.marketSnapshot.Get("ACME")
	if snap == nil {
		t.Fatal("Get(ACME): got nil")
	}
	if len(snap.Depth.Asks) != 1 || snap.Depth.Asks[0].Price != 10_060 {
		t.Errorf("Depth not applied: %+v", snap.Depth)
	}
	if frames := collectBufferedFrames(snapshotSubscriber); len(frames) != 1 {
		t.Errorf("len(frames): got %d, want 1", len(frames))
	}
}

// --- KindCandle: canonical persist + forward --------------------------

func TestRunMarketDataIngestor_KindCandle_Canonical_InsertAndForward(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	// Subscribe to 1m — the ingested candle is a 1m closed candle;
	// the streamRollup pass-through emits a 1m finalized bar.
	candleSubscriber := harness.addSubscriber(streamCandle, "ACME", TF1m)

	base := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindCandle,
		Instrument: "ACME",
		Candle: &emit.Candle{
			Instrument: "ACME",
			Start:      base,
			Open:       100, High: 110, Low: 95, Close: 105, Volume: 500,
		},
	}
	harness.closeAndWait()

	// Store.Insert called exactly once (canonical mode).
	if harness.inserter.callCount() != 1 {
		t.Errorf("Insert call count: got %d, want 1 (canonical mode)", harness.inserter.callCount())
	}
	stored := harness.inserter.copyCalls()[0]
	if stored.Market != "IN" || stored.Instrument != "ACME" {
		t.Errorf("stored identifiers: got (%q,%q), want (IN,ACME)", stored.Market, stored.Instrument)
	}
	if !stored.Start.Equal(base) {
		t.Errorf("stored Start: got %s, want %s (as-is from exchange)", stored.Start, base)
	}
	if stored.Volume != 500 {
		t.Errorf("stored Volume: got %d, want 500", stored.Volume)
	}
	// FanOut received the 1m finalized bar (pass-through).
	frames := collectBufferedFrames(candleSubscriber)
	if len(frames) != 1 {
		t.Fatalf("len(frames): got %d, want 1 (1m pass-through)", len(frames))
	}
	if _, ok := frames[0].(candleFrame); !ok {
		t.Errorf("frame[0]: got %T, want candleFrame", frames[0])
	}
}

// --- KindCandle: on-demand skips persist ------------------------------

func TestRunMarketDataIngestor_KindCandle_OnDemand_SkipsInsertStillForwards(t *testing.T) {
	harness := newIngestorHarness(t, ModeOnDemand)
	candleSubscriber := harness.addSubscriber(streamCandle, "ACME", TF1m)

	base := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindCandle,
		Instrument: "ACME",
		Candle: &emit.Candle{
			Instrument: "ACME", Start: base,
			Open: 100, High: 110, Low: 95, Close: 105, Volume: 500,
		},
	}
	harness.closeAndWait()

	if calls := harness.inserter.callCount(); calls != 0 {
		t.Errorf("Insert call count in on-demand mode: got %d, want 0", calls)
	}
	// Still forwards.
	if frames := collectBufferedFrames(candleSubscriber); len(frames) != 1 {
		t.Errorf("len(frames): got %d, want 1 (on-demand still forwards)", len(frames))
	}
}

// --- KindCandle: insert error → log + forward anyway ------------------

// Load-bearing per BODIES §1.6: a store.Insert error must NOT drop the
// publish — the live stream is more important than one missed row.
// (A missing row is the accepted "crash gap," not a hard stop.)
func TestRunMarketDataIngestor_KindCandle_InsertError_ForwardAnyway(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	harness.inserter.errorToReturn = errors.New("simulated DB fault")
	candleSubscriber := harness.addSubscriber(streamCandle, "ACME", TF1m)

	base := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindCandle,
		Instrument: "ACME",
		Candle: &emit.Candle{
			Instrument: "ACME", Start: base,
			Open: 100, High: 100, Low: 100, Close: 100, Volume: 1,
		},
	}
	harness.closeAndWait()

	// Insert WAS attempted (one call, errored).
	if harness.inserter.callCount() != 1 {
		t.Errorf("Insert call count: got %d, want 1 (attempt even on error)", harness.inserter.callCount())
	}
	// Forward STILL happened despite the error.
	frames := collectBufferedFrames(candleSubscriber)
	if len(frames) != 1 {
		t.Errorf("len(frames): got %d, want 1 (publish must forward anyway — BODIES §1.6)", len(frames))
	}
}

// --- KindCandle: last-minute emits higher-TF bars too -----------------

// Walking a 5m window: five 1-min candles land on the ingestor, each
// 1m bar is published; on the 5th, the 5m bar also finalizes and is
// published. Verifies the ingestor correctly forwards EVERY output of
// rollup.Ingest.
func TestRunMarketDataIngestor_KindCandle_LastMinuteOfBucket_PublishesHigherTF(t *testing.T) {
	harness := newIngestorHarness(t, ModeOnDemand) // skip persistence noise
	oneMinuteSubscriber := harness.addSubscriber(streamCandle, "ACME", TF1m)
	fiveMinuteSubscriber := harness.addSubscriber(streamCandle, "ACME", TF5m)

	base := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	for minute := 0; minute < 5; minute++ {
		start := base.Add(time.Duration(minute) * time.Minute)
		harness.tape <- emit.TapeEvent{
			Kind:       emit.KindCandle,
			Instrument: "ACME",
			Candle: &emit.Candle{
				Instrument: "ACME", Start: start,
				Open: 100, High: 100, Low: 100, Close: 100, Volume: 1,
			},
		}
	}
	harness.closeAndWait()

	oneMinuteFrames := collectBufferedFrames(oneMinuteSubscriber)
	if len(oneMinuteFrames) != 5 {
		t.Errorf("1m frames: got %d, want 5 (one per ingested minute)", len(oneMinuteFrames))
	}
	fiveMinuteFrames := collectBufferedFrames(fiveMinuteSubscriber)
	if len(fiveMinuteFrames) != 1 {
		t.Errorf("5m frames: got %d, want 1 (closes on minute :04)", len(fiveMinuteFrames))
	}
}

// --- KindSession ------------------------------------------------------

func TestRunMarketDataIngestor_KindSession_SessionOpened_UpdatesPhase(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindSession,
		Instrument: "ACME",
		Session:    &emit.SessionEvent{Kind: emit.SessionOpened},
	}
	harness.closeAndWait()

	if phase := harness.marketSnapshot.Phase(); phase != PhaseOpen {
		t.Errorf("Phase after SessionOpened: got %d, want PhaseOpen", phase)
	}
	// Snapshot publish on the carried instrument.
	if frames := collectBufferedFrames(snapshotSubscriber); len(frames) != 1 {
		t.Errorf("len(frames): got %d, want 1 (session publish)", len(frames))
	}
}

// SessionClosed flushes any forming higher-TF bars first (BODIES §1.5
// ordering), then applies the phase + publishes snapshot. Here: pass
// 3 minutes of candles (opens 5m/30m/60m forming bars), then send
// SessionClosed → the three partial bars must get published.
func TestRunMarketDataIngestor_KindSession_SessionClosed_FlushesFormingBars(t *testing.T) {
	harness := newIngestorHarness(t, ModeOnDemand)
	fiveMinuteSubscriber := harness.addSubscriber(streamCandle, "ACME", TF5m)
	thirtyMinuteSubscriber := harness.addSubscriber(streamCandle, "ACME", TF30m)
	sixtyMinuteSubscriber := harness.addSubscriber(streamCandle, "ACME", TF60m)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	base := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	// 3 minutes — opens 5m/30m/60m forming bars; none finalize yet.
	for minute := 0; minute < 3; minute++ {
		harness.tape <- emit.TapeEvent{
			Kind:       emit.KindCandle,
			Instrument: "ACME",
			Candle: &emit.Candle{
				Instrument: "ACME",
				Start:      base.Add(time.Duration(minute) * time.Minute),
				Open:       100, High: 100, Low: 100, Close: 100, Volume: 1,
			},
		}
	}
	// SessionClosed → FlushAll on the rollup → three partial bars
	// published.
	harness.tape <- emit.TapeEvent{
		Kind:       emit.KindSession,
		Instrument: "ACME",
		Session:    &emit.SessionEvent{Kind: emit.SessionClosed},
	}
	harness.closeAndWait()

	if len(collectBufferedFrames(fiveMinuteSubscriber)) != 1 {
		t.Error("5m: expected 1 flushed partial bar on SessionClosed")
	}
	if len(collectBufferedFrames(thirtyMinuteSubscriber)) != 1 {
		t.Error("30m: expected 1 flushed partial bar on SessionClosed")
	}
	if len(collectBufferedFrames(sixtyMinuteSubscriber)) != 1 {
		t.Error("60m: expected 1 flushed partial bar on SessionClosed")
	}
	// Phase advanced to PhaseClosed.
	if phase := harness.marketSnapshot.Phase(); phase != PhaseClosed {
		t.Errorf("Phase after SessionClosed: got %d, want PhaseClosed", phase)
	}
	// Snapshot publish (session folds into snapshot via phase).
	if len(collectBufferedFrames(snapshotSubscriber)) != 1 {
		t.Error("snapshot: expected 1 publish after SessionClosed")
	}
}

// --- Unknown Kind: skip-and-log, no panic ----------------------------

func TestRunMarketDataIngestor_UnknownKind_SkippedNoPanic(t *testing.T) {
	harness := newIngestorHarness(t, ModeCanonical)
	snapshotSubscriber := harness.addSubscriber(streamSnapshot, "ACME", 0)

	// Fabricate an out-of-range TapeKind. If the ingestor panicked
	// here, closeAndWait would hang or we'd see a test crash. Instead:
	// skip-and-log, continue.
	harness.tape <- emit.TapeEvent{
		Kind:       emit.TapeKind(99),
		Instrument: "ACME",
	}
	harness.closeAndWait()

	if frames := collectBufferedFrames(snapshotSubscriber); len(frames) != 0 {
		t.Errorf("unknown-kind leaked a frame: got %d, want 0", len(frames))
	}
}

// --- toStoreCandle (unit) --------------------------------------------

func TestToStoreCandle_CopiesFieldsAndDropsSeqAndEnd(t *testing.T) {
	start := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	exchangeCandle := &emit.Candle{
		Instrument: "ACME",
		Start:      start,
		Open:       100, High: 110, Low: 95, Close: 105, Volume: 500,
		Seq: 42, // must be dropped — the DB has no column for it
	}
	stored := toStoreCandle("IN", exchangeCandle)

	if stored.Market != "IN" {
		t.Errorf("Market: got %q, want IN", stored.Market)
	}
	if stored.Instrument != "ACME" {
		t.Errorf("Instrument: got %q, want ACME", stored.Instrument)
	}
	if !stored.Start.Equal(start) {
		t.Errorf("Start: got %s, want %s (as-is)", stored.Start, start)
	}
	if stored.Open != 100 || stored.High != 110 || stored.Low != 95 || stored.Close != 105 || stored.Volume != 500 {
		t.Errorf("OHLCV mismatch: got (O=%d H=%d L=%d C=%d V=%d)",
			stored.Open, stored.High, stored.Low, stored.Close, stored.Volume)
	}
	if !stored.End.IsZero() {
		t.Errorf("End: got %s, want zero (derived on read, not stored — BODIES §1.4)", stored.End)
	}
}
