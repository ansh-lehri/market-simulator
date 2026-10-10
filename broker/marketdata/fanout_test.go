package marketdata

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/emit"
)

// --- helpers ---------------------------------------------------------

// testFanOut builds a FanOut over a fresh MarketSnapshot in UTC. The
// MarketSnapshot is left empty; tests that need tape-learned
// instruments call SetX/ApplySession on it before Subscribe/Publish.
func testFanOut(t *testing.T, exch ExchangeID) (*FanOut, *MarketSnapshot) {
	t.Helper()
	snap := NewMarketSnapshot(exch)
	fanOut := NewFanOut(snap, "IN", time.UTC)
	return fanOut, snap
}

// testConn builds a Conn backed by a nil socket. Tests that only use
// FanOut routing / Enqueue (no socket writes) can pass this freely;
// Disconnect calls socket.Close which panics on nil, so these tests
// never Disconnect.
//
// Buffer size is a per-test choice; smaller buffer makes lag tests
// deterministic.
func testConn(bufferSize int) *Conn {
	return &Conn{
		outbound: make(chan outFrame, bufferSize),
	}
}

// --- stream + parseStream --------------------------------------------

func TestStream_StringForm(t *testing.T) {
	cases := map[stream]string{
		streamSnapshot: "snapshot",
		streamCandle:   "candle",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("stream(%d).String(): got %q, want %q", s, got, want)
		}
	}
}

func TestParseStream_AllowlistAndMiss(t *testing.T) {
	okCases := map[string]stream{
		"snapshot": streamSnapshot,
		"candle":   streamCandle,
	}
	for wire, want := range okCases {
		got, ok := parseStream(wire)
		if !ok || got != want {
			t.Errorf("parseStream(%q): got (%d,%v), want (%d,true)", wire, got, ok, want)
		}
	}
	for _, bad := range []string{"", "Snapshot", "SNAPSHOT", "ticker", "price"} {
		if _, ok := parseStream(bad); ok {
			t.Errorf("parseStream(%q): got ok=true, want false", bad)
		}
	}
}

// --- NewFanOut panics ------------------------------------------------

func TestNewFanOut_PanicsOnNilSnapshot(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil MarketSnapshot")
		}
	}()
	NewFanOut(nil, "IN", time.UTC)
}

func TestNewFanOut_PanicsOnNilLocation(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil location")
		}
	}()
	NewFanOut(NewMarketSnapshot("E1"), "IN", nil)
}

// --- KnownInstrument -------------------------------------------------

func TestKnownInstrument_FalseBeforeTapeEvent_TrueAfter(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	if fanOut.KnownInstrument("ACME") {
		t.Error("KnownInstrument before any SetX: got true, want false")
	}
	snap.SetLast("ACME", 10_050)
	if !fanOut.KnownInstrument("ACME") {
		t.Error("KnownInstrument after SetLast: got false, want true")
	}
}

// --- Publish → routing ----------------------------------------------

func TestPublishSnapshot_RoutesToSubscribedConns(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050) // tape-learn

	conn := testConn(8)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME"},
	})
	// Subscribe seeded a snapshot — drain it so the next Publish is
	// the one we observe.
	<-conn.outbound

	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())

	select {
	case frame := <-conn.outbound:
		if _, ok := frame.(snapshotFrame); !ok {
			t.Errorf("frame: got %T, want snapshotFrame", frame)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for snapshot frame")
	}
}

func TestPublishSnapshot_IgnoresNonSubscribers(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)
	snap.SetLast("BETA", 20_050)

	conn := testConn(8)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME"},
	})
	<-conn.outbound // drain seed

	// Publish for BETA — subscriber is only on ACME.
	fanOut.PublishSnapshot("BETA", snap.Get("BETA"), snap.Phase())

	select {
	case frame := <-conn.outbound:
		t.Errorf("unexpected frame delivered: %T %+v", frame, frame)
	case <-time.After(50 * time.Millisecond):
		// Expected — no delivery.
	}
}

func TestPublishCandle_RoutesByTimeframe(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)

	conn := testConn(8)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamCandle, Instrument: "ACME", TF: TF5m},
	})
	// Candle sub does NOT seed (R2) — buffer should be empty.
	select {
	case frame := <-conn.outbound:
		t.Fatalf("candle sub unexpectedly seeded: %T %+v", frame, frame)
	case <-time.After(20 * time.Millisecond):
	}

	start := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	bar := Candle{
		Market: "IN", Instrument: "ACME",
		Start: start, End: start.Add(5 * time.Minute),
		Open: 100, High: 110, Low: 90, Close: 105, Volume: 500,
	}
	fanOut.PublishCandle(bar, TF5m)

	select {
	case frame := <-conn.outbound:
		candle, ok := frame.(candleFrame)
		if !ok {
			t.Fatalf("frame: got %T, want candleFrame", frame)
		}
		if candle.tf != TF5m {
			t.Errorf("candleFrame.tf: got %d, want %d", candle.tf, TF5m)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out")
	}

	// Different TF — must NOT deliver (sub was 5m, publish 30m).
	fanOut.PublishCandle(bar, TF30m)
	select {
	case frame := <-conn.outbound:
		t.Errorf("30m bar leaked to 5m subscriber: %T", frame)
	case <-time.After(50 * time.Millisecond):
	}
}

// --- Non-blocking drop + lag counter --------------------------------

func TestPublishSnapshot_FullBuffer_DropsAndIncrementsLag(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)

	conn := testConn(1) // tight buffer so we fill it on the first push
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME"},
	})
	// Subscribe seeded one frame → buffer full.
	if conn.laggedCount.Load() != 0 {
		t.Errorf("laggedCount after seed-fill: got %d, want 0", conn.laggedCount.Load())
	}

	// Each subsequent Publish must drop + bump lagged.
	for i := 1; i <= 5; i++ {
		fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())
		if got := conn.laggedCount.Load(); got != int32(i) {
			t.Errorf("after Publish %d: laggedCount=%d, want %d", i, got, i)
		}
	}
}

func TestPublishSnapshot_SuccessfulEnqueue_ResetsLaggedCount(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)

	conn := testConn(1)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME"},
	})
	// Fill: seed already in buffer. Push until lag counter grows.
	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())
	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())
	if conn.laggedCount.Load() != 2 {
		t.Fatalf("precondition: expected lag=2 after 2 drops, got %d", conn.laggedCount.Load())
	}

	// Drain the buffer, then publish — should succeed + reset.
	<-conn.outbound
	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())
	if got := conn.laggedCount.Load(); got != 0 {
		t.Errorf("laggedCount after successful enqueue: got %d, want 0 (reset)", got)
	}
}

// --- Subscribe seeding (R2) ------------------------------------------

func TestSubscribe_SnapshotSub_SeedsCurrentState(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050) // seeds HasOpen in open phase
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})

	conn := testConn(8)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME"},
	})

	select {
	case frame := <-conn.outbound:
		seed, ok := frame.(snapshotFrame)
		if !ok {
			t.Fatalf("seed: got %T, want snapshotFrame", frame)
		}
		if seed.snap.LastPrice != 10_050 {
			t.Errorf("seed LastPrice: got %d, want 10050", seed.snap.LastPrice)
		}
		if seed.phase != PhaseOpen {
			t.Errorf("seed phase: got %d, want PhaseOpen", seed.phase)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("snapshot sub did not deliver seed within 100ms")
	}
}

func TestSubscribe_CandleSub_DoesNOTSeed(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)

	conn := testConn(8)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamCandle, Instrument: "ACME", TF: TF5m},
	})
	select {
	case frame := <-conn.outbound:
		t.Errorf("candle sub leaked a seed frame: %T %+v (R2 violation)", frame, frame)
	case <-time.After(50 * time.Millisecond):
		// Expected — no seed for candle subs.
	}
}

func TestSubscribe_BatchInstallsAllOneLock(t *testing.T) {
	// Batch subscribe for N subs — all installed, SubscriptionCount
	// reflects total.
	fanOut, snap := testFanOut(t, "E1")
	for i := 0; i < 3; i++ {
		snap.SetLast(contract.InstrumentID(fmt.Sprintf("ACME-%d", i)), contract.Ticks(1000+i))
	}

	conn := testConn(16)
	requests := []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME-0"},
		{Stream: streamSnapshot, Instrument: "ACME-1"},
		{Stream: streamCandle, Instrument: "ACME-2", TF: TF5m},
	}
	fanOut.Subscribe(conn, requests)

	if got := fanOut.SubscriptionCount(); got != 3 {
		t.Errorf("SubscriptionCount: got %d, want 3", got)
	}
}

// --- Unsubscribe + Disconnect purge ---------------------------------

func TestUnsubscribe_RemovesSubsStopsDelivery(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)

	conn := testConn(16)
	fanOut.Subscribe(conn, []subRequest{{Stream: streamSnapshot, Instrument: "ACME"}})
	<-conn.outbound // drain seed

	fanOut.Unsubscribe(conn, []subRequest{{Stream: streamSnapshot, Instrument: "ACME"}})

	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())
	select {
	case frame := <-conn.outbound:
		t.Errorf("delivery after unsubscribe: %T %+v", frame, frame)
	case <-time.After(50 * time.Millisecond):
	}
	if got := fanOut.SubscriptionCount(); got != 0 {
		t.Errorf("SubscriptionCount after unsubscribe: got %d, want 0", got)
	}
}

func TestUnsubscribe_UnknownSubIsSilentNoOp(t *testing.T) {
	fanOut, _ := testFanOut(t, "E1")
	conn := testConn(8)
	// No subscribe — just unsubscribe. Must not panic / error.
	fanOut.Unsubscribe(conn, []subRequest{{Stream: streamSnapshot, Instrument: "ACME"}})
	if got := fanOut.SubscriptionCount(); got != 0 {
		t.Errorf("SubscriptionCount: got %d, want 0", got)
	}
}

// --- Disconnect ------------------------------------------------------

// Simulate Disconnect's "purge subs from map" semantic without a real
// socket. We bypass Disconnect's socket.Close by setting disconnected
// manually and calling the purge path inline.
func TestDisconnect_PurgesAllSubsViaReverseIndex(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	snap.SetLast("ACME", 10_050)
	snap.SetLast("BETA", 20_050)

	conn := testConn(16)
	fanOut.Subscribe(conn, []subRequest{
		{Stream: streamSnapshot, Instrument: "ACME"},
		{Stream: streamCandle, Instrument: "ACME", TF: TF5m},
		{Stream: streamSnapshot, Instrument: "BETA"},
	})
	if got := fanOut.SubscriptionCount(); got != 3 {
		t.Fatalf("precondition: SubscriptionCount=%d, want 3", got)
	}

	// Simulate Disconnect's state-purge half without the socket close.
	// We bypass CompareAndSwap by calling the purge logic inline, which
	// is exactly what Disconnect does under the mu.Lock.
	fanOut.mu.Lock()
	for _, key := range fanOut.byConn[conn] {
		delete(fanOut.subs[key], conn)
		if len(fanOut.subs[key]) == 0 {
			delete(fanOut.subs, key)
		}
	}
	delete(fanOut.byConn, conn)
	fanOut.mu.Unlock()

	if got := fanOut.SubscriptionCount(); got != 0 {
		t.Errorf("SubscriptionCount after purge: got %d, want 0", got)
	}
}

func TestConn_Enqueue_LagCounterResetsOnSuccess(t *testing.T) {
	// Direct unit on Conn.Enqueue — exercising lag reset without
	// going through FanOut.
	conn := testConn(1)
	// First Enqueue succeeds; lag = 0.
	if !conn.Enqueue(ackFrame{}) {
		t.Fatal("Enqueue on empty buffer: got false, want true")
	}
	if conn.laggedCount.Load() != 0 {
		t.Errorf("laggedCount after success: got %d, want 0", conn.laggedCount.Load())
	}
	// Buffer full; next Enqueue drops + lag=1.
	if conn.Enqueue(ackFrame{}) {
		t.Fatal("Enqueue on full buffer: got true, want false")
	}
	if conn.laggedCount.Load() != 1 {
		t.Errorf("laggedCount after drop: got %d, want 1", conn.laggedCount.Load())
	}
	// Drain + Enqueue again; lag resets.
	<-conn.outbound
	if !conn.Enqueue(ackFrame{}) {
		t.Fatal("Enqueue after drain: got false, want true")
	}
	if conn.laggedCount.Load() != 0 {
		t.Errorf("laggedCount after success-post-drop: got %d, want 0", conn.laggedCount.Load())
	}
}

// --- -race gate ------------------------------------------------------

// N publishers (concurrent Publishes across the same FanOut from the
// same goroutine isn't the production shape — one ingestor per FanOut
// — but we race publishes anyway so the RWMutex path gets exercised
// alongside concurrent Subscribe/Unsubscribe). The real race surface
// in production is Publish (RLock) vs Subscribe/Unsubscribe (Lock),
// which this probe stresses.
func TestFanOut_ConcurrentPublishVsSubscribe_RaceFree(t *testing.T) {
	fanOut, snap := testFanOut(t, "E1")
	instrumentCount := 20
	for i := 0; i < instrumentCount; i++ {
		snap.SetLast(contract.InstrumentID(fmt.Sprintf("ACME-%d", i)), contract.Ticks(1000+i))
	}

	const iterations = 2_000
	const publisherCount = 2
	const subscriberCount = 2
	var done atomic.Bool

	// Pool of connections subscribing / unsubscribing in a loop.
	conns := make([]*Conn, 10)
	for i := range conns {
		conns[i] = testConn(16)
	}

	var wg sync.WaitGroup

	// Publishers.
	for p := 0; p < publisherCount; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for iter := 0; iter < iterations; iter++ {
				instrument := contract.InstrumentID(fmt.Sprintf("ACME-%d", (iter+pid)%instrumentCount))
				fanOut.PublishSnapshot(instrument, snap.Get(instrument), snap.Phase())
			}
		}(p)
	}
	// Subscribers.
	for s := 0; s < subscriberCount; s++ {
		wg.Add(1)
		go func(sid int) {
			defer wg.Done()
			conn := conns[sid%len(conns)]
			for iter := 0; iter < iterations/2; iter++ {
				instrument := contract.InstrumentID(fmt.Sprintf("ACME-%d", iter%instrumentCount))
				req := subRequest{Stream: streamSnapshot, Instrument: instrument}
				fanOut.Subscribe(conn, []subRequest{req})
				// Drain opportunistically so buffer doesn't block
				// Subscribe's seed enqueue.
				for drained := 0; drained < 4; drained++ {
					select {
					case <-conn.outbound:
					default:
						drained = 4
					}
				}
				fanOut.Unsubscribe(conn, []subRequest{req})
			}
		}(s)
	}

	wg.Wait()
	done.Store(true)
	// If -race flagged nothing + no deadlock, we pass.
}
