package marketdata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"simulator/contract"
)

// stubCandleStore is an in-memory candleStorePort for engine tests.
// Captures the args of the last RangeRead1m call and returns a
// configured slice (truncated to limit, mimicking the real Store's
// LIMIT contract).
type stubCandleStore struct {
	rows []Candle
	err  error

	gotMarket     string
	gotInstrument contract.InstrumentID
	gotFrom       time.Time
	gotTo         time.Time
	gotAfterTS    *time.Time
	gotLimit      int
	callCount     int
}

func (stub *stubCandleStore) RangeRead1m(
	ctx context.Context,
	market string,
	instrument contract.InstrumentID,
	from, to time.Time,
	afterTS *time.Time,
	limit int,
) ([]Candle, error) {
	stub.callCount++
	stub.gotMarket = market
	stub.gotInstrument = instrument
	stub.gotFrom = from
	stub.gotTo = to
	stub.gotAfterTS = afterTS
	stub.gotLimit = limit
	if stub.err != nil {
		return nil, stub.err
	}
	if limit > 0 && len(stub.rows) > limit {
		return stub.rows[:limit], nil
	}
	return stub.rows, nil
}

// seedCandlesPerMinute builds `count` contiguous 1-min candles starting
// at anchor, in whatever location anchor carries. Trivial OHLCV with
// Volume=1 per minute → a bucket's volume equals its minute count,
// handy for assertions.
func seedCandlesPerMinute(anchor time.Time, count int) []Candle {
	rows := make([]Candle, count)
	for index := 0; index < count; index++ {
		start := anchor.Add(time.Duration(index) * time.Minute)
		rows[index] = Candle{
			Market: "IN", Instrument: "ACME",
			Start: start, End: start.Add(time.Minute),
			Open: 100, High: 100, Low: 100, Close: 100, Volume: 1,
		}
	}
	return rows
}

// ----- happy paths ----------------------------------------------------

// Short batch (Store returned fewer rows than rowLimit → window
// exhausted): return all folded bars, cursor nil.
func TestQueryCandles_ShortBatch_ReturnsAllBars_CursorNil(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	stub := &stubCandleStore{rows: seedCandlesPerMinute(anchor, 50)} // exactly 10 5m buckets
	engine := NewReadEngine(stub)
	page, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: anchor, To: anchor.Add(60 * time.Minute),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if len(page.Candles) != 10 {
		t.Errorf("len: got %d, want 10", len(page.Candles))
	}
	if page.NextCursor != nil {
		t.Errorf("NextCursor: got %v, want nil (short batch)", *page.NextCursor)
	}
	// Engine asks Store for Limit*TF+1 rows (one minute of slack to
	// detect a trailing partial bucket).
	wantRowLimit := 10*5 + 1
	if stub.gotLimit != wantRowLimit {
		t.Errorf("store limit: got %d, want %d (Limit*TF+1)", stub.gotLimit, wantRowLimit)
	}
}

// Full batch (Store returned exactly rowLimit rows → more may exist):
// drop the trailing partial bar; cursor = that bucket's start.
func TestQueryCandles_FullBatch_DropsTrailingBar_CursorPointsAtDropped(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	// 51 1-min rows: 10 full 5m buckets (:00-:49) + 1 minute of the 11th
	// bucket (:50). Store returns exactly rowLimit=51 → full batch; the
	// 11th bar is partial (1 minute) → drop it.
	stub := &stubCandleStore{rows: seedCandlesPerMinute(anchor, 51)}
	engine := NewReadEngine(stub)
	page, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: anchor, To: anchor.Add(60 * time.Minute),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if len(page.Candles) != 10 {
		t.Fatalf("len: got %d, want 10 (last partial dropped)", len(page.Candles))
	}
	if page.NextCursor == nil {
		t.Fatal("NextCursor: got nil, want 11th bucket start (:50)")
	}
	want := anchor.Add(50 * time.Minute)
	if !page.NextCursor.Equal(want) {
		t.Errorf("NextCursor: got %s, want %s (dropped bucket's first minute)",
			*page.NextCursor, want)
	}
}

func TestQueryCandles_EmptyWindow_200EmptyNoCursor(t *testing.T) {
	stub := &stubCandleStore{rows: nil}
	engine := NewReadEngine(stub)
	page, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: time.Now(), To: time.Now().Add(time.Hour),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if page.Candles == nil {
		t.Error("Candles: got nil slice, want empty")
	}
	if len(page.Candles) != 0 {
		t.Errorf("len: got %d, want 0", len(page.Candles))
	}
	if page.NextCursor != nil {
		t.Errorf("NextCursor: got non-nil on empty, want nil")
	}
}

func TestQueryCandles_1mPassThrough(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	stub := &stubCandleStore{rows: seedCandlesPerMinute(anchor, 5)}
	engine := NewReadEngine(stub)
	page, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF1m,
		From: anchor, To: anchor.Add(60 * time.Minute),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if len(page.Candles) != 5 {
		t.Errorf("len: got %d, want 5 (1m pass-through)", len(page.Candles))
	}
	if page.NextCursor != nil {
		t.Errorf("NextCursor: got non-nil, want nil (short batch)")
	}
}

// ----- cursor translation --------------------------------------------

// Load-bearing: the engine's user-visible AfterTS is the dropped bar's
// first minute (inclusive), but the Store's SQL uses strict `> afterTS`.
// Engine must subtract one minute so the dropped bucket's first minute
// IS re-fetched. Without this translation, next-page would skip :50
// entirely and the chart would have a one-minute hole.
func TestQueryCandles_AfterTS_TranslatesCursorMinus1MinForStore(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	stub := &stubCandleStore{rows: nil}
	engine := NewReadEngine(stub)
	cursorFromClient := anchor.Add(50 * time.Minute)
	_, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: anchor, To: anchor.Add(2 * time.Hour),
		AfterTS: &cursorFromClient,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if stub.gotAfterTS == nil {
		t.Fatal("Store received nil afterTS (expected cursor minus 1 min)")
	}
	want := cursorFromClient.Add(-1 * time.Minute)
	if !stub.gotAfterTS.Equal(want) {
		t.Errorf("store afterTS: got %s, want %s (cursor minus 1 min)",
			*stub.gotAfterTS, want)
	}
}

func TestQueryCandles_AfterTSNil_PassesNilToStore(t *testing.T) {
	stub := &stubCandleStore{rows: nil}
	engine := NewReadEngine(stub)
	_, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: time.Now(), To: time.Now().Add(time.Hour),
		AfterTS: nil, Limit: 10,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if stub.gotAfterTS != nil {
		t.Errorf("store afterTS: got %v, want nil (first page)", stub.gotAfterTS)
	}
}

// ----- clamp + error propagation --------------------------------------

func TestQueryCandles_LimitAboveMax_ClampedToMax(t *testing.T) {
	stub := &stubCandleStore{rows: nil}
	engine := NewReadEngine(stub)
	_, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: time.Now(), To: time.Now().Add(time.Hour),
		Limit: 10_000,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	wantRowLimit := maxCandleLimit*int(TF5m) + 1
	if stub.gotLimit != wantRowLimit {
		t.Errorf("store limit: got %d, want %d (clamped to maxCandleLimit*TF+1)",
			stub.gotLimit, wantRowLimit)
	}
}

func TestQueryCandles_StoreError_PropagatesWrappedErrLoadFailed(t *testing.T) {
	wrappedErr := fmt.Errorf("broker/marketdata: test infra fault: %w", ErrLoadFailed)
	stub := &stubCandleStore{err: wrappedErr}
	engine := NewReadEngine(stub)
	_, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: time.Now(), To: time.Now().Add(time.Hour),
		Limit: 10,
	})
	if !errors.Is(err, ErrLoadFailed) {
		t.Errorf("err: got %v, want wraps ErrLoadFailed", err)
	}
}

// ----- trim fallback --------------------------------------------------

// Fallback trim: a gap-rich input can fold to more than query.Limit
// bars even on a short batch. Example: Limit=1, TF=5m, rowLimit=6. Two
// rows at :00 and :10 → fold to 2 partial bars (bucket :00 with 1 min,
// bucket :10 with 1 min). Short batch (2 < 6), but 2 bars > Limit=1 →
// trim; cursor = bars[1].Start = :10.
func TestQueryCandles_GapsOverProduce_TrimFallbackCursorPointsAtDroppedBar(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rows := []Candle{
		{Market: "IN", Instrument: "ACME",
			Start: anchor, End: anchor.Add(time.Minute),
			Open: 100, High: 100, Low: 100, Close: 100, Volume: 1},
		{Market: "IN", Instrument: "ACME",
			Start: anchor.Add(10 * time.Minute), End: anchor.Add(11 * time.Minute),
			Open: 200, High: 200, Low: 200, Close: 200, Volume: 2},
	}
	stub := &stubCandleStore{rows: rows}
	engine := NewReadEngine(stub)
	page, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: anchor, To: anchor.Add(2 * time.Hour),
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if len(page.Candles) != 1 {
		t.Errorf("len: got %d, want 1 (trim fallback)", len(page.Candles))
	}
	if page.NextCursor == nil {
		t.Fatal("NextCursor: got nil, want :10 (first trimmed bar's Start)")
	}
	want := anchor.Add(10 * time.Minute)
	if !page.NextCursor.Equal(want) {
		t.Errorf("NextCursor: got %s, want %s", *page.NextCursor, want)
	}
}

// ----- invariant: unknown market is a dev-fatal panic ----------------

func TestQueryCandles_UnknownMarket_Panics(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Error("expected panic on unknown market (handler-validation invariant)")
		}
	}()
	engine := NewReadEngine(&stubCandleStore{})
	_, _ = engine.QueryCandles(context.Background(), CandleQuery{
		Market: "ZZ", Instrument: "ACME", TF: TF5m,
		From: time.Now(), To: time.Now().Add(time.Hour),
		Limit: 10,
	})
}

// ----- Store args plumbed through ------------------------------------

// The engine's From / To / Market / Instrument pass through unchanged.
// Defends against a refactor that accidentally re-points one of them
// (e.g. wrong struct field).
func TestQueryCandles_PassesQueryArgsThroughToStore(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	stub := &stubCandleStore{rows: nil}
	engine := NewReadEngine(stub)
	_, err := engine.QueryCandles(context.Background(), CandleQuery{
		Market: "IN", Instrument: "ACME", TF: TF5m,
		From: anchor, To: anchor.Add(37 * time.Minute),
		Limit: 7,
	})
	if err != nil {
		t.Fatalf("QueryCandles: %v", err)
	}
	if stub.gotMarket != "IN" {
		t.Errorf("market: got %q, want IN", stub.gotMarket)
	}
	if stub.gotInstrument != "ACME" {
		t.Errorf("instrument: got %q, want ACME", stub.gotInstrument)
	}
	if !stub.gotFrom.Equal(anchor) {
		t.Errorf("from: got %s, want %s", stub.gotFrom, anchor)
	}
	if !stub.gotTo.Equal(anchor.Add(37 * time.Minute)) {
		t.Errorf("to: got %s, want %s", stub.gotTo, anchor.Add(37*time.Minute))
	}
}
