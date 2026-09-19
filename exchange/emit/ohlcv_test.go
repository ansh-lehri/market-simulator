package emit

import (
	"testing"
	"time"

	"simulator/contract"
)

// bucketStart is a fixed instant used across the OHLCV tests so the pinned
// values in the golden tables don't depend on clock reads.
var bucketStart = time.Date(2026, 9, 19, 9, 15, 0, 0, time.UTC)

const testInterval = time.Minute

// newTestAggregator seeds lastClose to referencePrice so a pre-first-trade
// flat candle has a real value to emit.
func newTestAggregator(referencePrice contract.Ticks) *OHLCVAggregator {
	return NewOHLCVAggregator("ACME", testInterval, bucketStart, referencePrice)
}

func TestNewOHLCVAggregator_NonPositiveInterval_Panics(t *testing.T) {
	cases := []time.Duration{0, -time.Second}
	for _, badInterval := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic on interval %v", badInterval)
				}
			}()
			NewOHLCVAggregator("ACME", badInterval, bucketStart, 100)
		}()
	}
}

func TestIngestTrade_NonPositivePriceOrQty_Panics(t *testing.T) {
	cases := []struct {
		name  string
		price contract.Ticks
		qty   contract.Qty
	}{
		{"zero price", 0, 5},
		{"negative price", -1, 5},
		{"zero qty", 100, 0},
		{"negative qty", 100, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			aggregator := newTestAggregator(100)
			aggregator.IngestTrade(tc.price, tc.qty)
		})
	}
}

// Table golden for the o/h/l/c/volume fold — each row is a trade sequence and
// the pinned candle values that must emerge on Roll.
func TestIngestAndRoll_FoldGoldenTable(t *testing.T) {
	type trade struct {
		price contract.Ticks
		qty   contract.Qty
	}
	cases := []struct {
		name   string
		trades []trade
		want   Candle
	}{
		{
			name:   "single trade",
			trades: []trade{{100, 5}},
			want: Candle{
				Instrument: "ACME", Start: bucketStart,
				Open: 100, High: 100, Low: 100, Close: 100, Volume: 5,
			},
		},
		{
			name:   "rising then falling",
			trades: []trade{{100, 1}, {105, 2}, {103, 3}, {110, 4}, {102, 5}},
			want: Candle{
				Instrument: "ACME", Start: bucketStart,
				Open: 100, High: 110, Low: 100, Close: 102, Volume: 15,
			},
		},
		{
			name:   "uniform price (auction clearing shape)",
			trades: []trade{{10050, 5}, {10050, 3}, {10050, 8}, {10050, 1}},
			want: Candle{
				Instrument: "ACME", Start: bucketStart,
				Open: 10050, High: 10050, Low: 10050, Close: 10050, Volume: 17,
			},
		},
		{
			name:   "monotonically rising",
			trades: []trade{{100, 1}, {101, 1}, {102, 1}, {103, 1}},
			want: Candle{
				Instrument: "ACME", Start: bucketStart,
				Open: 100, High: 103, Low: 100, Close: 103, Volume: 4,
			},
		},
		{
			name:   "monotonically falling",
			trades: []trade{{100, 1}, {99, 1}, {98, 1}, {97, 1}},
			want: Candle{
				Instrument: "ACME", Start: bucketStart,
				Open: 100, High: 100, Low: 97, Close: 97, Volume: 4,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aggregator := newTestAggregator(0)
			for _, tr := range tc.trades {
				aggregator.IngestTrade(tr.price, tr.qty)
			}
			got := aggregator.Roll(bucketStart.Add(testInterval))
			if got != tc.want {
				t.Fatalf("candle mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

// Roll must return a candle whose Seq is zero — the emitter stamps it. This
// is what makes EmitCandle's Seq stamp meaningful (invariant 3, and matches
// TestEmitCandle_StampsSeqAndMatchesEnvelope in emitter_test.go).
func TestRoll_LeavesSeqZeroForEmitterToStamp(t *testing.T) {
	aggregator := newTestAggregator(100)
	aggregator.IngestTrade(120, 3)
	got := aggregator.Roll(bucketStart.Add(testInterval))
	if got.Seq != 0 {
		t.Fatalf("Roll must leave Seq zero for the emitter to stamp; got %d", got.Seq)
	}
}

// Roll must reset per-bucket state — no trades bleed from bucket N into
// bucket N+1. Second Roll after a fresh sequence proves open/high/low/close
// reflect only the new trades and lastClose from the prior bucket did not
// pollute open.
func TestRoll_ResetsPerBucketStateNoBleed(t *testing.T) {
	aggregator := newTestAggregator(0)

	aggregator.IngestTrade(100, 5)
	aggregator.IngestTrade(110, 3)
	first := aggregator.Roll(bucketStart.Add(testInterval))
	wantFirst := Candle{
		Instrument: "ACME", Start: bucketStart,
		Open: 100, High: 110, Low: 100, Close: 110, Volume: 8,
	}
	if first != wantFirst {
		t.Fatalf("first candle mismatch\n got: %+v\nwant: %+v", first, wantFirst)
	}

	aggregator.IngestTrade(120, 2)
	aggregator.IngestTrade(115, 1)
	second := aggregator.Roll(bucketStart.Add(2 * testInterval))
	wantSecond := Candle{
		Instrument: "ACME", Start: bucketStart.Add(testInterval),
		Open: 120, High: 120, Low: 115, Close: 115, Volume: 3,
	}
	if second != wantSecond {
		t.Fatalf("second candle bled from first\n got: %+v\nwant: %+v", second, wantSecond)
	}
}

// Continuous-tape invariant 5: even with no trades, every interval emits a
// candle — flat at lastClose, volume 0.
func TestRoll_EmptyBucket_FlatCandleAtLastClose(t *testing.T) {
	aggregator := newTestAggregator(9999)

	got := aggregator.Roll(bucketStart.Add(testInterval))
	want := Candle{
		Instrument: "ACME", Start: bucketStart,
		Open: 9999, High: 9999, Low: 9999, Close: 9999, Volume: 0,
	}
	if got != want {
		t.Fatalf("empty-bucket flat candle mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

// Three consecutive Rolls with no IngestTrade between them → three flat
// candles all at the same lastClose. Each candle's Start must equal the prior
// Roll's now argument (continuity across silent intervals).
func TestRoll_ThreeConsecutiveEmptyBuckets_ContinuousTape(t *testing.T) {
	aggregator := newTestAggregator(500)

	tickTimes := []time.Time{
		bucketStart.Add(testInterval),
		bucketStart.Add(2 * testInterval),
		bucketStart.Add(3 * testInterval),
	}
	wantStarts := []time.Time{
		bucketStart,
		bucketStart.Add(testInterval),
		bucketStart.Add(2 * testInterval),
	}
	for index, now := range tickTimes {
		got := aggregator.Roll(now)
		want := Candle{
			Instrument: "ACME", Start: wantStarts[index],
			Open: 500, High: 500, Low: 500, Close: 500, Volume: 0,
		}
		if got != want {
			t.Fatalf("roll %d\n got: %+v\nwant: %+v", index, got, want)
		}
	}
}

// After a bucket with trades then a silent bucket, the silent bucket's flat
// candle must sit at the last trade's price — lastClose survives Roll.
func TestRoll_FlatAfterTrades_UsesLastTradePriceAsLastClose(t *testing.T) {
	aggregator := newTestAggregator(100)
	aggregator.IngestTrade(150, 4)
	aggregator.IngestTrade(155, 6)
	aggregator.Roll(bucketStart.Add(testInterval))

	silent := aggregator.Roll(bucketStart.Add(2 * testInterval))
	want := Candle{
		Instrument: "ACME", Start: bucketStart.Add(testInterval),
		Open: 155, High: 155, Low: 155, Close: 155, Volume: 0,
	}
	if silent != want {
		t.Fatalf("silent-after-trades candle should sit at last trade price\n got: %+v\nwant: %+v", silent, want)
	}
}

// IntervalElapsed boundary — table over deltas around exactly one interval.
func TestIntervalElapsed_Boundary(t *testing.T) {
	aggregator := newTestAggregator(0)
	cases := []struct {
		name  string
		delta time.Duration
		want  bool
	}{
		{"zero", 0, false},
		{"just before interval", testInterval - time.Nanosecond, false},
		{"exactly at interval", testInterval, true},
		{"just after interval", testInterval + time.Nanosecond, true},
		{"many intervals later", 5 * testInterval, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := bucketStart.Add(tc.delta)
			if got := aggregator.IntervalElapsed(now); got != tc.want {
				t.Fatalf("IntervalElapsed(delta=%v) = %v, want %v", tc.delta, got, tc.want)
			}
		})
	}
}

// After Roll, IntervalElapsed is measured against the new bucket's start
// (the now passed to Roll), not the original one.
func TestIntervalElapsed_ResetsAgainstNewBucketStart(t *testing.T) {
	aggregator := newTestAggregator(0)
	newStart := bucketStart.Add(testInterval)
	aggregator.Roll(newStart)

	if aggregator.IntervalElapsed(newStart.Add(testInterval - time.Nanosecond)) {
		t.Fatal("IntervalElapsed true just before the new bucket's interval elapses")
	}
	if !aggregator.IntervalElapsed(newStart.Add(testInterval)) {
		t.Fatal("IntervalElapsed false at the new bucket's interval boundary")
	}
}

// Determinism check (project convention for stateful components): the same
// input sequence yields identical output on two fresh aggregators.
func TestOHLCV_Deterministic(t *testing.T) {
	trades := []struct {
		price contract.Ticks
		qty   contract.Qty
	}{
		{100, 3}, {105, 2}, {110, 1}, {102, 4}, {107, 5},
	}
	runOnce := func() []Candle {
		aggregator := newTestAggregator(50)
		var candles []Candle
		for _, tr := range trades {
			aggregator.IngestTrade(tr.price, tr.qty)
		}
		candles = append(candles, aggregator.Roll(bucketStart.Add(testInterval)))
		candles = append(candles, aggregator.Roll(bucketStart.Add(2*testInterval)))
		candles = append(candles, aggregator.Roll(bucketStart.Add(3*testInterval)))
		return candles
	}
	first := runOnce()
	second := runOnce()
	if len(first) != len(second) {
		t.Fatalf("determinism: len mismatch %d vs %d", len(first), len(second))
	}
	for index := range first {
		if first[index] != second[index] {
			t.Fatalf("determinism: candle %d differs\n  first: %+v\n second: %+v", index, first[index], second[index])
		}
	}
}
