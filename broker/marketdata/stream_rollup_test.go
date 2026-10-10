package marketdata

import (
	"testing"
	"time"

	"simulator/contract"
)

// --- helpers ----------------------------------------------------------

// mkMinuteCandle builds a 1-min closed candle at a given minute offset
// from a per-test anchor instant. Minute-aligned Start, same OHLCV
// defaults across tests unless the test varies them.
func mkMinuteCandle(instrument contract.InstrumentID, anchor time.Time, offsetMinutes int, open, high, low, close int64, volume int64) Candle {
	start := anchor.Add(time.Duration(offsetMinutes) * time.Minute)
	return Candle{
		Market:     "IN",
		Instrument: instrument,
		Start:      start,
		End:        start.Add(time.Minute),
		Open:       open,
		High:       high,
		Low:        low,
		Close:      close,
		Volume:     volume,
	}
}

// barsByTimeframe indexes a slice of finalized bars by their timeframe
// (deduced from End-Start). Useful for asserting "which timeframes
// finalized on this Ingest call?" without caring about slice order.
func barsByTimeframe(bars []Candle) map[Timeframe]Candle {
	indexed := make(map[Timeframe]Candle, len(bars))
	for _, bar := range bars {
		dur := bar.End.Sub(bar.Start)
		tf := Timeframe(int(dur / time.Minute))
		indexed[tf] = bar
	}
	return indexed
}

// --- 1m pass-through --------------------------------------------------

// 1m is pass-through: every Ingest finalizes exactly the 1m bar for
// that candle. Nothing is retained between calls for the 1m slot.
func TestStreamRollup_1m_EmitsOnEveryIngest(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	for minute := 0; minute < 4; minute++ { // 4 ingests, none a 5m/30m/60m boundary
		out := rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 110, 90, 105, 10))
		indexed := barsByTimeframe(out)
		if _, has1m := indexed[TF1m]; !has1m {
			t.Errorf("minute %d: 1m bar missing from output", minute)
		}
		if _, has5m := indexed[TF5m]; has5m {
			t.Errorf("minute %d: 5m finalized unexpectedly (want only on :04)", minute)
		}
		if indexed[TF1m].Open != 100 || indexed[TF1m].Close != 105 || indexed[TF1m].Volume != 10 {
			t.Errorf("minute %d: 1m bar OHLCV wrong: %+v", minute, indexed[TF1m])
		}
	}
}

// --- 5m: last-minute finalization -------------------------------------

// 5m finalizes exactly on the bucket's last minute (:04, :09, ...). The
// four preceding ingests must only yield 1m bars.
func TestStreamRollup_5m_EmitsOnlyOnLastMinuteOfBucket(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	// Minutes :00..:03 — expect ONLY 1m bars.
	for minute := 0; minute < 4; minute++ {
		out := rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
		if len(out) != 1 {
			t.Fatalf("minute %d: emitted %d bars, want exactly 1 (the 1m pass-through)", minute, len(out))
		}
		if out[0].End.Sub(out[0].Start) != time.Minute {
			t.Errorf("minute %d: single bar not 1m: %+v", minute, out[0])
		}
	}
	// Minute :04 — expect 1m + 5m, both finalized.
	out := rollup.Ingest(mkMinuteCandle("ACME", anchor, 4, 100, 100, 100, 100, 1))
	indexed := barsByTimeframe(out)
	if _, has5m := indexed[TF5m]; !has5m {
		t.Fatal(":04 did not emit 5m bar")
	}
	bar5m := indexed[TF5m]
	if !bar5m.Start.Equal(anchor) {
		t.Errorf("5m Start: got %s, want %s", bar5m.Start, anchor)
	}
	if !bar5m.End.Equal(anchor.Add(5 * time.Minute)) {
		t.Errorf("5m End: got %s, want %s (exclusive)", bar5m.End, anchor.Add(5*time.Minute))
	}
}

// Pin the folded OHLCV of a 5m bar: open = first row, high = max, low =
// min, close = last, vol = sum. Mirrors the RollUp pinned test but on
// the streaming path, so a divergence between HTTP and WS folds would
// show up here (DESIGN §3.4: one impl, byte-identical).
func TestStreamRollup_5m_FoldsPinnedOHLCV(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	rollup.Ingest(mkMinuteCandle("ACME", anchor, 0, 100, 110, 95, 105, 10))
	rollup.Ingest(mkMinuteCandle("ACME", anchor, 1, 105, 120, 100, 115, 20))
	rollup.Ingest(mkMinuteCandle("ACME", anchor, 2, 115, 118, 90, 92, 15))
	rollup.Ingest(mkMinuteCandle("ACME", anchor, 3, 92, 102, 88, 101, 30))
	out := rollup.Ingest(mkMinuteCandle("ACME", anchor, 4, 101, 108, 99, 107, 25))

	indexed := barsByTimeframe(out)
	bar, ok := indexed[TF5m]
	if !ok {
		t.Fatal("5m bar missing from :04 output")
	}
	if bar.Open != 100 {
		t.Errorf("Open: got %d, want 100", bar.Open)
	}
	if bar.High != 120 {
		t.Errorf("High: got %d, want 120", bar.High)
	}
	if bar.Low != 88 {
		t.Errorf("Low: got %d, want 88", bar.Low)
	}
	if bar.Close != 107 {
		t.Errorf("Close: got %d, want 107", bar.Close)
	}
	if bar.Volume != 100 {
		t.Errorf("Volume: got %d, want 100 (sum)", bar.Volume)
	}
}

// Minute :59 of an hour-aligned anchor finalizes 1m + 5m + 30m + 60m in
// one Ingest call. The ordered output matches AllTimeframes order
// (1m→60m) so a client reading the bars-slice can rely on it.
func TestStreamRollup_FullHour_FinalMinuteFinalizesAllFourTimeframes(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	for minute := 0; minute < 59; minute++ {
		rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
	}
	out := rollup.Ingest(mkMinuteCandle("ACME", anchor, 59, 100, 100, 100, 100, 1))
	indexed := barsByTimeframe(out)
	for _, tf := range []Timeframe{TF1m, TF5m, TF30m, TF60m} {
		if _, has := indexed[tf]; !has {
			t.Errorf("timeframe %d: expected a finalized bar at :59, missing", tf)
		}
	}
	// Volumes: 1m carries just minute :59's volume; 5m carries 5 minutes'
	// (:55..:59); 30m carries 30 (:30..:59); 60m carries all 60 (:00..:59).
	wantVolumes := map[Timeframe]int64{TF1m: 1, TF5m: 5, TF30m: 30, TF60m: 60}
	for tf, wantVol := range wantVolumes {
		if indexed[tf].Volume != wantVol {
			t.Errorf("tf %d volume: got %d, want %d", tf, indexed[tf].Volume, wantVol)
		}
	}
	// Starts: all four start aligned to their own bucket boundary.
	wantStarts := map[Timeframe]time.Time{
		TF1m:  anchor.Add(59 * time.Minute),
		TF5m:  anchor.Add(55 * time.Minute),
		TF30m: anchor.Add(30 * time.Minute),
		TF60m: anchor,
	}
	for tf, wantStart := range wantStarts {
		if !indexed[tf].Start.Equal(wantStart) {
			t.Errorf("tf %d Start: got %s, want %s", tf, indexed[tf].Start, wantStart)
		}
	}
}

// Output order: 1m → 5m → 30m → 60m within one Ingest call (matches
// AllTimeframes order, driven by the loop). Downstream relies on this
// for predictable WS push ordering.
func TestStreamRollup_FinalMinute_EmitsInAllTimeframesOrder(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	for minute := 0; minute < 59; minute++ {
		rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
	}
	out := rollup.Ingest(mkMinuteCandle("ACME", anchor, 59, 100, 100, 100, 100, 1))
	if len(out) != 4 {
		t.Fatalf("len: got %d, want 4", len(out))
	}
	wantOrder := []Timeframe{TF1m, TF5m, TF30m, TF60m}
	for index, bar := range out {
		dur := bar.End.Sub(bar.Start)
		gotTf := Timeframe(int(dur / time.Minute))
		if gotTf != wantOrder[index] {
			t.Errorf("out[%d]: got tf=%d, want tf=%d", index, gotTf, wantOrder[index])
		}
	}
}

// --- FlushAll ---------------------------------------------------------

// Session-close while a 5m/30m/60m bucket is half-formed → FlushAll
// finalizes the partials and clears. 1m is cleared on each Ingest, so
// nothing to flush there.
func TestStreamRollup_FlushAll_FinalizesPartialBars(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	// 3 minutes of ingest — :00..:02. None finalize 5m/30m/60m
	// (those need :04/:29/:59).
	for minute := 0; minute < 3; minute++ {
		rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
	}
	flushed := rollup.FlushAll()
	indexed := barsByTimeframe(flushed)

	// 1m slot was cleared each Ingest — must NOT appear in flush output.
	if _, has1m := indexed[TF1m]; has1m {
		t.Error("1m bar appeared in FlushAll output (slot should be empty)")
	}
	// 5m/30m/60m partials — each covers 3 minutes.
	for _, tf := range []Timeframe{TF5m, TF30m, TF60m} {
		bar, ok := indexed[tf]
		if !ok {
			t.Errorf("tf %d: partial bar missing from FlushAll output", tf)
			continue
		}
		if bar.Volume != 3 {
			t.Errorf("tf %d: Volume got %d, want 3 (partial, 3 minutes)", tf, bar.Volume)
		}
		if !bar.Start.Equal(anchor) {
			t.Errorf("tf %d: Start got %s, want %s", tf, bar.Start, anchor)
		}
		// End is Start + full tf duration — exclusive end of the WOULD-BE
		// bucket, not of the last-present minute.
		wantEnd := anchor.Add(tf.Duration())
		if !bar.End.Equal(wantEnd) {
			t.Errorf("tf %d: End got %s, want %s (full-bucket exclusive)", tf, bar.End, wantEnd)
		}
	}
}

// After a full-hour Ingest run (every tf's last minute landed) FlushAll
// has nothing to finalize — slots are cleared.
func TestStreamRollup_FlushAll_EmptyWhenAllBucketsAlreadyEmitted(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	for minute := 0; minute < 60; minute++ {
		rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
	}
	flushed := rollup.FlushAll()
	if len(flushed) != 0 {
		t.Errorf("FlushAll after full hour: got %d bars, want 0", len(flushed))
	}
}

// FlushAll on a fresh rollup (no Ingest yet) returns an empty slice.
func TestStreamRollup_FlushAll_NoIngestYieldsEmptySlice(t *testing.T) {
	flushed := newStreamRollup(time.UTC).FlushAll()
	if flushed == nil {
		t.Error("FlushAll: got nil, want empty slice (never nil)")
	}
	if len(flushed) != 0 {
		t.Errorf("FlushAll: got %d bars, want 0", len(flushed))
	}
}

// Each instrument gets its own forming map — a 5m bar closing for ACME
// does not emit for BETA and vice versa.
func TestStreamRollup_MultiInstrument_FormingMapsAreIsolated(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	// Feed ACME minutes :00..:04 (finalizes ACME 5m) and BETA minutes :00..:02
	// (BETA 5m still forming).
	for minute := 0; minute < 5; minute++ {
		rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
	}
	for minute := 0; minute < 3; minute++ {
		rollup.Ingest(mkMinuteCandle("BETA", anchor, minute, 200, 200, 200, 200, 2))
	}
	flushed := rollup.FlushAll()

	// From FlushAll: BETA 5m/30m/60m partials (3 bars) — ACME 5m already
	// emitted mid-stream, so only ACME's partial 30m + 60m remain (2
	// bars). Total 5 bars.
	if len(flushed) != 5 {
		t.Fatalf("len: got %d, want 5 (ACME 30m+60m partials + BETA 5m+30m+60m partials)", len(flushed))
	}
	// Spot-check isolation: no bar carries a mix of ACME and BETA volume.
	for _, bar := range flushed {
		switch bar.Instrument {
		case "ACME":
			if bar.Volume != 5 {
				t.Errorf("ACME partial Volume: got %d, want 5 (5 minutes @ 1 vol)", bar.Volume)
			}
		case "BETA":
			if bar.Volume != 6 {
				t.Errorf("BETA partial Volume: got %d, want 6 (3 minutes @ 2 vol)", bar.Volume)
			}
		default:
			t.Errorf("unexpected instrument: %q", bar.Instrument)
		}
	}
}

// Determinism: replaying the same Ingest sequence twice on fresh
// rollups must yield byte-identical FlushAll outputs. Root CLAUDE.md:
// "add a determinism check (run a scenario twice, assert identical
// output) for stateful parts."
func TestStreamRollup_DeterministicFlushOrder(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)

	runOnce := func() []Candle {
		rollup := newStreamRollup(time.UTC)
		// Deliberately feed instruments in an order that would reveal
		// map-iteration nondeterminism if FlushAll didn't sort: BETA
		// first, then ACME, interleaved.
		for minute := 0; minute < 3; minute++ {
			rollup.Ingest(mkMinuteCandle("BETA", anchor, minute, 200, 200, 200, 200, 2))
			rollup.Ingest(mkMinuteCandle("ACME", anchor, minute, 100, 100, 100, 100, 1))
		}
		return rollup.FlushAll()
	}
	first := runOnce()
	second := runOnce()

	if len(first) != len(second) {
		t.Fatalf("len: first=%d, second=%d", len(first), len(second))
	}
	for index := range first {
		if first[index] != second[index] {
			t.Errorf("[%d] diverged: first=%+v second=%+v", index, first[index], second[index])
		}
	}
	// Also pin the order: all ACME bars precede all BETA bars (sort by
	// instrument asc). Then within each instrument, timeframes in
	// AllTimeframes order.
	wantOrder := []struct {
		instrument contract.InstrumentID
		timeframe  Timeframe
	}{
		{"ACME", TF5m}, {"ACME", TF30m}, {"ACME", TF60m},
		{"BETA", TF5m}, {"BETA", TF30m}, {"BETA", TF60m},
	}
	if len(first) != len(wantOrder) {
		t.Fatalf("want %d bars in deterministic order, got %d", len(wantOrder), len(first))
	}
	for index, want := range wantOrder {
		gotTf := Timeframe(int(first[index].End.Sub(first[index].Start) / time.Minute))
		if first[index].Instrument != want.instrument || gotTf != want.timeframe {
			t.Errorf("[%d]: got (%s, %d), want (%s, %d)",
				index, first[index].Instrument, gotTf, want.instrument, want.timeframe)
		}
	}
}

// --- Defensive gap ---------------------------------------------------

// A candle whose bucket differs from the stale forming bar's bucket
// finalizes the stale (as a partial) and opens a fresh one. Intra-session
// gaps aren't expected (the exchange emits a flat candle every minute)
// but the handler must survive them without panic or data loss on the
// partial.
func TestStreamRollup_DefensiveGap_FinalizesStaleFormingBar(t *testing.T) {
	anchor := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	rollup := newStreamRollup(time.UTC)

	// Minutes :00 and :01 — forming :00-:04 bucket has 2 minutes.
	rollup.Ingest(mkMinuteCandle("ACME", anchor, 0, 100, 100, 100, 100, 1))
	rollup.Ingest(mkMinuteCandle("ACME", anchor, 1, 100, 100, 100, 100, 1))
	// Jump to :10 — the :05-:09 bucket was skipped entirely. The forming
	// 5m bar is stale (bucket = :00, not :10) and must flush as a partial.
	out := rollup.Ingest(mkMinuteCandle("ACME", anchor, 10, 100, 100, 100, 100, 1))

	// Expected on this Ingest:
	//   - 1m bar at :10 (pass-through)
	//   - 5m partial bar at :00 (2 minutes, from the stale forming)
	//     — then a fresh 5m opens at :10 (not yet finalized, :10 is not
	//     :14).
	// So the output must contain BOTH a 1m-bar (End-Start=1m, Start=:10)
	// and a 5m-bar (End-Start=5m, Start=:00, Volume=2).
	var saw1mAt10 bool
	var saw5mPartialAt0 bool
	for _, bar := range out {
		dur := bar.End.Sub(bar.Start)
		switch dur {
		case time.Minute:
			if bar.Start.Equal(anchor.Add(10 * time.Minute)) {
				saw1mAt10 = true
			}
		case 5 * time.Minute:
			if bar.Start.Equal(anchor) && bar.Volume == 2 {
				saw5mPartialAt0 = true
			}
		}
	}
	if !saw1mAt10 {
		t.Error("expected 1m bar at :10 in output")
	}
	if !saw5mPartialAt0 {
		t.Errorf("expected 5m partial at :00 with volume=2; got out=%+v", out)
	}
}
