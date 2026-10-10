package marketdata

import (
	"testing"
	"time"
)

// --- ParseTimeframe ---------------------------------------------------

func TestParseTimeframe_ValidAllowlist(t *testing.T) {
	cases := map[string]Timeframe{
		"1m":  TF1m,
		"5m":  TF5m,
		"30m": TF30m,
		"60m": TF60m,
	}
	for wire, want := range cases {
		t.Run(wire, func(t *testing.T) {
			got, ok := ParseTimeframe(wire)
			if !ok {
				t.Fatalf("ParseTimeframe(%q): got ok=false, want true", wire)
			}
			if got != want {
				t.Errorf("ParseTimeframe(%q): got %d, want %d", wire, got, want)
			}
		})
	}
}

func TestParseTimeframe_RejectsOffAllowlist(t *testing.T) {
	// Case-sensitive: "5M" is NOT normalized to "5m". Everything off the
	// {1,5,30,60} allowlist is rejected — the HTTP edge turns that into 400.
	cases := []string{"", "2m", "15m", "90m", "5M", "5min", "5", "1h", "1d"}
	for _, wire := range cases {
		t.Run(wire, func(t *testing.T) {
			got, ok := ParseTimeframe(wire)
			if ok {
				t.Errorf("ParseTimeframe(%q): got ok=true (val=%d), want false", wire, got)
			}
		})
	}
}

func TestTimeframe_DurationMatchesMinutes(t *testing.T) {
	cases := map[Timeframe]time.Duration{
		TF1m:  1 * time.Minute,
		TF5m:  5 * time.Minute,
		TF30m: 30 * time.Minute,
		TF60m: 60 * time.Minute,
	}
	for tf, want := range cases {
		if got := tf.Duration(); got != want {
			t.Errorf("Timeframe(%d).Duration(): got %s, want %s", tf, got, want)
		}
	}
}

// --- bucketStart ------------------------------------------------------

// 1m is pass-through — the returned bucket is the input minute (seconds
// and nanos stripped). Covers the common case + proves the function
// never shifts for the simplest timeframe.
func TestBucketStart_1mPassThrough_StripsSubMinute(t *testing.T) {
	utc := time.UTC
	ts := time.Date(2026, 10, 10, 9, 37, 42, 123_000_000, utc)
	got := bucketStart(ts, TF1m, utc)
	want := time.Date(2026, 10, 10, 9, 37, 0, 0, utc)
	if !got.Equal(want) {
		t.Errorf("bucketStart(1m): got %s, want %s", got, want)
	}
	if got.Nanosecond() != 0 {
		t.Errorf("sub-second not stripped: nsec=%d", got.Nanosecond())
	}
	if got.Second() != 0 {
		t.Errorf("sub-minute not stripped: sec=%d", got.Second())
	}
}

func TestBucketStart_5m_FloorsToNearestFive(t *testing.T) {
	utc := time.UTC
	// 09:07 → 09:05
	got := bucketStart(time.Date(2026, 10, 10, 9, 7, 0, 0, utc), TF5m, utc)
	want := time.Date(2026, 10, 10, 9, 5, 0, 0, utc)
	if !got.Equal(want) {
		t.Errorf("bucketStart(5m, 09:07): got %s, want %s", got, want)
	}
}

func TestBucketStart_30m_FloorsToHalfHour(t *testing.T) {
	utc := time.UTC
	// 09:34 UTC → 09:30 UTC
	got := bucketStart(time.Date(2026, 10, 10, 9, 34, 0, 0, utc), TF30m, utc)
	want := time.Date(2026, 10, 10, 9, 30, 0, 0, utc)
	if !got.Equal(want) {
		t.Errorf("bucketStart(30m, 09:34): got %s, want %s", got, want)
	}
}

func TestBucketStart_60m_FloorsToTopOfHour(t *testing.T) {
	utc := time.UTC
	// 09:34 UTC → 09:00 UTC
	got := bucketStart(time.Date(2026, 10, 10, 9, 34, 0, 0, utc), TF60m, utc)
	want := time.Date(2026, 10, 10, 9, 0, 0, 0, utc)
	if !got.Equal(want) {
		t.Errorf("bucketStart(60m, 09:34): got %s, want %s", got, want)
	}
}

// THE load-bearing case (DESIGN §2.2): under IST (+5:30), 60m buckets
// must start at IST top-of-hour, NOT at UTC top-of-hour. A UTC-aligned
// 60m bucket would start at IST :30 past the hour — wrong on the chart.
//
// time.Time.Truncate aligns in UTC-epoch space; that would give the
// wrong answer here. This test proves we deliberately do NOT use it.
func TestBucketStart_IST_60m_StartsAtISTTopOfHourNotUTCTopOfHour(t *testing.T) {
	istLocation, ok := tzFor("IN")
	if !ok {
		t.Fatal("tzFor(IN) missed — zoneinfo missing on this machine")
	}
	// UTC 03:30 == IST 09:00 (top of the hour, IN).
	// If someone bugged this to align in UTC, the bucket would land at
	// UTC 03:00 == IST 08:30 — half an hour early, a different hour.
	instant := time.Date(2026, 10, 10, 3, 30, 0, 0, time.UTC)
	got := bucketStart(instant, TF60m, istLocation)
	want := time.Date(2026, 10, 10, 9, 0, 0, 0, istLocation)
	if !got.Equal(want) {
		t.Errorf("bucketStart(60m, IST): got %s, want %s (IST top-of-hour)", got, want)
	}
	// Also assert the WRONG alignment (UTC top-of-hour) is NOT what we got.
	wrongUTCAligned := time.Date(2026, 10, 10, 3, 0, 0, 0, time.UTC)
	if got.Equal(wrongUTCAligned) {
		t.Errorf("bucketStart(60m, IST) aligned in UTC — got %s, which equals wrong UTC-aligned %s", got, wrongUTCAligned)
	}
}

// Same subtle case for 30m: UTC :00 and :30 differ from IST :00 and :30
// by the half-hour offset. 30m under IST must land on IST half-hour marks.
func TestBucketStart_IST_30m_StartsAtISTHalfHour(t *testing.T) {
	istLocation, ok := tzFor("IN")
	if !ok {
		t.Fatal("tzFor(IN) missed — zoneinfo missing on this machine")
	}
	// UTC 04:04 == IST 09:34 → IST 30m bucket = IST 09:30 (= UTC 04:00).
	instant := time.Date(2026, 10, 10, 4, 4, 0, 0, time.UTC)
	got := bucketStart(instant, TF30m, istLocation)
	want := time.Date(2026, 10, 10, 9, 30, 0, 0, istLocation)
	if !got.Equal(want) {
		t.Errorf("bucketStart(30m, IST): got %s, want %s", got, want)
	}
}

// --- RollUp -----------------------------------------------------------

// 1m pass-through: fold returns a value-copy slice whose contents equal
// the input one-for-one. The caller cannot mutate the output and corrupt
// a stored row (we copy), but the field values must match exactly.
func TestRollUp_1mPassThrough(t *testing.T) {
	rows := []Candle{
		mkCandle("IN", "ACME", 0),
		mkCandle("IN", "ACME", 1),
		mkCandle("IN", "ACME", 2),
	}
	got := RollUp(rows, TF1m, time.UTC)
	if len(got) != len(rows) {
		t.Fatalf("len: got %d, want %d", len(got), len(rows))
	}
	for index := range rows {
		if !got[index].Start.Equal(rows[index].Start) ||
			got[index].Open != rows[index].Open ||
			got[index].High != rows[index].High ||
			got[index].Low != rows[index].Low ||
			got[index].Close != rows[index].Close ||
			got[index].Volume != rows[index].Volume {
			t.Errorf("row[%d]: got %+v, want %+v", index, got[index], rows[index])
		}
	}
}

func TestRollUp_EmptyInputReturnsEmptySlice(t *testing.T) {
	got := RollUp(nil, TF5m, time.UTC)
	if got == nil {
		t.Error("RollUp(nil): got nil slice, want empty slice (never nil)")
	}
	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}

// 5 minutes feeding one 5m bucket, OHLCV pinned per DESIGN §3.3:
// Open = first row's open; High = running max; Low = running min;
// Close = last row's close; Volume = sum; End = Start + 5min.
func TestRollUp_5m_SingleCompleteBucketOHLCV(t *testing.T) {
	utc := time.UTC
	base := time.Date(2026, 10, 10, 9, 0, 0, 0, utc)
	rows := []Candle{
		{Market: "IN", Instrument: "ACME", Start: base.Add(0 * time.Minute),
			Open: 100, High: 110, Low: 95, Close: 105, Volume: 10},
		{Market: "IN", Instrument: "ACME", Start: base.Add(1 * time.Minute),
			Open: 105, High: 120, Low: 100, Close: 115, Volume: 20},
		{Market: "IN", Instrument: "ACME", Start: base.Add(2 * time.Minute),
			Open: 115, High: 118, Low: 90, Close: 92, Volume: 15},
		{Market: "IN", Instrument: "ACME", Start: base.Add(3 * time.Minute),
			Open: 92, High: 102, Low: 88, Close: 101, Volume: 30},
		{Market: "IN", Instrument: "ACME", Start: base.Add(4 * time.Minute),
			Open: 101, High: 108, Low: 99, Close: 107, Volume: 25},
	}
	got := RollUp(rows, TF5m, utc)
	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1 (one bucket)", len(got))
	}
	bar := got[0]
	if !bar.Start.Equal(base) {
		t.Errorf("Start: got %s, want %s", bar.Start, base)
	}
	if !bar.End.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("End: got %s, want %s (exclusive)", bar.End, base.Add(5*time.Minute))
	}
	if bar.Open != 100 {
		t.Errorf("Open: got %d, want 100 (first row)", bar.Open)
	}
	if bar.High != 120 {
		t.Errorf("High: got %d, want 120 (max across rows)", bar.High)
	}
	if bar.Low != 88 {
		t.Errorf("Low: got %d, want 88 (min across rows)", bar.Low)
	}
	if bar.Close != 107 {
		t.Errorf("Close: got %d, want 107 (last row)", bar.Close)
	}
	if bar.Volume != 100 {
		t.Errorf("Volume: got %d, want 100 (sum)", bar.Volume)
	}
	if bar.Market != "IN" || bar.Instrument != "ACME" {
		t.Errorf("identifiers: got (%q,%q), want (IN,ACME)", bar.Market, bar.Instrument)
	}
}

// A partial bucket (session opens or closes mid-bucket) aggregates only
// the present minutes — no padding.
func TestRollUp_5m_PartialBucketAggregatesPresentMinutes(t *testing.T) {
	utc := time.UTC
	base := time.Date(2026, 10, 10, 9, 0, 0, 0, utc)
	// Only 3 of the 5 minutes in the :00-:04 bucket.
	rows := []Candle{
		{Market: "IN", Instrument: "ACME", Start: base.Add(0 * time.Minute),
			Open: 100, High: 105, Low: 100, Close: 103, Volume: 10},
		{Market: "IN", Instrument: "ACME", Start: base.Add(1 * time.Minute),
			Open: 103, High: 108, Low: 102, Close: 106, Volume: 20},
		{Market: "IN", Instrument: "ACME", Start: base.Add(2 * time.Minute),
			Open: 106, High: 110, Low: 104, Close: 109, Volume: 30},
	}
	got := RollUp(rows, TF5m, utc)
	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1 (one partial bucket)", len(got))
	}
	bar := got[0]
	if bar.Open != 100 || bar.High != 110 || bar.Low != 100 || bar.Close != 109 || bar.Volume != 60 {
		t.Errorf("partial OHLCV: got (O=%d H=%d L=%d C=%d V=%d), want (100 110 100 109 60)",
			bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
	}
	// End remains Start + 5m even for a partial bucket — exclusive end
	// of the WOULD-BE bucket, not the last-present-minute+1.
	if !bar.End.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("End on partial: got %s, want %s", bar.End, base.Add(5*time.Minute))
	}
}

// Two adjacent buckets, no gaps — must produce exactly two bars at the
// right boundaries.
func TestRollUp_5m_TwoAdjacentBuckets(t *testing.T) {
	utc := time.UTC
	base := time.Date(2026, 10, 10, 9, 0, 0, 0, utc)
	rows := make([]Candle, 0, 10)
	for minute := 0; minute < 10; minute++ {
		rows = append(rows, Candle{
			Market: "IN", Instrument: "ACME",
			Start: base.Add(time.Duration(minute) * time.Minute),
			Open:  100, High: 100, Low: 100, Close: 100, Volume: 1,
		})
	}
	got := RollUp(rows, TF5m, utc)
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	if !got[0].Start.Equal(base) {
		t.Errorf("bar[0].Start: got %s, want %s", got[0].Start, base)
	}
	if !got[1].Start.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("bar[1].Start: got %s, want %s", got[1].Start, base.Add(5*time.Minute))
	}
	if got[0].Volume != 5 || got[1].Volume != 5 {
		t.Errorf("volumes: got (%d, %d), want (5, 5)", got[0].Volume, got[1].Volume)
	}
}

// Gap: a missed bucket is OMITTED (not padded with an empty bar). This
// is what renders as a chart gap over night / inter-session breaks.
func TestRollUp_5m_GapOmitsEmptyBucket(t *testing.T) {
	utc := time.UTC
	base := time.Date(2026, 10, 10, 9, 0, 0, 0, utc)
	// Rows for :00-:04 bucket and :10-:14 bucket; the :05-:09 bucket has
	// no rows and must simply not appear.
	rows := []Candle{}
	for minute := 0; minute < 5; minute++ {
		rows = append(rows, Candle{
			Market: "IN", Instrument: "ACME",
			Start: base.Add(time.Duration(minute) * time.Minute),
			Open:  100, High: 100, Low: 100, Close: 100, Volume: 1,
		})
	}
	for minute := 10; minute < 15; minute++ {
		rows = append(rows, Candle{
			Market: "IN", Instrument: "ACME",
			Start: base.Add(time.Duration(minute) * time.Minute),
			Open:  200, High: 200, Low: 200, Close: 200, Volume: 2,
		})
	}
	got := RollUp(rows, TF5m, utc)
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2 (the :05 bucket is a gap, must be omitted)", len(got))
	}
	if !got[0].Start.Equal(base) {
		t.Errorf("bar[0].Start: got %s, want %s", got[0].Start, base)
	}
	if !got[1].Start.Equal(base.Add(10 * time.Minute)) {
		t.Errorf("bar[1].Start: got %s, want %s", got[1].Start, base.Add(10*time.Minute))
	}
}

// End-to-end tz: a 60m rollup under IST must bucket-align to IST
// top-of-hour, not UTC. Feed rows anchored at UTC :30 past a UTC hour
// (= IST top-of-hour) and verify the bar's Start lands on an IST
// top-of-hour instant.
func TestRollUp_60m_IST_BarAlignsToISTHourNotUTC(t *testing.T) {
	istLocation, ok := tzFor("IN")
	if !ok {
		t.Fatal("tzFor(IN) missed")
	}
	// 09:00..09:04 IST (= UTC 03:30..03:34) — 5 minutes in the IST 09:00
	// bucket under 60m.
	istHourStart := time.Date(2026, 10, 10, 9, 0, 0, 0, istLocation)
	rows := make([]Candle, 0, 5)
	for minute := 0; minute < 5; minute++ {
		rows = append(rows, Candle{
			Market: "IN", Instrument: "ACME",
			Start: istHourStart.Add(time.Duration(minute) * time.Minute),
			Open:  100, High: 100, Low: 100, Close: 100, Volume: 1,
		})
	}
	got := RollUp(rows, TF60m, istLocation)
	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}
	if !got[0].Start.Equal(istHourStart) {
		t.Errorf("bar.Start: got %s, want %s (IST top-of-hour)", got[0].Start, istHourStart)
	}
	// Mustn't be the UTC top-of-hour (which would be IST 05:30).
	utcHourStart := time.Date(2026, 10, 10, 3, 0, 0, 0, time.UTC)
	if got[0].Start.Equal(utcHourStart) {
		t.Errorf("bar.Start lands on UTC top-of-hour — bucket aligned in wrong tz")
	}
}

// --- test helper -------------------------------------------------------

// mkCandle is a tiny builder: default non-asserted fields so tests vary
// only what drives their assertion. Separate from the store_test.go
// helper of the same name (that one lives in the store integration
// tests); this one needs no DB.
//
// Note: this exists in the test file so it does not ship with the
// package. store_test.go has its own mkCandle used by DB tests.
//
// Deliberately named the same to keep the vocabulary consistent across
// tests in this package — Go test files share the package scope, so
// this must stay compatible with store_test.go's mkCandle. They are
// identical by design.
