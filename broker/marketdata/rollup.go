package marketdata

import "time"

// Timeframe is the minute-count granularity of a candle bar. Allowlist
// is {1,5,30,60} (DESIGN §3.1); anything else is rejected at the HTTP
// edge (400) or the WS edge (per-sub nack), never silently rounded.
type Timeframe int

const (
	TF1m  Timeframe = 1
	TF5m  Timeframe = 5
	TF30m Timeframe = 30
	TF60m Timeframe = 60
)

// AllTimeframes is the ordered allowlist. The streaming rollup folds
// every candle into all four forming bars (R1): decouples the fold from
// subscription churn at the cost of 4 trivial folds/min/instrument. The
// FanOut drops a push with no subscribers, so cost of over-rolling is
// bounded.
//
// Order matters for the streaming rollup's output: a single Ingest call
// may finalize several timeframes at once (e.g. the :59 of the hour
// closes 1m + 5m + 30m + 60m), and iterating in this slice-order gives
// callers a predictable 1m→60m sequence per instrument.
var AllTimeframes = []Timeframe{TF1m, TF5m, TF30m, TF60m}

// ParseTimeframe maps the wire string ("1m","5m","30m","60m") to a
// Timeframe. Off-allowlist returns (0, false). The HTTP handler surfaces
// this as 400 bad params; the WS handler as a per-sub nack.
//
// Case-sensitive by design — the WS protocol doc pins the lowercase
// forms, and silently normalizing would hide client bugs.
func ParseTimeframe(timeframeString string) (Timeframe, bool) {
	switch timeframeString {
	case "1m":
		return TF1m, true
	case "5m":
		return TF5m, true
	case "30m":
		return TF30m, true
	case "60m":
		return TF60m, true
	}
	return 0, false
}

// Duration returns the timeframe as a time.Duration — used to compute
// bucket End (= Start + Duration, exclusive) and the streaming rollup's
// last-minute check.
func (timeframe Timeframe) Duration() time.Duration {
	return time.Duration(timeframe) * time.Minute
}

// bucketStart aligns ts down to the nearest tf-minute mark in LOCAL
// wall-clock (DESIGN §2.2 — load-bearing). The subtle case is IST
// (+5:30) under 30m / 60m: aligning in UTC would start the Indian hourly
// bar at :30 past the IST hour, which is wrong on the chart. 1m and 5m
// are unaffected by the half-hour offset but run the same local path for
// consistency.
//
// Why not time.Time.Truncate: Truncate aligns in UTC-epoch-relative
// space, not local wall-clock, so for any tz whose offset isn't a
// multiple of tf, Truncate would mis-align 30m / 60m. Computing
// explicitly from the local date+clock sidesteps this.
//
// Seconds and nanoseconds on ts are dropped — a bucket boundary is
// minute-precision by construction.
func bucketStart(ts time.Time, timeframe Timeframe, location *time.Location) time.Time {
	localTime := ts.In(location)
	year, month, day := localTime.Date()
	hour, minute, _ := localTime.Clock()
	timeframeMinutes := int(timeframe)
	minute = (minute / timeframeMinutes) * timeframeMinutes
	return time.Date(year, month, day, hour, minute, 0, 0, location)
}

// RollUp folds oldest-first 1-min rows into tf-minute bars for ONE
// instrument. Returns completed bars only — the forming (current) minute
// is never in the input (DESIGN §3.4). Caller supplies the market tz via
// location so this stays pure (no registry call) and testable.
//
// Semantics (DESIGN §3.3):
//   - Pass-through on tf == TF1m (no fold needed). A fresh value-copy
//     slice is returned so a caller mutating the output cannot alias
//     into stored rows.
//   - Each folded bar's OHLCV: Open = first row's open, High = running
//     max, Low = running min, Close = last row's close, Volume = sum.
//   - End = Start + tf.Duration() (exclusive).
//   - A bucket with no rows is never created → omitted → renders as a
//     chart gap (overnight / inter-session boundary, or a mid-session
//     exchange hiccup).
//   - A partial bucket (session opens or closes mid-bucket) aggregates
//     whatever minutes are present; no padding.
//
// This is the ONE server-side fold shared by HTTP history and live
// streaming (DESIGN §3.4 / D-8) — humans and agents get byte-identical
// bars.
func RollUp(rows []Candle, timeframe Timeframe, location *time.Location) []Candle {
	if timeframe == TF1m {
		passthrough := make([]Candle, len(rows))
		copy(passthrough, rows)
		return passthrough
	}
	if len(rows) == 0 {
		return []Candle{}
	}
	out := make([]Candle, 0, len(rows)/int(timeframe)+1)
	var currentBar Candle
	currentBarSet := false
	for _, row := range rows {
		bucket := bucketStart(row.Start, timeframe, location)
		if !currentBarSet || !currentBar.Start.Equal(bucket) {
			if currentBarSet {
				out = append(out, currentBar)
			}
			currentBar = Candle{
				Market:     row.Market,
				Instrument: row.Instrument,
				Start:      bucket,
				End:        bucket.Add(timeframe.Duration()),
				Open:       row.Open,
				High:       row.High,
				Low:        row.Low,
				Close:      row.Close,
				Volume:     row.Volume,
			}
			currentBarSet = true
			continue
		}
		if row.High > currentBar.High {
			currentBar.High = row.High
		}
		if row.Low < currentBar.Low {
			currentBar.Low = row.Low
		}
		currentBar.Close = row.Close
		currentBar.Volume += row.Volume
	}
	if currentBarSet {
		out = append(out, currentBar)
	}
	return out
}
