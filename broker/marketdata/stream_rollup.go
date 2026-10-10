package marketdata

import (
	"sort"
	"time"

	"simulator/contract"
)

// streamRollup is the ingestor-local, single-writer forming-bar store.
// Only the ingestor goroutine touches it, so no lock is needed — same
// discipline as the exchange's OHLCVAggregator (BODIES §1.2).
//
// Per (instrument, timeframe) at most one forming bar is held; it is
// finalized + cleared the moment its last-minute candle lands (Ingest
// detects this via a bucket-end comparison in LOCAL tz) OR on FlushAll
// (SessionClosed — the only way a mid-bucket final 30m/60m bar closes).
//
// Timeframe 1m acts as pass-through: each incoming 1-min candle is both
// the first and last minute of its 1m bucket, so the 1m slot opens and
// immediately finalizes on every Ingest call. The slot is cleared each
// time, so forming[inst][TF1m] is effectively never retained between
// calls — kept in the loop for shape consistency with the other three.
type streamRollup struct {
	location *time.Location
	forming  map[contract.InstrumentID]map[Timeframe]*Candle
}

// newStreamRollup builds the forming store for a given market tz. The
// ingestor is the sole writer; passing location at construction keeps
// the hot path from re-resolving the tz per candle.
func newStreamRollup(location *time.Location) *streamRollup {
	return &streamRollup{
		location: location,
		forming:  make(map[contract.InstrumentID]map[Timeframe]*Candle),
	}
}

// Ingest folds one closed 1-min candle into all four forming bars (R1:
// fold every timeframe always). Returns every bar that finalized on
// this call — zero-lag emission, never waits for the next bucket to
// start:
//   - tf == 1m: always 1 bar (pass-through — the incoming minute IS the
//     finalized 1m bar).
//   - tf > 1m: 1 bar iff the incoming minute is the bucket's last
//     minute (in local wall-clock).
//
// A defensive gap (forming's bucket != this candle's bucket, when a
// non-last-minute candle lands) finalizes the stale forming bar and
// opens a fresh one for the new bucket — mid-session gaps aren't
// expected (the exchange emits a flat candle every minute,
// EXCHANGE_19 §5) but handling them keeps the stream robust.
//
// Returned bars are value copies; the caller may freely publish them
// without aliasing into forming state.
func (rollup *streamRollup) Ingest(candle Candle) []Candle {
	perInstrument, instrumentKnown := rollup.forming[candle.Instrument]
	if !instrumentKnown {
		perInstrument = make(map[Timeframe]*Candle, len(AllTimeframes))
		rollup.forming[candle.Instrument] = perInstrument
	}
	// Normalize the incoming candle's time to 1-minute LOCAL granularity
	// once; used both to drive bucket membership and to detect "is this
	// the bucket's last minute?" without being bitten by sub-second drift
	// in candle.Start.
	candleMinuteLocal := bucketStart(candle.Start, TF1m, rollup.location)

	out := make([]Candle, 0, len(AllTimeframes))
	for _, timeframe := range AllTimeframes {
		bucket := bucketStart(candle.Start, timeframe, rollup.location)
		formingBar := perInstrument[timeframe]
		if formingBar == nil || !formingBar.Start.Equal(bucket) {
			// Fresh bucket (or defensive gap): finalize stale if any,
			// open a new forming bar.
			if formingBar != nil {
				out = append(out, *formingBar)
			}
			formingBar = openBar(bucket, timeframe, candle)
			perInstrument[timeframe] = formingBar
		} else {
			foldIn(formingBar, candle)
		}
		// Last minute of this bucket? Finalize + clear. For tf=1m,
		// lastMinute == bucket, so this always fires on the same iteration
		// that opened the bar — pass-through behavior.
		lastMinute := bucket.Add(time.Duration(timeframe-1) * time.Minute)
		if candleMinuteLocal.Equal(lastMinute) {
			out = append(out, *formingBar)
			perInstrument[timeframe] = nil
		}
	}
	return out
}

// FlushAll finalizes every non-nil forming bar across every instrument
// and clears them. Called on SessionClosed (BODIES §1.7 / IMPL §4.3) —
// the only way the final (possibly partial) 30m / 60m bar of the day
// closes, since mid-bucket it has no "last minute" trigger.
//
// Output is sorted by (instrument asc, tf asc) for determinism — matches
// the root CLAUDE.md determinism-check gate for stateful parts. Replay
// a session twice and FlushAll returns byte-identical slices.
//
// Returned bars are value copies.
func (rollup *streamRollup) FlushAll() []Candle {
	out := make([]Candle, 0)
	instruments := make([]contract.InstrumentID, 0, len(rollup.forming))
	for instrument := range rollup.forming {
		instruments = append(instruments, instrument)
	}
	sort.Slice(instruments, func(i, j int) bool { return instruments[i] < instruments[j] })

	for _, instrument := range instruments {
		perInstrument := rollup.forming[instrument]
		for _, timeframe := range AllTimeframes {
			formingBar := perInstrument[timeframe]
			if formingBar == nil {
				continue
			}
			out = append(out, *formingBar)
			perInstrument[timeframe] = nil
		}
	}
	return out
}

// openBar builds a fresh forming bar from one 1-min candle at the given
// bucket boundary. Market + Instrument come from the candle (the
// ingestor stamped Market before calling); End = bucketStartAt + tf
// (exclusive, DESIGN §1.4 / IMPL §1.4).
func openBar(bucketStartAt time.Time, timeframe Timeframe, candle Candle) *Candle {
	return &Candle{
		Market:     candle.Market,
		Instrument: candle.Instrument,
		Start:      bucketStartAt,
		End:        bucketStartAt.Add(timeframe.Duration()),
		Open:       candle.Open,
		High:       candle.High,
		Low:        candle.Low,
		Close:      candle.Close,
		Volume:     candle.Volume,
	}
}

// foldIn merges one 1-min candle into an existing forming bar — high is
// a running max, low a running min, close tracks the latest, volume is
// cumulative. Open is set once at openBar and never touched again (first
// minute's open, DESIGN §3.3).
func foldIn(formingBar *Candle, candle Candle) {
	if candle.High > formingBar.High {
		formingBar.High = candle.High
	}
	if candle.Low < formingBar.Low {
		formingBar.Low = candle.Low
	}
	formingBar.Close = candle.Close
	formingBar.Volume += candle.Volume
}
