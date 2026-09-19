package emit

import (
	"time"

	"simulator/contract"
)

// OHLCVAggregator folds one instrument's trades into a rolling candle and, on
// each interval boundary, produces one Candle to emit — even when no trades
// landed in the bucket (a flat candle at the last known price, so the tape is
// continuous per invariant 5). Per-instrument, held by the instrument's shard
// and mutated only from the shard's serial loop; no lock (EXCHANGE_19 §4, §7).
//
// Seq on returned candles is zero — the emitter stamps it at EmitCandle so
// the tape event and the candle carry one number.
type OHLCVAggregator struct {
	instrument contract.InstrumentID
	interval   time.Duration

	curStart  time.Time
	open      contract.Ticks
	high      contract.Ticks
	low       contract.Ticks
	close     contract.Ticks
	volume    contract.Qty
	hasTrade  bool
	lastClose contract.Ticks
}

// NewOHLCVAggregator builds a fresh aggregator whose first bucket starts at
// start. referencePrice seeds lastClose so a flat candle emitted before the
// first trade lands at a real price (typically the session reference / prior
// close, supplied by the wiring). interval must be > 0.
func NewOHLCVAggregator(
	instrument contract.InstrumentID,
	interval time.Duration,
	start time.Time,
	referencePrice contract.Ticks,
) *OHLCVAggregator {
	if interval <= 0 {
		panic("emit.NewOHLCVAggregator: interval must be > 0")
	}
	return &OHLCVAggregator{
		instrument: instrument,
		interval:   interval,
		curStart:   start,
		lastClose:  referencePrice,
	}
}

// IngestTrade folds one trade into the current bucket. The first trade in a
// bucket sets open/high/low to price; every trade updates high/low/close, adds
// to volume, and refreshes lastClose so a subsequent flat candle lands there.
// price must be > 0 and qty > 0 (matcher's guarantees; violation is a bug).
func (aggregator *OHLCVAggregator) IngestTrade(price contract.Ticks, qty contract.Qty) {
	if price <= 0 {
		panic("emit.OHLCVAggregator.IngestTrade: price must be > 0")
	}
	if qty <= 0 {
		panic("emit.OHLCVAggregator.IngestTrade: qty must be > 0")
	}
	if !aggregator.hasTrade {
		aggregator.open = price
		aggregator.high = price
		aggregator.low = price
		aggregator.hasTrade = true
	}
	if price > aggregator.high {
		aggregator.high = price
	}
	if price < aggregator.low {
		aggregator.low = price
	}
	aggregator.close = price
	aggregator.volume += qty
	aggregator.lastClose = price
}

// IntervalElapsed reports whether now has reached or crossed the current
// bucket's end. Called by the shard's per-second monitor to decide whether to
// Roll.
func (aggregator *OHLCVAggregator) IntervalElapsed(now time.Time) bool {
	return now.Sub(aggregator.curStart) >= aggregator.interval
}

// Roll finalizes the current bucket into a Candle and resets state for the
// next bucket starting at now. If no trade landed in the bucket, the candle
// is flat at lastClose with volume 0 (invariant 5 — continuous tape). Seq is
// left zero for the emitter to stamp.
func (aggregator *OHLCVAggregator) Roll(now time.Time) Candle {
	var candle Candle
	if aggregator.hasTrade {
		candle = Candle{
			Instrument: aggregator.instrument,
			Start:      aggregator.curStart,
			Open:       aggregator.open,
			High:       aggregator.high,
			Low:        aggregator.low,
			Close:      aggregator.close,
			Volume:     aggregator.volume,
		}
	} else {
		candle = Candle{
			Instrument: aggregator.instrument,
			Start:      aggregator.curStart,
			Open:       aggregator.lastClose,
			High:       aggregator.lastClose,
			Low:        aggregator.lastClose,
			Close:      aggregator.lastClose,
			Volume:     0,
		}
	}
	aggregator.curStart = now
	aggregator.hasTrade = false
	aggregator.volume = 0
	return candle
}
