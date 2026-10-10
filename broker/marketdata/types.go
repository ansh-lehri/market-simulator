package marketdata

import (
	"time"

	"simulator/contract"
)

// Candle is the store-boundary 1-min OHLCV row (DESIGN §1.4 "type B";
// IMPL §1.1). The CandleStore and the rollup engine both speak this type
// — a stored 1-min row and a folded higher-timeframe bar share one
// shape, so a single reader walks either without a type switch.
//
// Field notes:
//   - Market stamps the broker's own market label ('IN', ...); the
//     exchange's InstrumentID carries no market, so Market must be the
//     broker's own column (DESIGN §1.1).
//   - Instrument is the opaque contract.InstrumentID the exchange emits.
//   - Start is the bucket's start instant, stored UTC via Postgres
//     TIMESTAMPTZ. The ingestor passes the exchange's time.Time through
//     as-is; local rendering (market tz) happens at the HTTP read edge
//     only (DESIGN §2 + BODIES §1.4).
//   - End is DERIVED, never stored (no end column in the schema):
//     Start + 1*time.Minute (exclusive). scanCandleRow sets it so every
//     reader gets it (IMPL §1.4). A folded 5m bar's End will be
//     Start + 5*time.Minute, set by the rollup engine in a later stage.
//   - Open/High/Low/Close are integer ticks (contract.Ticks = int64);
//     Volume is integer qty (contract.Qty = int64). No floats on the
//     wire or in the store (D-3 / P6-D33). 0 is a valid tick and a valid
//     volume (a flat zero-trade minute, EXCHANGE_19 §5).
type Candle struct {
	Market     string
	Instrument contract.InstrumentID
	Start      time.Time
	End        time.Time
	Open       contract.Ticks
	High       contract.Ticks
	Low        contract.Ticks
	Close      contract.Ticks
	Volume     contract.Qty
}
