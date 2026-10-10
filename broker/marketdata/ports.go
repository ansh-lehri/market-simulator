package marketdata

import (
	"context"
	"time"

	"simulator/contract"
)

// candleStorePort is the minimal shape the ReadEngine needs from the
// durable candle layer. One method — RangeRead1m — mirroring CandleStore.
// *CandleStore satisfies this implicitly.
//
// Lives on the consumer side (this package) per Go idiom — same pattern
// as broker/reads/ports.go. Keeping the interface where it is consumed
// means the CandleStore stays unaware of the engine's shape.
type candleStorePort interface {
	RangeRead1m(
		ctx context.Context,
		market string,
		instrument contract.InstrumentID,
		from, to time.Time,
		afterTS *time.Time,
		limit int,
	) ([]Candle, error)
}

// queryEnginePort is the minimal shape the HTTP candle handler needs
// from the ReadEngine. One method — QueryCandles — mirroring the
// engine's folded-page accessor. *ReadEngine satisfies this implicitly.
//
// Keeping the handler → engine seam as an interface lets status-map
// tests (200 / 503 / 400 branches) stub the engine without constructing
// a stub store.
type queryEnginePort interface {
	QueryCandles(ctx context.Context, query CandleQuery) (CandlePage, error)
}
