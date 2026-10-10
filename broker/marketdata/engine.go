package marketdata

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"simulator/contract"
)

// defaultCandleLimit is the default row-count ceiling when the HTTP
// client omits ?limit=. A reasonable UI page; the engine's own cap
// still bounds huge requests.
const defaultCandleLimit = 100

// maxCandleLimit is the engine-level clamp on query.Limit. A defensive
// backstop against callers requesting a page of 1_000_000 — a limit
// above this is silently clamped, not an error. Mirrors broker/store's
// maxRecent discipline.
const maxCandleLimit = 500

// CandleQuery is the internal query shape fed to the ReadEngine. The
// HTTP handler builds this from the parsed query string + the market
// tz; From/To arrive as UTC instants (the handler resolved the
// naive-local wire form via tzFor + ParseInLocation).
//
// AfterTS semantics (DESIGN §4.3): the "dropped bucket's first minute"
// from the previous response's NextCursor. nil = first page. The engine
// internally translates this to the Store's exclusive afterTS by
// subtracting one minute, so the Store's SQL `WHERE timestamp > $5`
// includes the dropped bucket's first minute.
type CandleQuery struct {
	Market     string
	Instrument contract.InstrumentID
	TF         Timeframe
	From, To   time.Time
	AfterTS    *time.Time
	Limit      int
}

// CandlePage is the engine's output — the folded bars + an optional
// next-page cursor. Pure values; the HTTP handler shapes this into the
// wire envelope (CandleResponse).
//
// NextCursor is the dropped bucket's first-minute instant (DESIGN §4.3)
// or nil when the batch was short (last page). The HTTP handler formats
// to a naive-local ISO string on the wire.
type CandlePage struct {
	Candles    []Candle
	NextCursor *time.Time
}

// ReadEngine folds oldest-first 1-min rows into the client's requested
// timeframe, paged via the trailing-partial-bucket cursor rule. Pure
// read — no state beyond the store handle.
type ReadEngine struct {
	store candleStorePort
}

// NewReadEngine wraps a candleStorePort for use by the HTTP handler.
// The store is NOT owned here; CandleStore's own lifecycle governs the
// pool (same discipline as broker/store's NewStore: a nil at
// construction will nil-deref at first use rather than being validated
// up front).
func NewReadEngine(store candleStorePort) *ReadEngine {
	return &ReadEngine{store: store}
}

// QueryCandles folds one page of 1-min rows to query.TF bars with the
// trailing-partial-bucket cursor rule (DESIGN §4.3):
//
//   - Full batch (len(raw) == rowLimit → more rows likely exist) → drop
//     the trailing (possibly partial) bar; cursor = that bar's Start so
//     the next page re-folds the bucket whole (no split, no duplicate).
//   - Short batch (ran out of rows in [From, To)) → last bar is complete;
//     cursor = nil (last page).
//   - Fallback trim: a gap-rich window can fold to more than query.Limit
//     bars on a short batch (e.g. one full :00-:04 bucket + one partial
//     :10-:12 bucket from a 50-min window with gaps). Trim down and set
//     cursor to the first trimmed bar's Start.
//
// The slack of one extra 1-min row (rowLimit = Limit*TF + 1) is exactly
// enough to detect "the (Limit+1)-th bucket has at least one row" — the
// trigger for the full-batch drop. Any larger slack would need more
// complex trimming logic.
//
// The engine translates the user-visible AfterTS (first minute of a
// dropped bucket, inclusive) to the Store's exclusive afterTS by
// subtracting one minute. Keeps the Store's RangeRead1m primitive
// general-purpose; the paging quirk lives here.
//
// On query.Market not in the tz allowlist: handler validation should
// prevent this; if it fires here, it's a programming error so we panic
// (invariant violation per root CLAUDE.md). On Store ErrLoadFailed:
// propagate verbatim so the handler can switch on the sentinel.
func (engine *ReadEngine) QueryCandles(ctx context.Context, query CandleQuery) (CandlePage, error) {
	if query.Limit > maxCandleLimit {
		slog.Debug("broker/marketdata: QueryCandles clamping limit",
			"requested", query.Limit,
			"max", maxCandleLimit,
		)
		query.Limit = maxCandleLimit
	}
	if query.Limit <= 0 {
		// Defensive — handler rejects Limit<=0 at 400 before here.
		return CandlePage{Candles: []Candle{}, NextCursor: nil}, nil
	}
	location, ok := tzFor(query.Market)
	if !ok {
		// Handler validation guarantees this. If it fires, it's a
		// programming error (invariant violation per root CLAUDE.md).
		panic(fmt.Sprintf("broker/marketdata: QueryCandles called with unknown market %q", query.Market))
	}

	// Translate user-visible cursor (= dropped bucket's first minute,
	// inclusive) → Store's exclusive afterTS by subtracting one minute.
	var storeAfterTS *time.Time
	if query.AfterTS != nil {
		translated := query.AfterTS.Add(-1 * time.Minute)
		storeAfterTS = &translated
	}

	rowLimit := query.Limit*int(query.TF) + 1
	rows, readErr := engine.store.RangeRead1m(ctx, query.Market, query.Instrument,
		query.From, query.To, storeAfterTS, rowLimit)
	if readErr != nil {
		return CandlePage{}, readErr
	}

	bars := RollUp(rows, query.TF, location)

	var nextCursor *time.Time
	if len(rows) == rowLimit && len(bars) > 0 {
		// Full batch — drop trailing (possibly partial) bar; next page
		// re-folds it from scratch (DESIGN §4.3).
		droppedStart := bars[len(bars)-1].Start
		bars = bars[:len(bars)-1]
		nextCursor = &droppedStart
	}
	if len(bars) > query.Limit {
		// Fallback: a gap-rich input can fold to more than query.Limit
		// bars even on a short batch (gaps make each partial bucket its
		// own bar). Trim and set cursor to the first trimmed bar.
		droppedStart := bars[query.Limit].Start
		bars = bars[:query.Limit]
		nextCursor = &droppedStart
	}

	return CandlePage{Candles: bars, NextCursor: nextCursor}, nil
}
