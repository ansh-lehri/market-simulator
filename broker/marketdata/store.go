package marketdata

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/contract"
)

// bucketDuration is the one base grain the store speaks: 1 minute. The
// 1-min grain itself depends on the exchange being configured with a
// 1-min OHLCVInterval; the broker receives whatever the exchange emits
// (DESIGN §1.2). Used to derive each row's End on read.
const bucketDuration = time.Minute

// insertCandleSQL persists one closed 1-min candle. Idempotent via
// ON CONFLICT DO NOTHING against the (market, instrument, timestamp) PK
// — a re-delivered minute is a harmless no-op, not an error (DESIGN §1.2).
// The ingestor's canonical-only decision (§6.2) lives at the ingestor;
// the store writes whatever it is handed.
const insertCandleSQL = `INSERT INTO candles (market, instrument, timestamp, open, high, low, close, volume)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (market, instrument, timestamp) DO NOTHING`

// selectRange1mSQL walks one instrument's 1-min rows forward over a
// half-open [from, to) window, keyset-paged on timestamp. The
// ($5::timestamptz IS NULL OR timestamp > $5) predicate lets a single
// prepared statement serve both "first page" (afterTS=nil) and "older
// page" (afterTS=cursor) — same shape as broker/store's selectTradePageSQL.
// Cast is ::timestamptz because the column is tz-aware (vs the account
// store's ::timestamp casts where the column is naive).
const selectRange1mSQL = `SELECT market, instrument, timestamp, open, high, low, close, volume
FROM candles
WHERE market = $1 AND instrument = $2
  AND timestamp >= $3 AND timestamp < $4
  AND ($5::timestamptz IS NULL OR timestamp > $5)
ORDER BY timestamp ASC
LIMIT $6`

// CandleStore is the broker-owned durable layer for 1-min OHLCV. It is
// shard-independent and account-free — the public market-data axis
// (DESIGN §0.1). Holds no state beyond its pool handle; shutdown is
// owned by whoever owns the pool (the broker Store today; the pool is
// shared, not duplicated — see NewCandleStore).
type CandleStore struct {
	pool *pgxpool.Pool
}

// NewCandleStore wraps the shared pgx pool for durable candle I/O. The
// pool is NOT owned here — bringup's Store holds the pool and tears it
// down; CandleStore just borrows it (same pattern used elsewhere in
// broker/store). A nil pool is a programming error at bringup and will
// nil-deref at first use.
func NewCandleStore(pool *pgxpool.Pool) *CandleStore {
	return &CandleStore{pool: pool}
}

// Insert persists one closed 1-min candle. Idempotent — a conflict on
// (market, instrument, timestamp) is NOT an error (ON CONFLICT DO
// NOTHING + 0 rows affected is the no-op path).
//
// Flow (IMPL §1.4):
//   - pool.Exec with the eight fields; on driver error wrap ErrLoadFailed
//     (slog.Error on the way).
//   - c.End is not persisted (no end column); the ingestor doesn't
//     need to set it before Insert.
//
// Canonical-vs-on-demand is NOT this store's call: the ingestor decides
// whether to call Insert at all (on-demand sessions skip persistence,
// DESIGN §1.2).
func (store *CandleStore) Insert(ctx context.Context, candle Candle) error {
	_, execErr := store.pool.Exec(ctx, insertCandleSQL,
		candle.Market,
		candle.Instrument,
		candle.Start,
		candle.Open,
		candle.High,
		candle.Low,
		candle.Close,
		candle.Volume,
	)
	if execErr != nil {
		slog.Error("broker/marketdata: Insert failed",
			"market", candle.Market,
			"instrument", candle.Instrument,
			"start", candle.Start,
			"err", execErr,
		)
		return fmt.Errorf("broker/marketdata: Insert (%s, %s, %s): %w (%v)",
			candle.Market, candle.Instrument, candle.Start, ErrLoadFailed, execErr)
	}
	slog.Debug("broker/marketdata: Insert ok",
		"market", candle.Market,
		"instrument", candle.Instrument,
		"start", candle.Start,
	)
	return nil
}

// RangeRead1m reads stored 1-min candles for one (market, instrument) over
// a half-open [from, to) window, oldest-first, keyset-paged forward on
// timestamp. The limit is honored verbatim — the per-response cap lives
// in the rollup engine (Stage 3), not the store (DESIGN §1.5).
//
// Return semantics (IMPL §1.4):
//   - limit <= 0 → ([]Candle{}, nil) with NO SQL issued (empty ask =
//     empty result, not an error; mirrors broker/store's RecentTrades
//     n<=0 shortcut).
//   - afterTS nil → first-page scan over [from, to).
//   - afterTS != nil → rows strictly later than *afterTS (keyset-forward).
//   - zero rows → ([]Candle{}, nil) — a legitimate gap, not an error.
//   - infra/driver failure → ErrLoadFailed-wrapped.
//
// Each returned Candle has End set to Start + bucketDuration; the field
// is derived here so every reader (HTTP handler, rollup engine) gets it
// without reaching into the store internals (one place, not two —
// IMPL §1.4 recommendation).
func (store *CandleStore) RangeRead1m(
	ctx context.Context,
	market string,
	instrument contract.InstrumentID,
	from time.Time,
	to time.Time,
	afterTS *time.Time,
	limit int,
) ([]Candle, error) {
	if limit <= 0 {
		slog.Debug("broker/marketdata: RangeRead1m limit<=0 shortcut",
			"market", market,
			"instrument", instrument,
			"limit", limit,
		)
		return []Candle{}, nil
	}

	// pgx accepts a nil *time.Time as SQL NULL; the ($5::timestamptz IS NULL
	// OR timestamp > $5) predicate collapses both branches into one query.
	var afterArg any
	if afterTS == nil {
		afterArg = nil
	} else {
		afterArg = *afterTS
	}

	rows, queryErr := store.pool.Query(ctx, selectRange1mSQL, market, instrument, from, to, afterArg, limit)
	if queryErr != nil {
		slog.Error("broker/marketdata: RangeRead1m query failed",
			"market", market,
			"instrument", instrument,
			"from", from,
			"to", to,
			"after_null", afterTS == nil,
			"limit", limit,
			"err", queryErr,
		)
		return nil, fmt.Errorf("broker/marketdata: RangeRead1m query: %w (%v)", ErrLoadFailed, queryErr)
	}
	defer rows.Close()

	candles := make([]Candle, 0, limit)
	for rows.Next() {
		candle, scanErr := scanCandleRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		candles = append(candles, candle)
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, fmt.Errorf("broker/marketdata: RangeRead1m iter: %w (%v)", ErrLoadFailed, iterErr)
	}

	slog.Debug("broker/marketdata: RangeRead1m ok",
		"market", market,
		"instrument", instrument,
		"from", from,
		"to", to,
		"after_null", afterTS == nil,
		"limit", limit,
		"returned", len(candles),
	)
	return candles, nil
}

// scanCandleRow decodes one row from selectRange1mSQL. The 8-field Scan
// order mirrors the SELECT column list exactly — change the column list
// and this must track (same rule as broker/store's scanTradeRow). End is
// derived (Start + bucketDuration), not scanned.
func scanCandleRow(rows pgx.Rows) (Candle, error) {
	var candle Candle
	scanErr := rows.Scan(
		&candle.Market,
		&candle.Instrument,
		&candle.Start,
		&candle.Open,
		&candle.High,
		&candle.Low,
		&candle.Close,
		&candle.Volume,
	)
	if scanErr != nil {
		return Candle{}, fmt.Errorf("broker/marketdata: scanCandleRow: %w (%v)", ErrLoadFailed, scanErr)
	}
	candle.End = candle.Start.Add(bucketDuration)
	return candle, nil
}
