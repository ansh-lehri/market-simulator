package conductor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"simulator/contract"
)

// selectLatestClosePriceSQL reads the most recent persisted close for one
// (market tag, instrument) across every past session. The key is the
// broker-side market tag (e.g. "IN"), NOT the per-instance market id —
// references survive across daily instance ids by design.
//
// Ordering is on the TIMESTAMPTZ column; LIMIT 1 returns the single most
// recent row (or no row for a first-ever session). The schema's primary
// key (market, instrument, timestamp) backs this scan without a
// secondary index.
const selectLatestClosePriceSQL = `SELECT close FROM candles
WHERE market = $1 AND instrument = $2
ORDER BY timestamp DESC
LIMIT 1`

// candleReader is the narrow seam readReference uses — just enough
// surface to query one row and scan it. *pgxpool.Pool satisfies this
// implicitly, and tests inject a fake implementation without opening a
// database connection.
//
// Kept inside this package: readReference is the only caller, so the
// seam does not need to be a public interface in a shared location.
type candleReader interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// readReference returns the reference price the exchange should receive
// for one instrument at bring-up: the most recent persisted close for
// (marketTag, instrument). The lookup keys on the broker-side market
// tag so a new session id does not invalidate yesterday's close.
//
// Three outcomes:
//   - A row is found → return the stored close as the reference.
//   - No row (first-ever session for this instrument) → fall back to
//     the configured seed price. Not an error; a seed-price-only path
//     is how a brand-new instrument gets its first reference.
//   - Any other scan error (connection failure, driver fault) → wrap
//     and return. The caller aborts the bring-up; the next arm retries.
//
// The caller is the canonical bring-up flow (Stage 4+). readReference
// does no money math and no persistence — it is a single read on a
// shared pool.
func readReference(ctx context.Context, reader candleReader, marketTag MarketTag, instrument InstrumentConfig) (contract.Ticks, error) {
	var latestClose int64
	scanErr := reader.QueryRow(ctx, selectLatestClosePriceSQL, string(marketTag), string(instrument.ID)).Scan(&latestClose)
	switch {
	case errors.Is(scanErr, pgx.ErrNoRows):
		slog.Debug("conductor: readReference no prior candle — falling back to seed price",
			"market", marketTag,
			"instrument", instrument.ID,
			"seed_price", instrument.SeedPrice,
		)
		return instrument.SeedPrice, nil
	case scanErr != nil:
		slog.Error("conductor: readReference failed",
			"market", marketTag,
			"instrument", instrument.ID,
			"err", scanErr,
		)
		return 0, fmt.Errorf("conductor: readReference (%s, %s): %w", marketTag, instrument.ID, scanErr)
	}
	slog.Debug("conductor: readReference prior close resolved",
		"market", marketTag,
		"instrument", instrument.ID,
		"close", latestClose,
	)
	return contract.Ticks(latestClose), nil
}
