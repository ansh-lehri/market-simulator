package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// TradeRow is one row of participant_trades (migration 0002) — the 15
// universal columns the drainer writes. Field order and types mirror the
// drainer's insertParticipantTradeSQL (economy/persistence/trades_sql.go)
// so a drift here surfaces at integration-test time as a Scan error.
//
// All money is integer coins / ticks (root value discipline). PnlPct is
// float32 because the DB column is REAL (32-bit); keeping the Go type at
// 32-bit matches the stored precision exactly, no silent widening.
//
// Strings for SessionID / InstrumentID / Direction / ExitReason: the DB
// stores them as TEXT, and this seam deliberately does not import
// economy/realize (which carries ExitReason as a named type). The
// Phase-6 response layer can retype at presentation if it wants.
type TradeRow struct {
	TradeID      string
	AccountID    AccountID
	SessionID    string
	InstrumentID string
	Direction    string // "long" | "short"
	Quantity     int64
	EntryPrice   int64 // ticks
	ExitPrice    int64 // ticks
	EntryAt      time.Time
	ExitAt       time.Time
	GrossPnl     int64 // SIGNED
	TotalCosts   int64 // >= 0
	NetPnl       int64 // SIGNED
	PnlPct       float32
	ExitReason   string
}

// PnLSummary is the account-level aggregate projection — pure sums + counts
// over participant_trades, no recomputation (BROKER_PHASE1 §3.6). WinCount
// counts rows with net_pnl > 0; LossCount counts rows with net_pnl < 0;
// breakevens (net_pnl == 0) are counted in neither (BROKER_READ_SURFACES
// §7.5).
type PnLSummary struct {
	TradeCount int
	GrossPnl   int64
	TotalCosts int64
	NetPnl     int64
	WinCount   int
	LossCount  int
}

// selectTradeCols is the 15-column SELECT list for participant_trades,
// matched 1:1 to the drainer's insertParticipantTradeSQL. One source of
// truth so RecentTrades and TradePage both drive the same scan helper.
const selectTradeCols = `trade_id, account_id, session_id, instrument_id,
	direction, quantity, entry_price, exit_price,
	entry_at, exit_at,
	gross_pnl, total_costs, net_pnl, pnl_pct,
	exit_reason`

// selectRecentTradesSQL rides ix_trades_account_recent (account_id,
// exit_at DESC) — index-only ordered scan, no sort step (schema §2.2 +
// migration 0002).
const selectRecentTradesSQL = `SELECT ` + selectTradeCols + `
FROM participant_trades
WHERE account_id = $1
ORDER BY exit_at DESC
LIMIT $2`

// selectTradePageSQL is selectRecentTradesSQL + the keyset-pagination
// predicate. `$2::timestamptz IS NULL` lets a single prepared statement
// serve both "newest page" (before=nil) and "older page" (before=last
// row's exit_at) — no branching in Go.
const selectTradePageSQL = `SELECT ` + selectTradeCols + `
FROM participant_trades
WHERE account_id = $1 AND ($2::timestamptz IS NULL OR exit_at < $2)
ORDER BY exit_at DESC
LIMIT $3`

// selectPnLSummarySQL projects the five sums + three counts in ONE round
// trip. count(*) always returns one row, so a new account with zero trades
// still gets a zero-filled summary (not pgx.ErrNoRows). coalesce guards the
// sum() → NULL case that empty aggregates would otherwise return.
const selectPnLSummarySQL = `SELECT
	count(*),
	coalesce(sum(gross_pnl), 0),
	coalesce(sum(total_costs), 0),
	coalesce(sum(net_pnl), 0),
	count(*) FILTER (WHERE net_pnl > 0),
	count(*) FILTER (WHERE net_pnl < 0)
FROM participant_trades
WHERE account_id = $1`

// scanTradeRow decodes one row from either selectRecentTradesSQL or
// selectTradePageSQL. The 15-field Scan order mirrors selectTradeCols
// exactly — change the column list and this must track.
func scanTradeRow(rows pgx.Rows) (TradeRow, error) {
	var row TradeRow
	scanErr := rows.Scan(
		&row.TradeID,
		&row.AccountID,
		&row.SessionID,
		&row.InstrumentID,
		&row.Direction,
		&row.Quantity,
		&row.EntryPrice,
		&row.ExitPrice,
		&row.EntryAt,
		&row.ExitAt,
		&row.GrossPnl,
		&row.TotalCosts,
		&row.NetPnl,
		&row.PnlPct,
		&row.ExitReason,
	)
	if scanErr != nil {
		return TradeRow{}, fmt.Errorf("broker/store: scanTradeRow: %w (%v)", ErrLoadFailed, scanErr)
	}
	return row, nil
}

// cappedLimit applies the maxRecent ceiling (types.go). Caller-supplied
// requestedLimit values above maxRecent are capped silently — Phase 1 is
// a defensive backstop, not a validator. A caller asking for 1_000_000
// gets maxRecent rows, not an error.
func cappedLimit(requestedLimit int) int {
	if requestedLimit > maxRecent {
		return maxRecent
	}
	return requestedLimit
}

// RecentTrades returns the N most recent closed trades for an account,
// newest-first by exit_at. Rides ix_trades_account_recent.
//
// Return semantics (BROKER_PHASE1 §3.6):
//   - requestedN <= 0           → ([]TradeRow{}, nil) with NO SQL issued
//     (empty ask = empty result, not an error; probes the "n<=0 short
//     circuits" rule from the design doc).
//   - requestedN > maxRecent    → capped at maxRecent, no error.
//   - zero rows                 → ([]TradeRow{}, nil) — empty ≠ error.
//   - infra/driver failure      → ErrLoadFailed-wrapped.
func (store *Store) RecentTrades(ctx context.Context, accountID AccountID, requestedN int) ([]TradeRow, error) {
	if requestedN <= 0 {
		slog.Debug("broker/store: RecentTrades n<=0 shortcut",
			"account_id", accountID,
			"requested_n", requestedN,
		)
		return []TradeRow{}, nil
	}
	limit := cappedLimit(requestedN)

	rows, queryErr := store.pool.Query(ctx, selectRecentTradesSQL, accountID, limit)
	if queryErr != nil {
		slog.Error("broker/store: RecentTrades query failed",
			"account_id", accountID,
			"limit", limit,
			"err", queryErr,
		)
		return nil, fmt.Errorf("broker/store: RecentTrades query: %w (%v)", ErrLoadFailed, queryErr)
	}
	defer rows.Close()

	trades := make([]TradeRow, 0, limit)
	for rows.Next() {
		row, scanErr := scanTradeRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		trades = append(trades, row)
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, fmt.Errorf("broker/store: RecentTrades iter: %w (%v)", ErrLoadFailed, iterErr)
	}

	slog.Debug("broker/store: RecentTrades ok",
		"account_id", accountID,
		"limit", limit,
		"returned", len(trades),
	)
	return trades, nil
}

// TradePage returns one page of closed trades for an account, newest-first
// by exit_at. Keyset-paginated on exit_at:
//   - before == nil → newest page.
//   - before != nil → rows strictly older than *before.
//
// The caller walks pages by passing the previous page's last row's
// ExitAt as the next `before`; a page with fewer than `requestedLimit`
// rows is the last page (BROKER_PHASE1 §3.6, BROKER_READ_SURFACES §7.1).
//
// requestedLimit <= 0 shortcuts to ([], nil), same rule as RecentTrades.
// Values above maxRecent are capped.
func (store *Store) TradePage(ctx context.Context, accountID AccountID, requestedLimit int, before *time.Time) ([]TradeRow, error) {
	if requestedLimit <= 0 {
		slog.Debug("broker/store: TradePage limit<=0 shortcut",
			"account_id", accountID,
			"requested_limit", requestedLimit,
		)
		return []TradeRow{}, nil
	}
	limit := cappedLimit(requestedLimit)

	// pgx accepts a nil *time.Time as a SQL NULL; the `$2::timestamptz IS
	// NULL OR ...` predicate handles both branches without a second query.
	var beforeArg any
	if before == nil {
		beforeArg = nil
	} else {
		beforeArg = *before
	}

	rows, queryErr := store.pool.Query(ctx, selectTradePageSQL, accountID, beforeArg, limit)
	if queryErr != nil {
		slog.Error("broker/store: TradePage query failed",
			"account_id", accountID,
			"limit", limit,
			"before_null", before == nil,
			"err", queryErr,
		)
		return nil, fmt.Errorf("broker/store: TradePage query: %w (%v)", ErrLoadFailed, queryErr)
	}
	defer rows.Close()

	trades := make([]TradeRow, 0, limit)
	for rows.Next() {
		row, scanErr := scanTradeRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		trades = append(trades, row)
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, fmt.Errorf("broker/store: TradePage iter: %w (%v)", ErrLoadFailed, iterErr)
	}

	slog.Debug("broker/store: TradePage ok",
		"account_id", accountID,
		"limit", limit,
		"before_null", before == nil,
		"returned", len(trades),
	)
	return trades, nil
}

// PnLSummary returns the account-level aggregate across every closed
// trade — pure stored-column projection, zero recomputation. A new
// account with no trades returns a zero-filled summary (NOT an error):
// count(*) always produces one row, coalesce handles NULL sums.
//
// Infra failure → ErrLoadFailed-wrapped.
func (store *Store) PnLSummary(ctx context.Context, accountID AccountID) (PnLSummary, error) {
	var summary PnLSummary
	scanErr := store.pool.QueryRow(ctx, selectPnLSummarySQL, accountID).Scan(
		&summary.TradeCount,
		&summary.GrossPnl,
		&summary.TotalCosts,
		&summary.NetPnl,
		&summary.WinCount,
		&summary.LossCount,
	)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			// Defensive: count(*) always yields a row, so pgx.ErrNoRows
			// here means something upstream is off. Treat as infra
			// failure, not a signal.
			slog.Error("broker/store: PnLSummary unexpected ErrNoRows (count(*) always has a row)",
				"account_id", accountID,
			)
			return PnLSummary{}, fmt.Errorf("broker/store: PnLSummary: %w (unexpected ErrNoRows)", ErrLoadFailed)
		}
		slog.Error("broker/store: PnLSummary query failed",
			"account_id", accountID,
			"err", scanErr,
		)
		return PnLSummary{}, fmt.Errorf("broker/store: PnLSummary: %w (%v)", ErrLoadFailed, scanErr)
	}

	slog.Debug("broker/store: PnLSummary ok",
		"account_id", accountID,
		"trade_count", summary.TradeCount,
		"net_pnl", summary.NetPnl,
	)
	return summary, nil
}
