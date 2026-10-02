package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// selectTradingCashSQL reads the signed trading_cash column for one
// account. trading_cash is signed (debt is legal, no CHECK(>=0)) — the
// Phase-2 QualifyOnDemand gate relies on a negative balance cleanly
// failing the `>= OnDemandMinimum` comparison, so the column type is
// load-bearing. Column is NOT NULL (migration 0001), so Scan into a plain
// int64 is correct — no sql.NullInt64 needed.
const selectTradingCashSQL = `SELECT trading_cash FROM accounts WHERE account_id = $1`

// TradingCash reads the account's durable trading_cash in paise/coins. Pure
// durable read — zero writes, no Redis. Consumed by Phase-2 QualifyOnDemand
// (BROKER_PHASE2 §2 / P2-2·D2): the gate wraps this value with
// cfg.OnDemandMinimum and returns a bare boolean to L7, so this method
// MUST NOT shape or compare — it returns the raw signed value.
//
// Return semantics (BROKER_PHASE2 §2):
//   - row found    → (signed value, nil)      — may be negative (debt)
//   - no row       → (0, ErrNoAccount)        — signal, not failure
//   - driver err   → (0, wrap(ErrLoadFailed))
//
// Context: ctx-bound (query cancels with ctx deadline). Zero writes;
// the Store's off-session sole-writer invariant is unaffected.
func (store *Store) TradingCash(ctx context.Context, accountID AccountID) (int64, error) {
	var tradingCash int64
	scanErr := store.pool.QueryRow(ctx, selectTradingCashSQL, string(accountID)).Scan(&tradingCash)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			slog.Debug("broker/store: TradingCash miss",
				"account_id", accountID,
			)
			return 0, ErrNoAccount
		}
		slog.Error("broker/store: TradingCash query failed",
			"account_id", accountID,
			"err", scanErr,
		)
		return 0, fmt.Errorf("broker/store: TradingCash query (%s): %w (%v)", accountID, ErrLoadFailed, scanErr)
	}
	slog.Debug("broker/store: TradingCash ok",
		"account_id", accountID,
		"trading_cash", tradingCash,
	)
	return tradingCash, nil
}
