package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// selectAccountCashSQL reads both cash columns for one account in a
// single row-trip — the pair the pre-session cash view needs. Columns:
//
//   - trading_cash: SIGNED BIGINT NOT NULL (migration 0001, no CHECK;
//     debt is legal and must round-trip signed so callers can distinguish
//     negative balances from "no row").
//   - bank_cash:    BIGINT NOT NULL CHECK >= 0 (migration 0001; non-
//     negative by schema, but we still Scan into int64 so an unexpected
//     negative value — which would be a schema violation — surfaces to
//     the caller rather than being silently truncated).
//
// Both NOT NULL → plain int64 Scan is correct (no sql.NullInt64 needed).
const selectAccountCashSQL = `SELECT trading_cash, bank_cash FROM accounts WHERE account_id = $1`

// AccountCash reads the account's durable (trading_cash, bank_cash) pair
// in paise/coins. Pure durable read — zero writes, no Redis, no shard.
//
// Serves the pre-session cash view: a user who just signed up / logged
// in but has not yet joined a market still needs to see their trading
// cash and bank balance. The shard-side balance read (ReadBalance)
// requires a live session and so cannot answer that question; this
// method is the off-shard answer, read straight from Postgres.
//
// Return semantics (mirrors TradingCash — same error contract):
//
//   - row found  → (trading, bank, nil)       — trading may be negative
//                                              (debt), bank is non-negative
//                                              by schema
//   - no row     → (0, 0, ErrNoAccount)       — signal, not a transport
//                                              failure
//   - driver err → (0, 0, wrap(ErrLoadFailed))
//
// Context: ctx-bound (query cancels with ctx deadline). Zero writes —
// the Store's off-session sole-writer invariant is unaffected (an
// account is live-on-a-shard XOR off-session, so a concurrent DurableTransfer
// cannot race this read against a shard-side MoveCash).
//
// Returns the raw signed trading_cash — matches TradingCash's "no
// shape, no compare" rule. Any comparison or presentation shaping is
// the caller's job (the HTTP handler in broker/reads).
func (store *Store) AccountCash(ctx context.Context, accountID AccountID) (int64, int64, error) {
	var tradingCash, bankCash int64
	scanErr := store.pool.QueryRow(ctx, selectAccountCashSQL, string(accountID)).Scan(&tradingCash, &bankCash)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			slog.Debug("broker/store: AccountCash miss",
				"account_id", accountID,
			)
			return 0, 0, ErrNoAccount
		}
		slog.Error("broker/store: AccountCash query failed",
			"account_id", accountID,
			"err", scanErr,
		)
		return 0, 0, fmt.Errorf("broker/store: AccountCash query (%s): %w (%v)", accountID, ErrLoadFailed, scanErr)
	}
	slog.Debug("broker/store: AccountCash ok",
		"account_id", accountID,
		"trading_cash", tradingCash,
		"bank_cash", bankCash,
	)
	return tradingCash, bankCash, nil
}
