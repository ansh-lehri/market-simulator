package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// TransferDirection selects which cash column rises and which falls on
// a DurableTransfer. The zero value is deliberately unassigned so a
// caller that forgets to set the direction fails validation with
// ErrBadTransfer instead of silently defaulting to one side.
type TransferDirection int

const (
	_         TransferDirection = iota
	ToTrading                   // Bank → Available (trading_cash).  Guard: bank_cash  >= amount.
	ToBank                      // Available (trading_cash) → Bank.  Guard: trading_cash >= amount.
)

// String keeps slog boundary logs readable — a bare int tells a reader
// nothing when they land on a line in the drainage log.
func (direction TransferDirection) String() string {
	switch direction {
	case ToTrading:
		return "to_trading"
	case ToBank:
		return "to_bank"
	default:
		return fmt.Sprintf("invalid_direction(%d)", int(direction))
	}
}

// toTradingSQL and toBankSQL are the two guarded UPDATEs. One SQL
// statement each: atomic guard-and-move with no transaction wrapper and
// no read-then-write race (BROKER_PHASE1 §4.3). The guard predicate
// combined with RETURNING gives us a race-free "did the move succeed"
// signal — zero rows RETURNED means the guard rejected it; one row
// RETURNED carries the post-move balances.
//
// updated_at is bumped so audit queries on the accounts row see the
// transfer; schema §1 defines the column (migration 0001).
const (
	toTradingSQL = `UPDATE accounts
	   SET bank_cash    = bank_cash    - $2,
	       trading_cash = trading_cash + $2,
	       updated_at   = now()
	 WHERE account_id = $1 AND bank_cash >= $2
	 RETURNING trading_cash, bank_cash`

	toBankSQL = `UPDATE accounts
	   SET trading_cash = trading_cash - $2,
	       bank_cash    = bank_cash    + $2,
	       updated_at   = now()
	 WHERE account_id = $1 AND trading_cash >= $2
	 RETURNING trading_cash, bank_cash`

	// accountExistsSQL is the 0-rows disambiguation probe (P1-D8). We
	// cannot call AccountHandle here because AccountHandle is keyed on
	// (owner_type, owner_id) and DurableTransfer only knows account_id.
	// Semantically identical probe — "does this account row exist" —
	// via account_id to match the UPDATE's own filter.
	accountExistsSQL = `SELECT 1 FROM accounts WHERE account_id = $1`
)

// DurableTransfer executes the Stage-2 off-session bank↔trading move
// (BROKER_PHASE1 §4). One guarded UPDATE per direction, conserving by
// construction (both legs change by the same amount in opposite signs).
//
// Returns the post-move (trading_cash, bank_cash) on success. "available"
// in the return name mirrors the off-session mapping: without any live
// reservations, trading_cash IS the available bucket (§4.4). A later
// session loading the account will see the post-transfer trading_cash.
//
// Return semantics:
//   - amount <= 0                 → (0, 0, ErrBadTransfer)
//   - direction not in {ToTrading, ToBank}
//                                  → (0, 0, ErrBadTransfer)
//   - account exists + guard pass → (trading_cash, bank_cash, nil)
//   - account exists + guard fail → (0, 0, ErrInsufficient)
//   - account missing            → (0, 0, ErrNoAccount)
//   - infra/driver failure       → (0, 0, ErrLoadFailed-wrapped)
//
// Safety (BROKER_PHASE1 §4.5): off-session, DurableTransfer is the sole
// mutator of accounts cash columns, and an account is live-on-a-shard
// XOR off-session — so this never races a live MoveCash. Conservation
// holds without a lock. The DB's ck_bank_nonneg CHECK on bank_cash is a
// backstop: a buggy guard would be rejected by Postgres, not corrupt
// the balance.
func (store *Store) DurableTransfer(
	ctx context.Context,
	accountID AccountID,
	direction TransferDirection,
	amount int64,
) (available int64, bank int64, err error) {
	if amount <= 0 {
		slog.Debug("broker/store: DurableTransfer bad amount",
			"account_id", accountID,
			"direction", direction,
			"amount", amount,
		)
		return 0, 0, fmt.Errorf("broker/store: DurableTransfer amount must be > 0 (got %d): %w", amount, ErrBadTransfer)
	}

	var updateSQL string
	switch direction {
	case ToTrading:
		updateSQL = toTradingSQL
	case ToBank:
		updateSQL = toBankSQL
	default:
		slog.Debug("broker/store: DurableTransfer bad direction",
			"account_id", accountID,
			"direction_raw", int(direction),
		)
		return 0, 0, fmt.Errorf("broker/store: DurableTransfer unknown direction %s: %w", direction, ErrBadTransfer)
	}

	var tradingCash, bankCash int64
	scanErr := store.pool.QueryRow(ctx, updateSQL, accountID, amount).Scan(&tradingCash, &bankCash)
	if scanErr == nil {
		slog.Info("broker/store: DurableTransfer ok",
			"account_id", accountID,
			"direction", direction,
			"amount", amount,
			"post_trading_cash", tradingCash,
			"post_bank_cash", bankCash,
		)
		return tradingCash, bankCash, nil
	}

	if !errors.Is(scanErr, pgx.ErrNoRows) {
		slog.Error("broker/store: DurableTransfer update failed",
			"account_id", accountID,
			"direction", direction,
			"amount", amount,
			"err", scanErr,
		)
		return 0, 0, fmt.Errorf("broker/store: DurableTransfer update: %w (%v)", ErrLoadFailed, scanErr)
	}

	// Zero rows affected — disambiguate missing vs insufficient via the
	// existence probe (P1-D8).
	exists, probeErr := store.accountExists(ctx, accountID)
	if probeErr != nil {
		slog.Error("broker/store: DurableTransfer probe failed",
			"account_id", accountID,
			"direction", direction,
			"err", probeErr,
		)
		return 0, 0, fmt.Errorf("broker/store: DurableTransfer probe: %w (%v)", ErrLoadFailed, probeErr)
	}
	if !exists {
		slog.Debug("broker/store: DurableTransfer no such account",
			"account_id", accountID,
			"direction", direction,
		)
		return 0, 0, ErrNoAccount
	}
	slog.Debug("broker/store: DurableTransfer guard rejected (insufficient funds)",
		"account_id", accountID,
		"direction", direction,
		"amount", amount,
	)
	return 0, 0, ErrInsufficient
}

// accountExists is the by-id existence probe DurableTransfer uses to
// disambiguate a zero-rows UPDATE: missing row → ErrNoAccount; existing
// row → guard failed → ErrInsufficient. Unexported — the only caller is
// DurableTransfer. AccountHandle (handle.go) does the same lookup keyed
// by owner identity, not account_id, so it is not the right probe here.
//
// Returns (false, nil) for pgx.ErrNoRows (the expected "missing" case);
// (true, nil) when a row exists; (false, err) for any other failure so
// the caller can wrap it as ErrLoadFailed.
func (store *Store) accountExists(ctx context.Context, accountID AccountID) (bool, error) {
	var marker int
	scanErr := store.pool.QueryRow(ctx, accountExistsSQL, accountID).Scan(&marker)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return false, nil
		}
		return false, scanErr
	}
	return true, nil
}
