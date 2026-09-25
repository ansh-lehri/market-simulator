package persistence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// loadAccountSQL is the fixed SELECT for one accounts row (schema §2.1). It
// pulls the seven durable columns the DTO carries. Never fetches created_at
// / updated_at — those are book-keeping columns Postgres maintains itself
// and no domain code reads.
const loadAccountSQL = `SELECT
	account_id, owner_type, owner_id,
	trading_cash, bank_cash,
	drip_active, drip_progress
FROM accounts
WHERE account_id = $1`

// saveAccountSQL is the fixed UPDATE for one accounts row (schema §2.1).
// Only mutable columns appear here — account_id / owner_type / owner_id are
// immutable identity (never re-written) and created_at is set at INSERT time.
// updated_at is refreshed to now() on every write.
//
// Persistence never INSERTs into accounts: creation is out of scope
// (LAYER_5_PERSISTENCE §3 "SELECT ... ErrNoAccount when absent, never create
// here"). A pre-existing row is required.
const saveAccountSQL = `UPDATE accounts SET
	trading_cash = $2,
	bank_cash = $3,
	drip_active = $4,
	drip_progress = $5,
	updated_at = now()
WHERE account_id = $1`

// loadAccount reads one accounts row into a flat DurableAccount. Returns
// ErrNoAccount when the row does not exist so callers dispatch with
// errors.Is. Postgres/driver errors are wrapped with context.
func (component *store) loadAccount(ctx context.Context, accountID string) (DurableAccount, error) {
	slog.Debug("persistence: loadAccount", "account_id", accountID)

	var durable DurableAccount
	var dripProgress *int64 // pgx v5 handles nullable BIGINT via *int64
	scanErr := component.db.QueryRow(ctx, loadAccountSQL, accountID).Scan(
		&durable.AccountID,
		&durable.OwnerType,
		&durable.OwnerID,
		&durable.TradingCash,
		&durable.BankCash,
		&durable.DripActive,
		&dripProgress,
	)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return DurableAccount{}, fmt.Errorf("%w: %s", ErrNoAccount, accountID)
		}
		return DurableAccount{}, fmt.Errorf("persistence: loadAccount %s: %w", accountID, scanErr)
	}
	durable.DripProgress = dripProgress

	slog.Info("persistence: loadAccount ok",
		"account_id", durable.AccountID,
		"trading_cash", durable.TradingCash,
		"bank_cash", durable.BankCash,
		"drip_active", durable.DripActive,
	)
	return durable, nil
}

// saveAccount writes the durable resting figure back to the accounts row.
// The caller (lifecycle, D7) has already reconciled the live
// Available/Reserved/Settled split to one signed trading_cash — persistence
// does no money math.
//
// A missing row returns ErrNoAccount (an UPDATE that touches 0 rows is a
// programming error at this layer: creation is out of scope).
func (component *store) saveAccount(ctx context.Context, durable DurableAccount) error {
	slog.Debug("persistence: saveAccount",
		"account_id", durable.AccountID,
		"trading_cash", durable.TradingCash,
		"bank_cash", durable.BankCash,
		"drip_active", durable.DripActive,
	)

	tag, execErr := component.db.Exec(ctx, saveAccountSQL,
		durable.AccountID,
		durable.TradingCash,
		durable.BankCash,
		durable.DripActive,
		durable.DripProgress, // *int64 → nullable BIGINT
	)
	if execErr != nil {
		return fmt.Errorf("persistence: saveAccount %s: %w", durable.AccountID, execErr)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: saveAccount touched %d rows for %s",
			ErrNoAccount, tag.RowsAffected(), durable.AccountID)
	}

	slog.Info("persistence: saveAccount ok", "account_id", durable.AccountID)
	return nil
}
