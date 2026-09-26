package persistence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgUniqueViolation is the SQLSTATE code Postgres raises when a UNIQUE
// constraint is violated. Used by openAccount to translate the
// uq_accounts_owner violation into ErrAccountExists.
const pgUniqueViolation = "23505"

// accountsOwnerUniqueConstraint is the schema §2.1 constraint name that
// enforces "one account per (owner_type, owner_id)". A 23505 on any OTHER
// constraint is a schema/caller bug and surfaces as a wrapped error.
const accountsOwnerUniqueConstraint = "uq_accounts_owner"

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
// Creation goes through openAccount / OpenAccount (item 18); this UPDATE is
// the session-close write-back path only.
const saveAccountSQL = `UPDATE accounts SET
	trading_cash = $2,
	bank_cash = $3,
	drip_active = $4,
	drip_progress = $5,
	updated_at = now()
WHERE account_id = $1`

// openAccountSQL is the fixed INSERT for one accounts row (schema §2.1),
// the durable-birth path (LAYER_5_2_LIFECYCLE_FLOWS item 18). All seven
// caller-supplied columns are listed explicitly; created_at / updated_at
// default to now() at the DB.
//
// The uq_accounts_owner UNIQUE constraint on (owner_type, owner_id) is the
// SOLE arbiter of "one account per participant"; a race between two
// concurrent creates for the same owner surfaces as 23505 on this
// constraint, which openAccount translates into ErrAccountExists (no
// check-then-insert).
const openAccountSQL = `INSERT INTO accounts
	(account_id, owner_type, owner_id, trading_cash, bank_cash, drip_active, drip_progress)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`

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

// generateAccountID mints a 128-bit random hex id for a new accounts row.
// crypto/rand failure is dev-fatal (root: broken invariant, panic) — the
// same policy realize/persistence.go uses for its trade-id fallback.
func generateAccountID() string {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		panic("persistence: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(randomBytes[:])
}

// openAccount inserts one new accounts row and returns the id used. The
// durable-birth path for LAYER_5_2_LIFECYCLE_FLOWS item 18 (open_account).
//
// AccountID policy: the caller MAY supply DurableAccount.AccountID (any
// unique TEXT — a scheme-prefixed id, a deterministic test id, an id
// generated by an upstream layer). When empty, persistence mints a 128-bit
// hex id as a fallback. The uq_accounts_owner constraint and the accounts
// primary key enforce the real invariants (one-per-owner, id uniqueness)
// regardless of who mints the id.
//
// Error mapping:
//   - uq_accounts_owner UNIQUE violation → ErrAccountExists (business, not
//     fatal — caller looks up the existing handle for agent re-spawn /
//     human re-login)
//   - any other 23505 (e.g. accounts_pkey collision on a caller-supplied
//     id) → wrapped fmt.Errorf so callers can inspect via errors.As
//   - any other pg / driver error → wrapped fmt.Errorf
//
// Redis is untouched here — creation is a durable, session-less event.
func (component *store) openAccount(ctx context.Context, durable DurableAccount) (string, error) {
	accountID := durable.AccountID
	if accountID == "" {
		accountID = generateAccountID()
	}

	slog.Debug("persistence: openAccount",
		"account_id", accountID,
		"owner_type", durable.OwnerType,
		"owner_id", durable.OwnerID,
		"trading_cash", durable.TradingCash,
		"bank_cash", durable.BankCash,
	)

	_, execErr := component.db.Exec(ctx, openAccountSQL,
		accountID,
		durable.OwnerType,
		durable.OwnerID,
		durable.TradingCash,
		durable.BankCash,
		durable.DripActive,
		durable.DripProgress, // *int64 → nullable BIGINT
	)
	if execErr != nil {
		var pgErr *pgconn.PgError
		if errors.As(execErr, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == accountsOwnerUniqueConstraint {
			return "", fmt.Errorf("%w: owner_type=%s owner_id=%s",
				ErrAccountExists, durable.OwnerType, durable.OwnerID)
		}
		return "", fmt.Errorf("persistence: openAccount owner_type=%s owner_id=%s: %w",
			durable.OwnerType, durable.OwnerID, execErr)
	}

	slog.Info("persistence: openAccount ok",
		"account_id", accountID,
		"owner_type", durable.OwnerType,
		"owner_id", durable.OwnerID,
	)
	return accountID, nil
}
