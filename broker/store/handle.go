package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// selectAccountByOwnerSQL resolves (owner_type, owner_id) → account_id.
// Both filters are required (the UNIQUE constraint is on the pair:
// uq_accounts_owner UNIQUE (owner_type, owner_id) — migration 0001). At
// most one row can match; QueryRow is correct, not Query.
const selectAccountByOwnerSQL = `SELECT account_id FROM accounts WHERE owner_type = $1 AND owner_id = $2`

// AccountHandle resolves an owner's identity to its AccountID.
//
// Return semantics (BROKER_PHASE1 §2.3):
//   - row found      → (accountID, nil)
//   - no match       → ("", ErrNoAccount)                 // signal, not failure
//   - infra/driver   → ("", fmt.Errorf(..., ErrLoadFailed))
//   - bad arguments  → ("", fmt.Errorf(..., ErrLoadFailed))
//
// ownerType is a plain string at this seam (P1-D5) — the accounts.
// owner_type column stores "agent" / "human" as text, and L5's typed
// lifecycle.OwnerType stays up in Phase 2's EnsureAccount, which does
// the .String() before calling here. The Store speaks the DB's language.
//
// ErrNoAccount is deliberately not a failure: Phase 2 reads it as
// "owner has no account yet" and triggers an OpenAccount; Phase 1
// Stage C's DurableTransfer reuses the same lookup shape as its 0-rows
// existence probe (P1-D8). Keep this method narrow and composable.
func (store *Store) AccountHandle(ctx context.Context, ownerType, ownerID string) (AccountID, error) {
	if ownerType == "" || ownerID == "" {
		slog.Debug("broker/store: AccountHandle bad arguments",
			"owner_type_empty", ownerType == "",
			"owner_id_empty", ownerID == "",
		)
		return "", fmt.Errorf("broker/store: AccountHandle requires non-empty owner_type and owner_id: %w", ErrLoadFailed)
	}

	var accountID AccountID
	scanErr := store.pool.QueryRow(ctx, selectAccountByOwnerSQL, ownerType, ownerID).Scan(&accountID)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			slog.Debug("broker/store: AccountHandle miss",
				"owner_type", ownerType,
				"owner_id", ownerID,
			)
			return "", ErrNoAccount
		}
		slog.Error("broker/store: AccountHandle query failed",
			"owner_type", ownerType,
			"owner_id", ownerID,
			"err", scanErr,
		)
		return "", fmt.Errorf("broker/store: AccountHandle query (%s, %s): %w (%v)", ownerType, ownerID, ErrLoadFailed, scanErr)
	}

	slog.Debug("broker/store: AccountHandle ok",
		"owner_type", ownerType,
		"owner_id", ownerID,
		"account_id", accountID,
	)
	return accountID, nil
}
