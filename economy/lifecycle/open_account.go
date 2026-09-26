package lifecycle

import (
	"context"
	"errors"
	"log/slog"

	"simulator/economy/account"
	"simulator/economy/persistence"
)

// OpenAccount is LAYER_5_2_LIFECYCLE_FLOWS item 18 — the one operation
// that brings a participant account into existence. Called at agent
// CREATE (Layer 2 spawns an agent) and human signup (platform layer);
// writes ONE row into Postgres accounts and returns the AccountID handle
// the caller stores. Every money operation FKs to this AccountID, so
// nothing about a participant's money can happen until this has run.
//
// This is a durable, SESSION-LESS event. No Redis, no working state — a
// session (canonical OR on-demand) can only bootstrap AFTER this
// completes, so this is deliberately not gated on session_type. A
// first-time user who wants on-demand must go through OpenAccount first
// (doc §4 step 2 explicitly forbids lazy-create in on-demand).
//
// Static starting values come from config, not hard-coded: startingCapital
// and startingBank are resolved by the caller (platform config, agent
// spawn config) and passed in. This flow is policy-free — it records
// what it is handed.
//
// Flow (per doc §2):
//  1. Validate inputs — birth-time floors: trading_cash and bank_cash
//     are BOTH >= 0 at birth (signed trading_cash exists for LATER debt,
//     not at birth; negative start would always be a caller bug).
//  2. Delegate to persistence.OpenAccount, which mints an id (empty
//     AccountID → fallback) and issues the INSERT.
//  3. Translate persistence errors:
//     - ErrAccountExists → ErrAccountExistsFor{OwnerType, OwnerID}
//       (business error; caller looks up the existing handle).
//     - anything else → ErrCreateFailedFor{OwnerType, OwnerID, Cause}.
//
// Concurrency / idempotency: two simultaneous creates for the same
// participant → one wins, the other gets ErrAccountExistsFor via
// uq_accounts_owner (DB as sole arbiter). Not idempotent by AccountID
// (each successful call mints a new one), but idempotent by participant
// via the constraint. Correct caller pattern on ErrAccountExistsFor:
// look up and use the existing handle.
func (service *Service) OpenAccount(
	ctx context.Context,
	ownerType OwnerType,
	ownerID string,
	startingCapital int64,
	startingBank int64,
) (account.AccountID, error) {
	// Step 1 — validate inputs before touching the store. Order matches
	// doc §2 step 1.
	if !ownerType.isValid() {
		slog.Warn("lifecycle: open_account bad owner_type",
			"owner_type", int(ownerType),
			"owner_id", ownerID,
		)
		return "", ErrBadOwnerType
	}
	if ownerID == "" {
		slog.Warn("lifecycle: open_account empty owner_id",
			"owner_type", ownerType,
		)
		return "", ErrBadOwnerID
	}
	if startingCapital < 0 {
		// trading_cash is signed for LATER debt (Option 4). NEVER at
		// birth — a negative start is always a caller bug.
		slog.Warn("lifecycle: open_account negative starting_capital",
			"owner_type", ownerType,
			"owner_id", ownerID,
			"starting_capital", startingCapital,
		)
		return "", ErrNegativeStartingCapital
	}
	if startingBank < 0 {
		// bank_cash has DB CHECK (>= 0); the bank never goes negative.
		// Catch it here so the caller gets a lifecycle-shaped error
		// instead of a driver constraint violation.
		slog.Warn("lifecycle: open_account negative starting_bank",
			"owner_type", ownerType,
			"owner_id", ownerID,
			"starting_bank", startingBank,
		)
		return "", ErrNegativeStartingBank
	}

	slog.Info("lifecycle: open_account begin",
		"owner_type", ownerType,
		"owner_id", ownerID,
		"starting_capital", startingCapital,
		"starting_bank", startingBank,
	)

	// Step 2 — persistence mints the id (AccountID empty triggers the
	// fallback) and issues the INSERT. See [[feedback_helpers_as_fallback_not_monopoly]]:
	// persistence's generator is a fallback, not a monopoly; lifecycle
	// leaves AccountID empty so persistence mints, but a future caller
	// with a specific id scheme could bypass lifecycle and call
	// persistence.OpenAccount directly with their own id.
	durable := persistence.DurableAccount{
		// AccountID: "" — persistence mints
		OwnerType:   ownerType.String(),
		OwnerID:     ownerID,
		TradingCash: startingCapital,
		BankCash:    startingBank,
		DripActive:  false, // no account drips at birth (doc §2 step 3)
		// DripProgress: nil (birth default)
	}

	newID, err := service.persist.OpenAccount(ctx, durable)
	if err != nil {
		// Step 3 — translate persistence errors.
		if errors.Is(err, persistence.ErrAccountExists) {
			slog.Warn("lifecycle: open_account ErrAccountExists",
				"owner_type", ownerType,
				"owner_id", ownerID,
			)
			return "", ErrAccountExistsFor{OwnerType: ownerType, OwnerID: ownerID}
		}
		slog.Error("lifecycle: open_account ErrCreateFailed",
			"owner_type", ownerType,
			"owner_id", ownerID,
			"cause", err,
		)
		return "", ErrCreateFailedFor{OwnerType: ownerType, OwnerID: ownerID, Cause: err}
	}

	slog.Info("lifecycle: open_account ok",
		"account_id", newID,
		"owner_type", ownerType,
		"owner_id", ownerID,
	)
	return account.AccountID(newID), nil
}
