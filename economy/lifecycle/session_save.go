package lifecycle

import (
	"context"
	"log/slog"

	"simulator/economy/account"
	"simulator/economy/persistence"
)

// SessionSave is LAYER_5_2_LIFECYCLE_FLOWS item 21 — the canonical
// session's teardown. Reconciles the in-memory split back into the single
// signed trading_cash figure and writes it to Postgres, then discards the
// Redis snapshot. The LAST teardown step; auto-square (5.4) → trades
// flushed must have run first.
//
// Flow (per doc §5):
//  1. Guard PersistOnClose — on-demand must never reach a Postgres write.
//     Mis-routed on-demand → ErrNotCanonical (return; caller routes to
//     SessionDiscard). Defense-in-depth: refusal is safe, corruption isn't.
//  2. Assert flat — auto-square must have zeroed positions and released
//     every reservation. Reserved != 0 → panic. Any live position → panic.
//     Non-flat at save = ordering bug (dev-fatal).
//  3. Reconcile — trading_cash(durable) = Available + Settled. Bank / drip
//     carry over. This is the ONLY money math lifecycle does.
//  4. SaveAccount (one Postgres UPDATE, atomic inside persistence).
//     Failure → ErrSaveFailedFor. CRITICAL: do NOT discard the snapshot on
//     save failure — the snapshot is the only surviving copy.
//  5. DeleteSnapshot — obsolete only after the durable write commits.
//     Failure is non-fatal (TTL cleans stale keys; a save-succeeded /
//     discard-failed session has valid Postgres state).
func (service *Service) SessionSave(ctx context.Context, sessionID string, loaded LoadedAccount) error {
	// Step 1 — guard PersistOnClose.
	if !loaded.Account.PersistOnClose {
		slog.Warn("lifecycle: session_save refused on non-canonical account",
			"session_id", sessionID,
			"account_id", loaded.Account.ID,
		)
		return ErrNotCanonical
	}

	slog.Info("lifecycle: session_save begin",
		"session_id", sessionID,
		"account_id", loaded.Account.ID,
	)

	// Step 2 — assert flat. These are dev-fatal invariant checks: if we
	// reach save with unsettled state, the caller's teardown sequence
	// broke (auto-square did not run, or a resting order survived it).
	assertFlatOrPanic(loaded.Account)

	// Step 3 — reconcile. Available may be negative (Option B); Settled
	// is >= 0. Their sum is the final signed trading_cash.
	finalTradingCash := loaded.Account.Cash.Available + loaded.Account.Cash.Settled

	durable := persistence.DurableAccount{
		AccountID:    loaded.Account.ID,
		OwnerType:    loaded.OwnerType.String(),
		OwnerID:      loaded.OwnerID,
		TradingCash:  finalTradingCash, // SIGNED — no floor, no clamp (Option 4)
		BankCash:     loaded.Account.Cash.Bank,
		DripActive:   loaded.Account.DripActive,
		DripProgress: loaded.Account.DripProgress,
	}

	// Step 4 — Postgres UPDATE. On failure, RETURN WITHOUT deleting the
	// snapshot: the snapshot is the only surviving copy of the final
	// state (doc §5 step 5 — "the one move that actually loses money
	// state").
	if err := service.persist.SaveAccount(ctx, durable); err != nil {
		slog.Error("lifecycle: session_save ErrSaveFailed (snapshot RETAINED)",
			"session_id", sessionID,
			"account_id", loaded.Account.ID,
			"cause", err,
		)
		return ErrSaveFailedFor{AccountID: loaded.Account.ID, Cause: err}
	}

	// Step 5 — Redis discard. Postgres has committed; the safety net is
	// obsolete. Failure here is non-fatal — a stale key expires via TTL
	// and the durable state is correct.
	if err := service.persist.DeleteSnapshot(ctx, sessionID, string(loaded.Account.ID)); err != nil {
		slog.Warn("lifecycle: session_save DeleteSnapshot failed (non-fatal — Postgres already committed)",
			"session_id", sessionID,
			"account_id", loaded.Account.ID,
			"cause", err,
		)
		// Deliberately return nil: the durable state is correct; the
		// stale key is a cleanup nit, not a session-level failure.
	}

	slog.Info("lifecycle: session_save ok",
		"session_id", sessionID,
		"account_id", loaded.Account.ID,
		"trading_cash", finalTradingCash,
	)
	return nil
}

// assertFlatOrPanic enforces the ordering contract with 5.4 (auto-square
// → trades flushed → save). A non-flat account at save time means the
// sequence broke — dev-fatal per doc §5 step 2.
//
// Two invariants:
//  1. Cash.Reserved == 0 — no reservation cash locked.
//  2. Every Position has Available == 0 AND Reserved == 0 — no live units.
//
// A residual position ENTRY in the map with all-zero counts is OK (5.4
// may leave the map key after RemovePosition drains it); only non-zero
// counts panic.
func assertFlatOrPanic(live *account.Account) {
	if live.Cash.Reserved != 0 {
		panic("lifecycle.session_save: reserved cash at close (auto-square did not release reservations)")
	}
	for instrumentID, position := range live.Positions {
		if position == nil {
			continue
		}
		if position.Available != 0 || position.Reserved != 0 {
			panic("lifecycle.session_save: unsquared position at close: instrument=" +
				string(instrumentID) +
				" (auto-square did not zero units)")
		}
	}
}
