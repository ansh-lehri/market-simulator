package lifecycle

import (
	"context"
	"log/slog"
)

// SessionDiscard is LAYER_5_2_LIFECYCLE_FLOWS item 22 — the on-demand
// session's teardown. Drops the Redis snapshot and returns; nothing
// touches Postgres. The isolated on-demand session vanishes without a
// trace; the durable account is exactly as it was at load time.
//
// Guard is DELIBERATELY ASYMMETRIC with SessionSave (doc §6):
//   - SessionSave  refuses a mis-routed on-demand with ErrNotCanonical
//     (RETURN, safe — refusing to write cannot corrupt anything).
//   - SessionDiscard PANICS on a mis-routed canonical, because
//     discarding a canonical session silently throws away real money
//     progress with NO recovery path (snapshot deleted, no error). This
//     is irreversible data loss — the loudest possible signal is the
//     right response.
//
// Together the two guards route bidirectionally-safe: save-refuses-on-
// demand, discard-panics-on-canonical.
//
// Combined with realize's on-demand gate (5.4 skips EnqueueClosedTrade
// when PersistOnClose=false), the on-demand session leaves ZERO durable
// footprint: no account update, no trade rows, no cost rows. Real P&L
// was visible in-session (from in-memory + Redis while live) and
// evaporates at discard.
func (service *Service) SessionDiscard(ctx context.Context, sessionID string, loaded LoadedAccount) error {
	// Dev-fatal invariant: canonical accounts MUST go through SessionSave.
	// This is not a returnable error — a silent discard here would lose
	// real trading progress with no recovery.
	if loaded.Account.PersistOnClose {
		panic("lifecycle.session_discard: canonical account (PersistOnClose=true) reached discard — must go through session_save; discarding here would silently lose money progress")
	}

	slog.Info("lifecycle: session_discard begin",
		"session_id", sessionID,
		"account_id", loaded.Account.ID,
	)

	// Redis DEL only. Postgres is NEVER touched — the whole point of the
	// on-demand no-footprint promise.
	if err := service.persist.DeleteSnapshot(ctx, sessionID, string(loaded.Account.ID)); err != nil {
		// Surface as-is (not wrapped). Unlike session_save's post-commit
		// DELETE (non-fatal because Postgres already committed correct
		// state), here a failed delete leaves a stale snapshot key
		// AROUND; the caller may want to log / retry / route.
		slog.Error("lifecycle: session_discard DeleteSnapshot failed",
			"session_id", sessionID,
			"account_id", loaded.Account.ID,
			"cause", err,
		)
		return err
	}

	slog.Info("lifecycle: session_discard ok",
		"session_id", sessionID,
		"account_id", loaded.Account.ID,
	)
	return nil
}
