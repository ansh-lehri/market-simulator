package funnel

import (
	"context"
	"fmt"
	"log/slog"

	"simulator/economy/lifecycle"
)

// SessionEndCmd is the conductor's payload for ending one live session.
// Shape parallels SessionStartCmd so the lifecycle surface stays
// symmetric: {start, end} × {Canonical, OnDemand}. The broker owns the
// delivery onto the owning shard; the conductor fills these fields from
// its own registry at exit time.
//
// Fields:
//
//   - AccountID — routing key hashed to the owning shard. Validated
//     non-empty off-shard.
//   - SessionID — the live session the caller wants to end. The shard
//     handler compares against the live binding.SessionID and refuses
//     on mismatch, so a stale teardown cannot overwrite a replaced
//     session. Validated non-empty off-shard.
//   - Mode      — selects the teardown branch:
//     Canonical  → detach-keep: uninstall binding + park LoadedAccount
//                  in shard.detached for the market-close FinalizeMarket
//                  to persist and unload.
//     OnDemand   → discard: drop the ephemeral LoadedAccount entirely
//                  (sessions + per-exchange index both pruned) and
//                  delete the Redis snapshot. Nothing is persisted —
//                  the on-demand account carries PersistOnClose=false
//                  from load, which gates every durable write.
type SessionEndCmd struct {
	AccountID AccountID
	SessionID SessionID
	Mode      lifecycle.SessionType
}

// EndSession ends one live session on its owning shard. Two stages,
// matching the StartSession shape:
//
//  1. OFF-SHARD, on the caller's goroutine:
//     - Validate the command (empty AccountID / empty SessionID /
//       unknown Mode value) before any routing so a malformed trigger
//       does not touch the shard.
//
//  2. ON-SHARD, via the funnel command path:
//     - Canonical → route an UninstallSession command (detach-keep;
//       pure in-memory on the shard).
//     - OnDemand  → route a DiscardSession command (drop + Redis
//       snapshot delete; the ctx deadline plumbs through so the
//       shard's blocking Redis call is bounded).
//     - Wait for the shard's reply, or ErrBrokerTimeout if the caller's
//       ctx fires first.
//
// Why the teardown runs on-shard: `sessions` and the per-exchange index
// are the shard's owned maps; single-writer requires every mutation to
// land on the owner. Canonical has no I/O (nothing to move off-shard);
// on-demand's Redis delete is bounded enough to run on the owner
// alongside the state cleanup, same pattern as FinalizeMarket's save.
//
// Error contract:
//
//   - nil                      → session ended cleanly. For Canonical:
//                                 session uninstalled, LoadedAccount
//                                 parked for market-close FinalizeMarket.
//                                 For OnDemand: ephemeral account dropped
//                                 from the shard, Redis snapshot deleted.
//   - ErrBadAccountID          → empty AccountID; off-shard reject.
//   - ErrBadSessionID          → empty SessionID; off-shard reject.
//   - error from fmt.Errorf    → unknown Mode (programming error at the
//                                 caller — Mode must be Canonical or
//                                 OnDemand).
//   - ErrNoLiveSession         → no ownedSession on the shard for the
//                                 AccountID. Idempotent on the caller
//                                 side: the account was already ended
//                                 or never installed.
//   - ErrSessionMismatch       → live session's SessionID differs from
//                                 the command's. Stale retry.
//   - ErrDiscardFailed (OnDemand only, wrapped via %w) → shard-side
//                                 maps were cleaned, but the Redis
//                                 snapshot delete failed. The ephemeral
//                                 account is gone from the shard; a
//                                 stale snapshot key is TTL-cleaned.
//                                 Caller should log and proceed.
//   - ErrBrokerTimeout         → shard did not reply before ctx fired;
//                                 the teardown may still have landed
//                                 (caller reconciles).
func (broker *Broker) EndSession(ctx context.Context, cmd SessionEndCmd) error {
	// Stage 1 — validate off-shard.
	if cmd.AccountID == "" {
		slog.Error("funnel: EndSession ErrBadAccountID",
			"session_id", cmd.SessionID,
		)
		return ErrBadAccountID
	}
	if cmd.SessionID == "" {
		slog.Error("funnel: EndSession ErrBadSessionID",
			"account_id", cmd.AccountID,
		)
		return ErrBadSessionID
	}
	switch cmd.Mode {
	case lifecycle.Canonical, lifecycle.OnDemand:
		// proceed — Mode value is known; the on-shard branch below
		// picks the right command type.
	default:
		slog.Error("funnel: EndSession unknown Mode",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"mode", int(cmd.Mode),
		)
		return fmt.Errorf("funnel: EndSession: unknown Mode %v (want Canonical|OnDemand)", cmd.Mode)
	}

	slog.Info("funnel: EndSession begin",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"mode", cmd.Mode,
	)

	// Stage 2 — route the teardown command for the picked Mode and
	// wait for the shard's reply. Reply channel is buffered-1 so the
	// shard's send never blocks even if the caller's ctx fires first
	// and waitReply has already walked away.
	reply := make(chan error, 1)
	switch cmd.Mode {
	case lifecycle.Canonical:
		broker.route(UninstallSession{
			AccountID: cmd.AccountID,
			SessionID: cmd.SessionID,
			Reply:     reply,
		})
	case lifecycle.OnDemand:
		broker.route(DiscardSession{
			Ctx:       ctx,
			AccountID: cmd.AccountID,
			SessionID: cmd.SessionID,
			Reply:     reply,
		})
	}

	teardownErr := broker.waitReply(ctx, reply)
	if teardownErr != nil {
		slog.Error("funnel: EndSession teardown failed",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"mode", cmd.Mode,
			"err", teardownErr,
		)
		return teardownErr
	}

	slog.Info("funnel: EndSession ok",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"mode", cmd.Mode,
	)
	return nil
}
