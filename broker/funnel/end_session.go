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
//     on mismatch, so a stale uninstall cannot overwrite a replaced
//     session. Validated non-empty off-shard.
//   - Mode      — selects the teardown branch. This phase implements
//     Canonical (detach-keep: uninstall + park LoadedAccount for EOD
//     FinalizeMarket). OnDemand (discard: uninstall + SessionDiscard +
//     unload) ships in the on-demand teardown phase; EndSession returns
//     ErrModeNotImplemented until then.
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
//       unsupported Mode) before any routing so a malformed trigger
//       does not touch the shard.
//     - Mode gate: Canonical → proceed to stage 2; OnDemand →
//       ErrModeNotImplemented (sibling phase); anything else → programming
//       error (unknown Mode value).
//
//  2. ON-SHARD, via the funnel command path:
//     - Route an UninstallSession command onto the owning shard.
//     - Wait for the shard's reply (nil on uninstall, ErrNoLiveSession /
//       ErrSessionMismatch on refusal), or the caller's ctx fires →
//       ErrBrokerTimeout.
//
// Why the uninstall runs on-shard: `sessions` is the shard's owned map;
// single-writer requires every mutation to land on the owner. There is
// no slow I/O to move off-shard (no Postgres read, no Redis write — the
// account stays loaded; persistence happens later at FinalizeMarket),
// so stage 1 does the validate and stage 2 is the ONLY stage that costs
// a shard round-trip.
//
// Error contract:
//
//   - nil                      → session uninstalled, LoadedAccount
//                                 parked in shard.detached for EOD
//                                 FinalizeMarket.
//   - ErrBadAccountID          → empty AccountID; off-shard reject.
//   - ErrBadSessionID          → empty SessionID; off-shard reject.
//   - ErrModeNotImplemented    → Mode == OnDemand; off-shard reject.
//   - error from fmt.Errorf    → unknown Mode (programming error at the
//                                 caller — Mode must be Canonical or
//                                 OnDemand).
//   - ErrNoLiveSession         → no ownedSession on the shard for the
//                                 AccountID. Idempotent on the caller
//                                 side: the account was already detached
//                                 or never installed.
//   - ErrSessionMismatch       → live session's SessionID differs from
//                                 the command's. Stale retry.
//   - ErrBrokerTimeout         → shard did not reply before ctx fired;
//                                 the uninstall may still have landed
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
	case lifecycle.Canonical:
		// proceed
	case lifecycle.OnDemand:
		slog.Warn("funnel: EndSession OnDemand branch not implemented this phase",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
		)
		return ErrModeNotImplemented
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

	// Stage 2 — route the UninstallSession command and wait for the
	// shard's reply.
	reply := make(chan error, 1) // buffered-1 per Tier-0 §7
	broker.route(UninstallSession{
		AccountID: cmd.AccountID,
		SessionID: cmd.SessionID,
		Reply:     reply,
	})
	uninstallErr := broker.waitReply(ctx, reply)
	if uninstallErr != nil {
		slog.Error("funnel: EndSession uninstall failed",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"err", uninstallErr,
		)
		return uninstallErr
	}

	slog.Info("funnel: EndSession ok",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"mode", cmd.Mode,
	)
	return nil
}
