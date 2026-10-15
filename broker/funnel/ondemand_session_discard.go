package funnel

// This file holds the on-demand half of the session-end machinery: the
// DiscardSession shard-internal command and its handleDiscardSession
// handler. It is a sibling of uninstall_session.go (the canonical half)
// and is driven BY end_session.go (the outer broker method).
//
// --- Why a separate file (and a separate command type) --------------
//
// The two session-end paths share four lines of preamble ("live session
// exists? SessionID matches?") and diverge completely after. Three
// concrete differences justify a dedicated command + handler rather
// than overloading UninstallSession with a Mode field:
//
//  1. Different shard-side map manipulations.
//     - Canonical uninstall MOVES the LoadedAccount from `sessions`
//       into `detached` and KEEPS the loadedByExchange entry. The
//       account is still loaded on the shard, just parked for
//       FinalizeMarket at market close.
//     - On-demand discard DROPS from `sessions`, PRUNES the
//       loadedByExchange entry, and never touches `detached`. The
//       ephemeral account leaves the shard entirely.
//
//  2. Different I/O profile.
//     - Uninstall is pure in-memory. No Postgres, no Redis.
//       UninstallSession therefore carries no Ctx — there is no
//       blocking call to bound.
//     - Discard calls lifecycle.SessionDiscard which does a Redis
//       DeleteSnapshot on the shard goroutine. That blocking I/O
//       needs a bounded deadline, so DiscardSession carries a Ctx
//       field — same reason FinalizeMarketOnShard carries one.
//
//  3. Different invariant guard.
//     - Discard panics on PersistOnClose=true. A canonical account
//       reaching this path would silently delete the Redis snapshot
//       that still carries real money progress, with no recovery.
//       Dev-fatal is the only correct response.
//     - Uninstall has no equivalent panic — it never deletes anything
//       that cannot be reconstructed; a mis-routed canonical through
//       uninstall just parks a loaded account that FinalizeMarket
//       picks up normally.
//
// Overloading UninstallSession with a Mode field would mean: a struct
// field (Ctx) dead for one branch, a Mode-switch inside the handler
// over two unrelated code paths, and a dispatch-case comment that can
// no longer describe one thing. Separate commands keep each handler
// focused on one effect — matching the shipped pattern in this package
// (FinalizeMarketOnShard is a separate command from UninstallSession
// for the same reason, even though both "end state for an account").
//
// --- Relation with end_session.go -----------------------------------
//
// end_session.go is the broker-facing method EndSession plus the
// SessionEndCmd value type the conductor fills. It is Mode-blind in
// its validation (empty AccountID / SessionID / unknown Mode reject
// off-shard the same way for both branches) and Mode-aware exactly
// once: it builds the right shard command for the Mode (an
// UninstallSession for Canonical, a DiscardSession for OnDemand),
// routes it, and waits for the one reply. Nothing in that file knows
// anything about the shard-side map choreography; nothing in this
// file knows anything about validation, routing, or reply-plumbing.
//
// The split is the usual off-shard-vs-on-shard seam:
//
//   end_session.go              → off-shard validate + route + wait
//   ondemand_session_discard.go → on-shard body for the OnDemand branch
//   uninstall_session.go        → on-shard body for the Canonical branch
//
// Error contract symmetry: both branches surface ErrNoLiveSession for
// an idempotent repeat and ErrSessionMismatch for a stale SessionID.
// The OnDemand branch additionally wraps ErrDiscardFailed when the
// shard-side maps were pruned but the Redis delete failed (forward-
// progress: the account is gone from the shard regardless; a stale
// snapshot key is TTL-cleaned; the error exists for operability).

import (
	"context"
	"fmt"
	"log/slog"
)

// DiscardSession is the shard-internal command the broker's EndSession
// method routes to the owning shard to end one live on-demand session.
// The on-demand discard path: the ephemeral LoadedAccount is DROPPED
// entirely from the shard — removed from `sessions` and from the
// per-exchange index — then the service-level discard (Redis snapshot
// delete) runs. Nothing is parked for later persist, nothing is written
// to Postgres: the on-demand session leaves zero durable footprint by
// design (PersistOnClose=false on the loaded account carries that
// contract into every downstream gate).
//
// Fields:
//
//   - Ctx         — bounded by the caller's EndSession deadline; passed
//                   verbatim into the SessionDiscard call. The command
//                   carries ctx (and the sibling detach-keep command
//                   does not) because the discard path does blocking
//                   Redis I/O on the shard goroutine; the deadline must
//                   plumb through.
//   - AccountID   — routing key the funnel's hash() reads via Account().
//   - SessionID   — the SessionID the caller believes is live on the
//                   account. The handler verifies this matches the live
//                   binding.SessionID before dropping; a mismatch
//                   (stale retry, replaced session) is refused so the
//                   live binding stays intact.
//   - Reply       — buffered(1), caller-owned. The handler sends exactly
//                   once: nil on clean drop + Redis delete, a wrapped
//                   ErrDiscardFailed when the Redis delete surfaces an
//                   error (shard maps still cleaned — ephemeral account
//                   is gone regardless), ErrNoLiveSession when no
//                   session is live for the AccountID (idempotent on
//                   repeat), or ErrSessionMismatch on a stale SessionID.
//
// Shape mirrors UninstallSession plus a Ctx field — same validation
// contract (SessionID match, idempotent on no-session) but a different
// terminal effect (full drop + Redis cleanup, not detach-keep).
type DiscardSession struct {
	Ctx       context.Context
	AccountID AccountID
	SessionID SessionID
	Reply     chan error
}

// Account returns the routing key for the funnel router.
func (cmd DiscardSession) Account() AccountID { return cmd.AccountID }

// handleDiscardSession runs on the owner goroutine. Four outcomes:
//
//  1. no live session for the AccountID — reply ErrNoLiveSession. The
//     caller (conductor's on-demand teardown) treats this as idempotent:
//     a repeat EndSession after a successful discard lands here, the
//     same way canonical uninstall's second-call behavior reports.
//
//  2. live session's binding.SessionID != cmd.SessionID — reply
//     ErrSessionMismatch. The command is stale; refuse so the live
//     binding stays intact and the caller can re-resolve.
//
//  3. live session's account has PersistOnClose=true — PANIC. A
//     canonical account reaching the on-demand discard path is a wiring
//     bug (the loader stamps PersistOnClose at session load; a mismatch
//     between the loader's branch and the end-session Mode means the
//     caller is routing the wrong teardown). Silently discarding a
//     canonical account would delete the Redis snapshot that still
//     carries real money progress, with no recovery. Dev-fatal signal
//     is the only correct response.
//
//  4. match — drop from `sessions`, prune the per-exchange index entry
//     (deleting the index map itself when it becomes empty), then run
//     the service-level discard (Redis snapshot delete). The maps are
//     ALWAYS cleaned before the discard call: the ephemeral account's
//     only durable artifact is the Redis snapshot, and the user's intent
//     is to end the session — a stale snapshot is TTL-cleaned, so
//     forward-progress wins over "retry the shard-side cleanup." On a
//     discard error the reply wraps ErrDiscardFailed; on success the
//     reply is nil.
//
// Blocking: the Redis delete runs on the shard goroutine (same pattern
// as handleFinalizeMarketOnShard's save calls). Bounded by the embedded
// ctx's deadline.
func (ownerShard *shard) handleDiscardSession(cmd DiscardSession) {
	liveSession, present := ownerShard.sessions[cmd.AccountID]
	if !present {
		slog.Warn("funnel: handleDiscardSession no live session",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
		)
		cmd.Reply <- ErrNoLiveSession
		return
	}
	if liveSession.binding.SessionID != cmd.SessionID {
		slog.Warn("funnel: handleDiscardSession SessionID mismatch — refusing",
			"account_id", cmd.AccountID,
			"cmd_session_id", cmd.SessionID,
			"live_session_id", liveSession.binding.SessionID,
		)
		cmd.Reply <- ErrSessionMismatch
		return
	}
	if liveSession.account.Account.PersistOnClose {
		panic(fmt.Sprintf(
			"funnel: handleDiscardSession: account %q has PersistOnClose=true — canonical account reached on-demand discard path (wiring bug; silent discard would lose durable money progress)",
			cmd.AccountID,
		))
	}

	boundExchangeID := liveSession.binding.ExchangeID
	loadedForDiscard := liveSession.account

	// Shard-side cleanup FIRST, service-level Redis delete after. The
	// ephemeral account leaves the shard regardless of the Redis
	// outcome — a stale snapshot is TTL-cleaned, retrying would do
	// nothing useful, and the user asked us to end it.
	delete(ownerShard.sessions, cmd.AccountID)
	if accountsForExchange := ownerShard.loadedByExchange[boundExchangeID]; accountsForExchange != nil {
		delete(accountsForExchange, cmd.AccountID)
		if len(accountsForExchange) == 0 {
			delete(ownerShard.loadedByExchange, boundExchangeID)
		}
	}

	if ownerShard.discarder == nil {
		slog.Error("funnel: handleDiscardSession session discarder missing (shard not wired for discard)",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"exchange_id", boundExchangeID,
		)
		cmd.Reply <- fmt.Errorf("funnel: handleDiscardSession: account %q: session discarder missing on shard: %w",
			cmd.AccountID, ErrDiscardFailed)
		return
	}

	if discardErr := ownerShard.discarder.SessionDiscard(cmd.Ctx, string(cmd.SessionID), loadedForDiscard); discardErr != nil {
		slog.Error("funnel: handleDiscardSession SessionDiscard failed; shard maps already cleaned",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"exchange_id", boundExchangeID,
			"err", discardErr,
		)
		cmd.Reply <- fmt.Errorf("funnel: handleDiscardSession: account %q: %w: %w",
			cmd.AccountID, ErrDiscardFailed, discardErr)
		return
	}

	slog.Info("funnel: handleDiscardSession discarded",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"exchange_id", boundExchangeID,
	)
	cmd.Reply <- nil
}
