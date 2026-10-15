package funnel

import "log/slog"

// UninstallSession is the shard-internal command the broker's EndSession
// method routes to the owning shard to end one live session WITHOUT
// persisting, squaring, or unloading the account. The canonical
// detach-keep path: the LoadedAccount survives on the shard (parked in
// shard.detached) so a later rejoin can re-install over its carried
// positions and FinalizeMarket at EOD can persist + unload it.
//
// Fields:
//
//   - AccountID    — routing key the funnel's hash() reads via Account().
//   - SessionID    — the SessionID the caller believes is live on the
//                    account. The handler verifies this matches the live
//                    binding.SessionID before uninstalling; a mismatch
//                    (stale retry, replaced session) is refused.
//   - Reply        — buffered(1), caller-owned. The handler sends exactly
//                    once: nil on uninstall, ErrNoLiveSession when no
//                    session is live for the AccountID (idempotent — treat
//                    as "already detached"), or ErrSessionMismatch when
//                    the live session's SessionID does not match.
//
// Shape mirrors InstallSession deliberately — the shard-command surface
// of the session lifecycle stays symmetric.
type UninstallSession struct {
	AccountID AccountID
	SessionID SessionID
	Reply     chan error
}

// Account returns the routing key for the funnel router.
func (cmd UninstallSession) Account() AccountID { return cmd.AccountID }

// handleUninstallSession runs on the owner goroutine. Three outcomes:
//
//  1. ownedSession missing — reply ErrNoLiveSession. The caller
//     (conductor leave flow) treats this as idempotent: the account was
//     already detached, or never installed in the first place. No panic:
//     the race window between a conductor leave and some other reap
//     (future server shutdown, admin-forced reset) is legal.
//
//  2. live session's binding.SessionID != cmd.SessionID — reply
//     ErrSessionMismatch. The command is stale (an old retry, or a
//     conductor pointing at a session that was replaced). Refuse so the
//     live binding stays intact; the caller can re-resolve and retry.
//
//  3. match — delete from `sessions`, park a detachedAccount in
//     `detached` carrying the LoadedAccount + the SessionID + the
//     ExchangeID (so FinalizeMarket can save it later without walking
//     the index for the ExchangeID). The loadedByExchange index entry
//     STAYS — the account is still loaded on the shard, just parked.
//     Reply nil.
//
// No persist, no square, no unload. The detach-keep contract: the
// account carries its positions, cash buckets, and reservation book
// across the detach boundary; a rejoin picks them up by re-installing
// over the same AccountID (XOR is free now).
func (ownerShard *shard) handleUninstallSession(cmd UninstallSession) {
	liveSession, present := ownerShard.sessions[cmd.AccountID]
	if !present {
		slog.Warn("funnel: handleUninstallSession no live session",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
		)
		cmd.Reply <- ErrNoLiveSession
		return
	}
	if liveSession.binding.SessionID != cmd.SessionID {
		slog.Warn("funnel: handleUninstallSession SessionID mismatch — refusing",
			"account_id", cmd.AccountID,
			"cmd_session_id", cmd.SessionID,
			"live_session_id", liveSession.binding.SessionID,
		)
		cmd.Reply <- ErrSessionMismatch
		return
	}

	ownerShard.detached[cmd.AccountID] = detachedAccount{
		loaded:        liveSession.account,
		lastSessionID: liveSession.binding.SessionID,
		exchangeID:    liveSession.binding.ExchangeID,
	}
	delete(ownerShard.sessions, cmd.AccountID)

	slog.Info("funnel: handleUninstallSession detached",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"exchange_id", liveSession.binding.ExchangeID,
	)
	cmd.Reply <- nil
}
