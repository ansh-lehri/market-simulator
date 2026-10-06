package funnel

import (
	"log/slog"

	"simulator/economy/lifecycle"
)

// InstallSession is BROKER_PHASE2 §3's first command crossing the Tier-0
// funnel. Off-shard StartSession builds the LoadedAccount (slow Store I/O)
// and the SessionBinding, then routes this command to the owning shard;
// the shard's handler is pure in-memory: XOR guard + map write + reply.
//
// Why a command and not an inline map-write: the sessions map is owned by
// the shard goroutine. Single-writer protects it (no mutex, by design) —
// which means every write must land on the owner. The slow I/O does NOT
// have to land on the owner (it is not shard-owned state), so the design
// splits: off-shard load, on-shard install.
//
// Fields:
//
//   - AccountID    — routing key the funnel's hash() reads via Account()
//   - Loaded       — the whole lifecycle.LoadedAccount envelope (Account +
//                    OwnerType + OwnerID); teardown's SessionSave needs the
//                    identity fields, so Phase 2 carries them verbatim
//                    rather than passing a bare *account.Account.
//   - Binding      — the per-session exchange wiring, injected at
//                    session-install; stored on ownedSession for the
//                    mode-blind punch (Tier 1).
//   - Participant  — the self-match id stamped on every outbound punch;
//     distinct from BrokerID.
//   - Instruments  — the allowlist validateSpec reads at place time
//     (BROKER_PHASE5 §1.4 P5-D9). Optional: nil ⇒ the handler
//     defaults to permissiveInstruments{} so lifecycle plumbing
//     works unchanged until session-install populates it with a
//     sealed set. Carried, cross-point.
//   - Reply        — buffered(1), caller-owned. handleInstallSession sends
//     exactly once: nil on install, ErrSessionAlreadyLive
//     on XOR refusal. The buffered send means the shard
//     never blocks even if the caller has walked away
//     (ctx timeout path in waitReply).
type InstallSession struct {
	AccountID   AccountID
	Loaded      lifecycle.LoadedAccount
	Binding     SessionBinding
	Participant ParticipantID
	Instruments InstrumentSet
	Reply       chan error
}

// Account returns the routing key for the funnel router.
func (cmd InstallSession) Account() AccountID { return cmd.AccountID }

// handleInstallSession runs on the owner goroutine. The whole body is XOR
// guard + map write + reply — nothing else (BROKER_PHASE2 §3 P2-3·D5).
// No order flow, no nextOrderID special case: a fresh load has an empty
// split and empty tracks by construction (hydrateFreshAccount in
// economy/lifecycle), so there is nothing to seed here.
//
// XOR invariant: if an ownedSession already exists for the AccountID, the
// install is refused with ErrSessionAlreadyLive. Install never
// overwrites. The one-live-session rule (canonical XOR on-demand per user)
// is enforced here as its last line of defense; the orchestration
// registry upholds it upstream, but the funnel does not trust that at
// this seam.
func (ownerShard *shard) handleInstallSession(cmd InstallSession) {
	if _, live := ownerShard.sessions[cmd.AccountID]; live {
		slog.Warn("funnel: handleInstallSession refusing — session already live",
			"account_id", cmd.AccountID,
			"session_id", cmd.Binding.SessionID,
		)
		cmd.Reply <- ErrSessionAlreadyLive
		return
	}
	// Default an unset InstrumentSet to permissive — until session-install
	// populates the command field, every live session should fail open
	// rather than closed (BROKER_PHASE5 §1.4 P5-D9).
	installedInstruments := cmd.Instruments
	if installedInstruments == nil {
		installedInstruments = permissiveInstruments{}
	}
	ownerShard.sessions[cmd.AccountID] = &ownedSession{
		account:     cmd.Loaded,
		binding:     cmd.Binding,
		participant: cmd.Participant,
		instruments: installedInstruments,
	}
	slog.Info("funnel: handleInstallSession installed",
		"account_id", cmd.AccountID,
		"session_id", cmd.Binding.SessionID,
		"exchange_id", cmd.Binding.ExchangeID,
		"persist_on_close", cmd.Loaded.Account.PersistOnClose,
	)
	cmd.Reply <- nil
}
