package funnel

import (
	"simulator/contract"
	"simulator/economy/lifecycle"
)

// ownedSession is one live session's state — the shard owns it outright
// and nobody else reads it (BROKER_TIER0 §Point-5). Held in
// shard.sessions, keyed by AccountID.
//
// Four fields, each carrying a specific concern:
//
//	account      the in-memory account, wrapped in the lifecycle
//	             LoadedAccount envelope (Account + OwnerType + OwnerID)
//	             because session save / discard need the whole envelope,
//	             not the bare *Account. Code that needs the raw account
//	             uses .account.Account (e.g. shard.Get above).
//	binding      the outbound punch handle for this session — the ONE
//	             exchange inbound channel this session sends to
//	             (canonical vs on-demand invisible here; see binding.go).
//	participant  the self-match id stamped on every punch; distinct
//	             from BrokerID (BROKER_TIER0 §Point-5 appendix 3).
//	instruments  the per-session allowlist consulted by validateSpec
//	             (BROKER_PHASE5 §1.4 P5-D9). Set at install time —
//	             handleInstallSession defaults to permissiveInstruments{}
//	             until session-install populates InstallSession.Instruments
//	             with a sealed snapshot of the live universe.
//
// Lifetime: created at session load (a lifecycle-tier concern), read/mutated
// throughout the session by the shard's goroutine only, removed at session
// end.
type ownedSession struct {
	account     lifecycle.LoadedAccount
	binding     SessionBinding
	participant ParticipantID
	instruments InstrumentSet
}

// knownInstrument is validateSpec's instrument check, routed through the
// session's InstrumentSet. Separate method so the call site reads naturally
// (`if !sess.knownInstrument(order.Instrument)`), matching the Phase-5 doc
// pseudocode and keeping the InstrumentSet concrete type invisible to the
// handler.
//
// Nil-safe: if instruments was never set (should not happen in production —
// handleInstallSession always installs permissiveInstruments{} at least),
// returns true so a mis-wired session fails open rather than closed. The
// fail-open posture is the Phase-5 production stance; tests that need
// closed-set behaviour set a sealed stub explicitly.
func (session *ownedSession) knownInstrument(instrument contract.InstrumentID) bool {
	if session.instruments == nil {
		return true
	}
	return session.instruments.Contains(instrument)
}
