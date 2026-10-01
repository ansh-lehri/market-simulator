package funnel

import "simulator/economy/lifecycle"

// ownedSession is one live session's state — the shard owns it outright
// and nobody else reads it (BROKER_TIER0 §Point-5). Held in
// shard.sessions, keyed by AccountID.
//
// Three fields, each carrying a specific concern:
//   account      the in-memory L5 account, wrapped in the lifecycle
//                LoadedAccount envelope (Account + OwnerType + OwnerID)
//                because session save / discard need the whole envelope,
//                not the bare *Account. Code that needs the raw account
//                uses .account.Account (e.g. shard.Get above).
//   binding      the outbound punch handle for this session — the ONE
//                exchange inbound channel this session sends to
//                (canonical vs on-demand invisible here; see binding.go).
//   participant  the self-match id stamped on every punch; distinct
//                from BrokerID (BROKER_TIER0 §Point-5 appendix 3).
//
// Lifetime: created at session load (a lifecycle-tier concern, out of
// scope for Phase 0), read/mutated throughout the session by the shard's
// goroutine only, removed at session end. Phase 0 leaves the map empty
// at construction and never populates it.
type ownedSession struct {
	account     lifecycle.LoadedAccount
	binding     SessionBinding
	participant ParticipantID
}
