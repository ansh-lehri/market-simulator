package funnel

import "simulator/contract"

// SessionBinding is the per-session outbound half of the exchange seam
// (BROKER_TIER0 §Point-8). It lives NEXT TO the account on ownedSession
// and is read as a plain field at punch time — no lookup, no lock. That
// co-location is the load-bearing decision Option-A: the punch hot path
// stays contention-free (a field read on already-owned state beats a
// shared map every shard hits), the binding's lifetime equals the
// session's (loads and unloads with the account), and single-writer
// stays intact (only the owner reads it).
//
// The payoff is that Tier-1's punch is MODE-BLIND — the shard sends to
// sess.binding.ExchangeInbound whatever channel was bound; canonical vs
// on-demand lives entirely in what was injected, never in punch logic
// (BROKER_TIER0 §Point-8 pseudo-flow).
//
// ExchangeInbound is the load-bearing field — write-only (chan<-), the
// exchange's inbound edge. Results come back a different way, not on
// this channel. The element type is contract.InboundMessage (the
// confirmed wrapper carrying Kind + BrokerID + Instrument + Submit /
// Cancel), not a bare FundedOrder.
//
// How a binding is injected — how the channel reaches the broker and
// how a session loads onto the owning shard — is the lifecycle tier's
// concern (BROKER_TIER0 §Point-8 boundary). Tier 0 locks only the
// struct and the carrying model.
type SessionBinding struct {
	SessionID       SessionID
	ExchangeID      ExchangeID
	ExchangeInbound chan<- contract.InboundMessage
}
