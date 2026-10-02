package funnel

import (
	"log/slog"

	"simulator/contract"
)

// punch delivers an already-built InboundMessage onto this session's
// write-only exchange inbound edge. Runs ON the owner goroutine (called
// from a shard handler for a live session the caller has already
// resolved from shard.sessions).
//
// Three disciplines live in the two-line body (BROKER_PHASE4 §1.4):
//
//  1. BrokerID stamp. The shard owns the invariant that every outbound
//     message carries THIS broker's id; stamping here means no Phase-5
//     caller can forget it. The exchange echoes BrokerID back on the
//     EngineResult, which is what lets the settlement consumer filter
//     for our broker and route by OrderID (P4-D1, P4-D4).
//
//  2. Blocking send, no timeout, no select-default. The edge is
//     unbuffered (exchange/wire/exchange.go:62) and the exchange ingress
//     is a dedicated always-draining goroutine (exchange/wire/ingress.go),
//     so the send returns immediately in practice. Silently dropping
//     (default) or timing out an order is wrong — backpressure is the
//     exchange's to own (P4-D2).
//
//  3. Fire-and-forget. No reply channel; the exchange answers
//     asynchronously on the Private stream, correlated by
//     OrderID + BrokerID. The handler that called punch does not wait
//     on the exchange (P4-D4).
//
// Stop-safety is teardown-ordering's job, not a per-punch guard: a live
// session cannot point at a stopped exchange because every session bound
// to an instance is torn down before that instance's Stop() runs
// (BROKER_PHASE2 SweepMarketClose + instance-lifecycle). A post-Stop
// punch would panic ("send on closed channel"), which is the correct
// surfacing of a real teardown-ordering bug — a recover() here would
// mask it (P4-D3).
//
// Scope: punch only moves the message. Building the FundedOrder,
// running Gate-4 reservation, minting the OrderID, and creating the
// OrderTrack are Phase 5 (handlePlaceOrder / handleCancel).
func (ownerShard *shard) punch(liveSession *ownedSession, outboundMessage contract.InboundMessage) {
	outboundMessage.BrokerID = ownerShard.brokerID
	slog.Debug("funnel: punch sending to exchange",
		"broker_id", outboundMessage.BrokerID,
		"session_id", liveSession.binding.SessionID,
		"exchange_id", liveSession.binding.ExchangeID,
		"instrument", outboundMessage.Instrument,
		"kind", outboundMessage.Kind,
	)
	liveSession.binding.ExchangeInbound <- outboundMessage
}
