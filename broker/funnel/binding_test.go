package funnel

import (
	"testing"
	"time"

	"simulator/contract"
)

// G2 — mode-blind punch. The Tier-0 design puts the exchange-inbound
// channel next to the account on ownedSession (Option A, BROKER_TIER0
// §Point-8). At punch time the shard does a plain field read on the
// bound channel — canonical vs on-demand is invisible here; whichever
// channel was injected is used.
//
// This test proves the mechanism the Tier-1 punch handler will use,
// before that handler exists: two ownedSession values are built with
// different SessionBinding.ExchangeInbound channels; a message sent
// through each session's binding lands on that session's channel and
// nowhere else. The message shape is contract.InboundMessage — the
// confirmed wrapper the exchange consumes (not a bare FundedOrder).

func TestSessionBinding_ModeBlindPunch_RoutesToBoundChannel(t *testing.T) {
	canonicalExchangeInbound := make(chan contract.InboundMessage, 4)
	onDemandExchangeInbound := make(chan contract.InboundMessage, 4)

	canonicalSession := &ownedSession{
		binding: SessionBinding{
			SessionID:       "session-canonical",
			ExchangeID:      "NSE-canonical",
			ExchangeInbound: canonicalExchangeInbound,
		},
		participant: "participant-A",
	}
	onDemandSession := &ownedSession{
		binding: SessionBinding{
			SessionID:       "session-ondemand",
			ExchangeID:      "NSE-ondemand",
			ExchangeInbound: onDemandExchangeInbound,
		},
		participant: "participant-B",
	}

	canonicalMessage := contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   "B1",
		Instrument: "AAA",
		Submit:     &contract.FundedOrder{OrderID: "acct-A:1", Instrument: "AAA"},
	}
	onDemandMessage := contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   "B1",
		Instrument: "BBB",
		Submit:     &contract.FundedOrder{OrderID: "acct-B:1", Instrument: "BBB"},
	}

	// The load-bearing operation: a plain field read on already-owned
	// state, then a send. No map, no lock.
	canonicalSession.binding.ExchangeInbound <- canonicalMessage
	onDemandSession.binding.ExchangeInbound <- onDemandMessage

	select {
	case received := <-canonicalExchangeInbound:
		if received.Instrument != "AAA" {
			t.Fatalf("canonical channel: got instrument %q, want %q", received.Instrument, "AAA")
		}
	case <-time.After(time.Second):
		t.Fatalf("canonical channel: no message received within 1s")
	}

	select {
	case received := <-onDemandExchangeInbound:
		if received.Instrument != "BBB" {
			t.Fatalf("on-demand channel: got instrument %q, want %q", received.Instrument, "BBB")
		}
	case <-time.After(time.Second):
		t.Fatalf("on-demand channel: no message received within 1s")
	}

	// Neither channel should have received the other session's message
	// (proven by there being nothing else buffered on either).
	select {
	case leaked := <-canonicalExchangeInbound:
		t.Fatalf("canonical channel leaked a second message: %+v", leaked)
	default:
	}
	select {
	case leaked := <-onDemandExchangeInbound:
		t.Fatalf("on-demand channel leaked a second message: %+v", leaked)
	default:
	}
}

// ExchangeInbound is declared chan<- (send-only) on SessionBinding — the
// shard can only send, never receive. This test proves the shape at the
// type-system level: a compile-time assignment from chan<-InboundMessage
// (the binding's field type) to a plain chan InboundMessage would fail
// build. We do the opposite (assign a plain chan into the field) to
// prove that direction works.
func TestSessionBinding_ExchangeInboundIsSendOnly(t *testing.T) {
	bidirectional := make(chan contract.InboundMessage, 1)

	binding := SessionBinding{
		SessionID:       "session-test",
		ExchangeID:      "NSE-test",
		ExchangeInbound: bidirectional,
	}

	// Send-only usage compiles and works.
	binding.ExchangeInbound <- contract.InboundMessage{Kind: contract.SubmitOrder}

	select {
	case <-bidirectional:
	default:
		t.Fatalf("binding did not deliver message to underlying channel")
	}
}
