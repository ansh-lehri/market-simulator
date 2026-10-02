package funnel

import (
	"testing"
	"time"

	"simulator/contract"
)

// newPunchHarness builds the minimal shard + ownedSession pair needed to
// exercise punch against a caller-supplied inbound edge. The shard carries
// only the fields punch touches (brokerID); every other field stays
// zero-value because punch never reads them.
func newPunchHarness(
	brokerIDUnderTest contract.BrokerID,
	exchangeInbound chan<- contract.InboundMessage,
) (*shard, *ownedSession) {
	ownerShard := &shard{brokerID: brokerIDUnderTest}
	liveSession := &ownedSession{
		binding: SessionBinding{
			SessionID:       "sess-under-test",
			ExchangeID:      "exch-under-test",
			ExchangeInbound: exchangeInbound,
		},
	}
	return ownerShard, liveSession
}

// TestShardPunch_StampsBrokerIDOnOutbound — the load-bearing invariant:
// no matter what BrokerID the caller put on the message (including a
// different broker's id, a typo, or the zero value), punch overwrites it
// with the shard's own brokerID before the send (BROKER_PHASE4 §1.4,
// discipline 1). This is what guarantees every result on Private can be
// routed back to the originating broker.
func TestShardPunch_StampsBrokerIDOnOutbound(t *testing.T) {
	exchangeInbound := make(chan contract.InboundMessage, 1)
	ownerShard, liveSession := newPunchHarness("B-under-test", exchangeInbound)

	callerMessage := contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   "B-caller-mistakenly-set",
		Instrument: "ACME",
		Submit: &contract.FundedOrder{
			OrderID:    "acc-1:1",
			Instrument: "ACME",
			Side:       contract.Buy,
			Type:       contract.Limit,
			TIF:        contract.GTC,
			Qty:        10,
			LimitPrice: 1000,
		},
	}

	ownerShard.punch(liveSession, callerMessage)

	delivered := <-exchangeInbound
	if delivered.BrokerID != "B-under-test" {
		t.Errorf("delivered.BrokerID = %q, want %q (punch must stamp the shard's broker id)",
			delivered.BrokerID, "B-under-test")
	}

	// The caller's own copy must be untouched — punch receives
	// outboundMessage BY VALUE (not pointer), so the stamp is on the
	// local shallow copy and does not reach back to the caller's struct.
	if callerMessage.BrokerID != "B-caller-mistakenly-set" {
		t.Errorf("callerMessage.BrokerID = %q, want %q (caller copy must not be mutated)",
			callerMessage.BrokerID, "B-caller-mistakenly-set")
	}
}

// TestShardPunch_PassesSubmitAndCancelThrough — dual-branch cover:
// punch is Kind-blind by design (it does not route on Kind, the exchange
// ingress does on Instrument). The test proves the primitive forwards
// both variants unchanged — the memory-rule's "cover both branches of a
// dual-branched API explicitly."
func TestShardPunch_PassesSubmitAndCancelThrough(t *testing.T) {
	cases := []struct {
		name          string
		callerMessage contract.InboundMessage
		verify        func(t *testing.T, delivered contract.InboundMessage)
	}{
		{
			name: "submit_order_variant",
			callerMessage: contract.InboundMessage{
				Kind:       contract.SubmitOrder,
				Instrument: "ACME",
				Submit: &contract.FundedOrder{
					OrderID:    "acc-1:7",
					Instrument: "ACME",
					Side:       contract.Buy,
					Type:       contract.Market,
					TIF:        contract.IOC,
					Qty:        5,
					MaxSpend:   50_000,
				},
			},
			verify: func(t *testing.T, delivered contract.InboundMessage) {
				if delivered.Kind != contract.SubmitOrder {
					t.Errorf("delivered.Kind = %v, want SubmitOrder", delivered.Kind)
				}
				if delivered.Submit == nil {
					t.Fatalf("delivered.Submit = nil, want the FundedOrder forwarded")
				}
				if delivered.Cancel != nil {
					t.Errorf("delivered.Cancel = %+v, want nil on a submit", delivered.Cancel)
				}
				if delivered.Submit.OrderID != "acc-1:7" {
					t.Errorf("delivered.Submit.OrderID = %q, want %q",
						delivered.Submit.OrderID, "acc-1:7")
				}
				if delivered.Instrument != "ACME" {
					t.Errorf("delivered.Instrument = %q, want %q",
						delivered.Instrument, "ACME")
				}
			},
		},
		{
			name: "cancel_order_variant",
			callerMessage: contract.InboundMessage{
				Kind:       contract.CancelOrder,
				Instrument: "ACME",
				Cancel:     &contract.CancelRequest{OrderID: "acc-1:7"},
			},
			verify: func(t *testing.T, delivered contract.InboundMessage) {
				if delivered.Kind != contract.CancelOrder {
					t.Errorf("delivered.Kind = %v, want CancelOrder", delivered.Kind)
				}
				if delivered.Cancel == nil {
					t.Fatalf("delivered.Cancel = nil, want the CancelRequest forwarded")
				}
				if delivered.Submit != nil {
					t.Errorf("delivered.Submit = %+v, want nil on a cancel", delivered.Submit)
				}
				if delivered.Cancel.OrderID != "acc-1:7" {
					t.Errorf("delivered.Cancel.OrderID = %q, want %q",
						delivered.Cancel.OrderID, "acc-1:7")
				}
				if delivered.Instrument != "ACME" {
					t.Errorf("delivered.Instrument = %q, want %q",
						delivered.Instrument, "ACME")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exchangeInbound := make(chan contract.InboundMessage, 1)
			ownerShard, liveSession := newPunchHarness("B-under-test", exchangeInbound)
			ownerShard.punch(liveSession, tc.callerMessage)
			tc.verify(t, <-exchangeInbound)
		})
	}
}

// TestShardPunch_BlocksUntilExchangeReceives — proves P4-D2's blocking
// discipline: punch is a plain send, not a select-with-default and not
// a timed send. On an unbuffered edge it MUST block until a receiver
// drains; swallowing the message or timing out would be an order-loss
// bug. The exchange ingress is always-draining in production, so this
// blocking is invisible there; the test uses an on-demand receiver to
// make it observable.
func TestShardPunch_BlocksUntilExchangeReceives(t *testing.T) {
	exchangeInbound := make(chan contract.InboundMessage) // unbuffered → forces the block
	ownerShard, liveSession := newPunchHarness("B-under-test", exchangeInbound)

	punchReturned := make(chan struct{})
	go func() {
		ownerShard.punch(liveSession, contract.InboundMessage{
			Kind:       contract.SubmitOrder,
			Instrument: "ACME",
			Submit:     &contract.FundedOrder{OrderID: "acc-1:1", Instrument: "ACME"},
		})
		close(punchReturned)
	}()

	// Grace window: with no receiver yet, punch must still be parked on
	// the send. If it returns here, the send went to a buffered-or-dropped
	// path and the invariant is broken.
	select {
	case <-punchReturned:
		t.Fatal("punch returned before any receiver drained — expected a blocking send")
	case <-time.After(20 * time.Millisecond):
		// Still blocked, as required.
	}

	// Now drain. punch must return promptly after the receive.
	<-exchangeInbound
	select {
	case <-punchReturned:
		// Correct — the send completed once the receiver took the value.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("punch did not return within 100ms after the exchange received the message")
	}
}
