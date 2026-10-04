package funnel

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/gate"
	"simulator/exchange/wire"
)

// TestExchangeSeamIntegration_PunchToSettlementRoundTrip wires a real
// *wire.Exchange to the Phase-4 primitives and proves the full order
// loop closes end-to-end:
//
//   punch (Point 1)  →  exchange.Inbound  →  ingress  →  shard  →
//   emitter.EmitResult  →  exchange.Private  →  runSettlementConsumer
//   (Point 2)  →  accountOf  →  broker.route  →  ApplyResult on the
//   owning broker shard's inbox.
//
// The assertion is observational: an ApplyResult lands on the broker
// shard that hashes to the parsed AccountID, carrying the exchange's
// EngineResult verbatim with the stamped BrokerID echoed back. The exact
// Outcome is not asserted — the hand-built FundedOrder lands on an empty
// book with no gate inputs tuned for a specific match path, so any of
// {OutcomeMatched (with Rested), OutcomeRejected} is acceptable. What
// IS asserted is that one result arrives, is OUR broker's, carries OUR
// OrderID, and routes to the shard hashAccount picks.
//
// The broker shards are NOT started — we inspect each shard's inbox
// directly to observe the routed command, which is only safe when no
// run goroutine is competing for it (consistent with the other
// consumer tests).
func TestExchangeSeamIntegration_PunchToSettlementRoundTrip(t *testing.T) {
	const (
		brokerIDUnderTest    contract.BrokerID    = "B-integration"
		accountIDUnderTest                        = "integration-acct"
		orderIDUnderTest     contract.OrderID     = "integration-acct:1"
		instrumentUnderTest  contract.InstrumentID = "ACME"
		brokerShardCount                          = 2
		brokerInboxDepth                          = 16
		exchangeShardCount                        = 1
		exchangeTickInterval                      = 5 * time.Millisecond
	)

	// --- Build the exchange ------------------------------------------------
	// Schedule with PhaseContinuous already active, so an order submitted
	// during the test is admissible (not RejectMarketClosed).
	now := time.Now()
	schedule := clock.SessionSchedule{
		Boundaries: []clock.PhaseBoundary{
			{At: now.Add(-time.Second), Phase: clock.PhaseContinuous},
			{At: now.Add(time.Hour), Phase: clock.PhaseClosed},
		},
	}
	instrumentSpec := wire.InstrumentSpec{
		Instrument:    instrumentUnderTest,
		Reference:     10000,
		BandBps:       1000,
		Rules:         gate.Rulebook{TickSize: 1},
		OHLCVInterval: time.Minute,
		OHLCVStart:    now,
	}
	exchange := wire.NewExchange(exchangeShardCount, schedule, []wire.InstrumentSpec{instrumentSpec})

	// --- Build the broker (not started; test probes inboxes directly) -----
	broker := newConsumerHarness(t, brokerIDUnderTest, brokerShardCount, brokerInboxDepth)

	// Session binding points at the real exchange edge. In production L7
	// injects this at session install; here the test does it manually
	// since we only have a punch to exercise, no session lifecycle.
	ownerShardForOrder := broker.shards[hashAccount(accountIDUnderTest, brokerShardCount)]
	liveSession := &ownedSession{
		binding: SessionBinding{
			SessionID:       "sess-integration",
			ExchangeID:      "exch-integration",
			ExchangeInbound: exchange.Inbound,
		},
	}

	// --- Start exchange + consumer ----------------------------------------
	// Public tape must have a drain or the exchange's shard loop will
	// wedge on EmitSession/EmitQuote/EmitDepth (Public is unbuffered —
	// exchange/wire/exchange.go:63). The Phase-4 market-data ingestor
	// (Points 3-5, deferred) is that drain in production; the test
	// substitutes a bare drain goroutine. Exits when exchange.Stop
	// closes Public.
	publicDrainExited := make(chan struct{})
	go func() {
		defer close(publicDrainExited)
		for range exchange.Public {
		}
	}()

	exchange.Start(exchangeTickInterval)
	consumerExited := make(chan struct{})
	go func() {
		defer close(consumerExited)
		broker.runSettlementConsumer(exchange.Private)
	}()

	// Give the monitor one tick to observe Closed→Continuous and fire
	// SessionOpened fan-out before the first submit lands — mirrors the
	// pattern in exchange/wire/vertical_slice_test.go.
	time.Sleep(4 * exchangeTickInterval)

	// --- Punch one order through the real seam ----------------------------
	// Limit buy on an empty book: with no asks, nothing matches and the
	// remainder rests → OutcomeMatched with Rested != nil, zero trades.
	// The test does not pin the outcome; it only requires that SOME
	// result returns on Private carrying our OrderID + BrokerID.
	ownerShardForOrder.punch(liveSession, contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		Instrument: instrumentUnderTest,
		Submit: &contract.FundedOrder{
			OrderID:    orderIDUnderTest,
			Participant: "p-integration",
			Instrument: instrumentUnderTest,
			Side:       contract.Buy,
			Type:       contract.Limit,
			TIF:        contract.GTC,
			Qty:        10,
			LimitPrice: 10000,
		},
	})

	// --- Settle shutdown (closes Private → consumer exits cleanly) --------
	// Give the exchange a moment to run the submit and emit the result
	// before Stop(). The exchange's Stop ordering (close inbound → shards
	// drain → wg.Wait → close public+private) guarantees the result has
	// been sent on Private before Private closes, so the consumer sees it.
	time.Sleep(4 * exchangeTickInterval)
	exchange.Stop()

	select {
	case <-consumerExited:
	case <-time.After(time.Second):
		t.Fatal("settlement consumer did not exit within 1s of exchange.Stop")
	}
	select {
	case <-publicDrainExited:
	case <-time.After(time.Second):
		t.Fatal("public tape drain did not exit within 1s of exchange.Stop")
	}

	// --- Assert routing ---------------------------------------------------
	drained := drainInbox(ownerShardForOrder.inbox)
	if len(drained) != 1 {
		t.Fatalf("owning shard inbox had %d commands, want 1", len(drained))
	}
	applyResult, ok := drained[0].(ApplyResult)
	if !ok {
		t.Fatalf("drained[0] = %T, want ApplyResult", drained[0])
	}
	if applyResult.AccountID != accountIDUnderTest {
		t.Errorf("ApplyResult.AccountID = %q, want %q",
			applyResult.AccountID, accountIDUnderTest)
	}
	if applyResult.Result.OrderID != orderIDUnderTest {
		t.Errorf("ApplyResult.Result.OrderID = %q, want %q",
			applyResult.Result.OrderID, orderIDUnderTest)
	}
	if applyResult.Result.BrokerID != brokerIDUnderTest {
		t.Errorf("ApplyResult.Result.BrokerID = %q, want %q (exchange must echo the id punch stamped)",
			applyResult.Result.BrokerID, brokerIDUnderTest)
	}

	// Non-owning shards received nothing — proves AccountID-hash routing
	// was honored across the real round trip.
	for shardIndex := 0; shardIndex < brokerShardCount; shardIndex++ {
		if broker.shards[shardIndex] == ownerShardForOrder {
			continue
		}
		if remaining := drainInbox(broker.shards[shardIndex].inbox); len(remaining) != 0 {
			t.Errorf("non-owning shard %d inbox had %d commands, want 0: %+v",
				shardIndex, len(remaining), remaining)
		}
	}
}

