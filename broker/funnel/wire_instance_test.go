package funnel

import (
	"fmt"
	"testing"
	"time"

	"simulator/contract"
)

// WireInstance is the thin spawner that attaches one exchange instance's
// private stream to a broker. Everything substantive (routing, foreign-
// broker skip, malformed-OrderID skip, exit-on-close) lives in
// runSettlementConsumer and is covered by settlement_consumer_test.go.
// These tests cover only the WireInstance-specific behaviors:
//
//  1. It is non-blocking (spawns a goroutine and returns).
//  2. The spawned consumer is a real one — a well-formed result sent on
//     its privateStream lands on the owning shard's inbox.
//  3. Multiple WireInstance calls on one broker all drain their own
//     stream independently (one consumer per wired instance).

// well-formed test result that encodes a known AccountID in its OrderID
// — the consumer parses the account out and routes to the owning shard.
func newRoutableResult(brokerID contract.BrokerID, accountID AccountID, seq int) contract.EngineResult {
	return contract.EngineResult{
		BrokerID: brokerID,
		OrderID:  OrderID(fmt.Sprintf("%s:%d", accountID, seq)),
		Outcome:  contract.OutcomeMatched,
	}
}

func TestWireInstance_SpawnsConsumerThatRoutesResults(t *testing.T) {
	const (
		brokerID         contract.BrokerID = "B-wire-under-test"
		shardCount                         = 4
		inboxDepth                         = 16
		ownedAccountID   AccountID         = "owner-account"
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	privateStream := make(chan contract.EngineResult)
	broker.WireInstance("exchange-A", privateStream)

	// Send one well-formed result; the spawned consumer should route it
	// onto the owning shard's inbox.
	expectedResult := newRoutableResult(brokerID, ownedAccountID, 1)
	privateStream <- expectedResult
	close(privateStream)

	// With the stream closed, give the consumer a brief window to drain
	// and exit before we inspect inboxes. Deadline is generous — the
	// consumer does one route call per result.
	time.Sleep(50 * time.Millisecond)

	owningShardIndex := hashAccount(ownedAccountID, shardCount)
	routed := drainInbox(broker.shards[owningShardIndex].inbox)
	if len(routed) != 1 {
		t.Fatalf("expected exactly one routed command on owning shard %d, got %d", owningShardIndex, len(routed))
	}
	applyCmd, ok := routed[0].(ApplyResult)
	if !ok {
		t.Fatalf("routed command on owning shard = %T, want ApplyResult", routed[0])
	}
	if applyCmd.AccountID != ownedAccountID {
		t.Errorf("ApplyResult.AccountID = %q, want %q", applyCmd.AccountID, ownedAccountID)
	}
	if applyCmd.Result.OrderID != expectedResult.OrderID {
		t.Errorf("ApplyResult.Result.OrderID = %q, want %q", applyCmd.Result.OrderID, expectedResult.OrderID)
	}

	// Every other shard inbox must be empty — the owning shard is the
	// only valid routing target for a single accountID.
	for shardIndex, shard := range broker.shards {
		if shardIndex == owningShardIndex {
			continue
		}
		if drained := drainInbox(shard.inbox); len(drained) != 0 {
			t.Errorf("shard %d received %d commands; wanted 0", shardIndex, len(drained))
		}
	}
}

func TestWireInstance_SpawnIsNonBlocking(t *testing.T) {
	const (
		brokerID   contract.BrokerID = "B-nonblock"
		shardCount                   = 2
		inboxDepth                   = 1
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	privateStream := make(chan contract.EngineResult)
	// WireInstance must return without blocking on anything (no send on
	// the stream, no handshake with the consumer). Measure directly.
	callDone := make(chan struct{})
	go func() {
		broker.WireInstance("exchange-B", privateStream)
		close(callDone)
	}()
	select {
	case <-callDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("WireInstance did not return within 100ms (expected immediate return after spawning the goroutine)")
	}

	// Keep the stream open-and-empty for the lifetime of the test so the
	// consumer goroutine does not spin or exit on us; test cleanup closes
	// it to let the goroutine drain cleanly.
	t.Cleanup(func() { close(privateStream) })
}

func TestWireInstance_MultipleInstancesDrainIndependently(t *testing.T) {
	const (
		brokerID            contract.BrokerID = "B-multi-wire"
		shardCount                            = 4
		inboxDepth                            = 16
		firstAccountID      AccountID         = "acct-first"
		secondAccountID     AccountID         = "acct-second"
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	firstStream := make(chan contract.EngineResult)
	secondStream := make(chan contract.EngineResult)

	broker.WireInstance("exchange-first", firstStream)
	broker.WireInstance("exchange-second", secondStream)

	// Send one routable result on each stream. The two consumers must
	// drain their own streams concurrently; neither can block the other.
	firstStream <- newRoutableResult(brokerID, firstAccountID, 1)
	secondStream <- newRoutableResult(brokerID, secondAccountID, 1)

	close(firstStream)
	close(secondStream)

	time.Sleep(50 * time.Millisecond)

	firstShardIndex := hashAccount(firstAccountID, shardCount)
	secondShardIndex := hashAccount(secondAccountID, shardCount)

	firstRouted := drainInbox(broker.shards[firstShardIndex].inbox)
	if len(firstRouted) == 0 {
		t.Errorf("first-exchange result did not reach owning shard %d", firstShardIndex)
	}
	// If the two accounts happen to collide on the same shard we expect
	// two commands there and none on the other; otherwise one on each.
	if firstShardIndex == secondShardIndex {
		if len(firstRouted) != 2 {
			t.Errorf("colliding shard %d received %d commands; wanted 2", firstShardIndex, len(firstRouted))
		}
	} else {
		if len(firstRouted) != 1 {
			t.Errorf("owning shard %d received %d commands; wanted 1", firstShardIndex, len(firstRouted))
		}
		secondRouted := drainInbox(broker.shards[secondShardIndex].inbox)
		if len(secondRouted) != 1 {
			t.Errorf("owning shard %d received %d commands; wanted 1", secondShardIndex, len(secondRouted))
		}
	}
}
