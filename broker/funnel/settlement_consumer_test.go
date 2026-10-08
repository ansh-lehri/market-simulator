package funnel

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/capstone"
)

// newConsumerHarness builds a Broker with shardCount nil-bundle shards
// and inbox depth large enough to absorb every ApplyResult the test
// routes. Shards are NOT started — the test inspects each shard's inbox
// directly after the consumer exits, which only works because no run
// goroutine is pulling commands off.
//
// Observation model: the consumer sends on broker.shards[idx].inbox via
// broker.route; we close privateStream, wait on the consumer's exit
// signal, then drain each inbox non-blockingly. Any routed command is
// still queued because no shard.run() is draining it.
func newConsumerHarness(t *testing.T, brokerID contract.BrokerID, shardCount int, inboxDepth int) *Broker {
	t.Helper()
	bundles := make([]*capstone.Bundle, shardCount)
	// Nil bundles → shards without resv/realiz. Legal for consumer tests
	// (we never dispatch; the handlers that would call reserve/realize
	// never run), per NewBroker's doc comment.
	return NewBroker(brokerID, inboxDepth, bundles, nil, MoneyConfig{}, nil)
}

// drainInbox pops every queued Command off a shard inbox without
// blocking. Safe only when the test has closed the consumer's upstream
// and no run goroutine is competing for the inbox.
func drainInbox(shardInbox chan Command) []Command {
	drained := make([]Command, 0, len(shardInbox))
	for {
		select {
		case cmd := <-shardInbox:
			drained = append(drained, cmd)
		default:
			return drained
		}
	}
}

// runConsumerToClose spawns the consumer, feeds every result, closes the
// stream, and waits for the consumer goroutine to exit. Returns once the
// consumer has drained — safe to inspect shard inboxes after.
func runConsumerToClose(t *testing.T, broker *Broker, results []contract.EngineResult) {
	t.Helper()
	privateStream := make(chan contract.EngineResult)
	consumerExited := make(chan struct{})
	go func() {
		defer close(consumerExited)
		broker.runSettlementConsumer(privateStream)
	}()

	for _, result := range results {
		privateStream <- result
	}
	close(privateStream)

	select {
	case <-consumerExited:
	case <-time.After(time.Second):
		t.Fatal("settlement consumer did not exit within 1s of Private close")
	}
}

// TestSettlementConsumer_RoutesToOwningShardByParsedAccountID — the
// load-bearing invariant: a result whose OrderID encodes AccountID "X"
// MUST land on broker.shards[hashAccount("X", N)] as an ApplyResult
// wrapping the original EngineResult verbatim (BROKER_PHASE4 §2.3,
// P4-D5/D6).
func TestSettlementConsumer_RoutesToOwningShardByParsedAccountID(t *testing.T) {
	const (
		brokerID    contract.BrokerID = "B-under-test"
		shardCount                    = 4
		inboxDepth                    = 32
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	// Three different accounts so routing hits more than one shard
	// (which shards specifically is hash-determined; the test asserts
	// against hashAccount rather than hard-coding indices).
	results := []contract.EngineResult{
		{Outcome: contract.OutcomeMatched, OrderID: "alice:1", BrokerID: brokerID},
		{Outcome: contract.OutcomeRejected, OrderID: "bob:2", BrokerID: brokerID,
			Rejection: &contract.Rejection{Reason: contract.RejectMalformed, Stage: "structural"}},
		{Outcome: contract.OutcomeCancelled, OrderID: "charlie:3", BrokerID: brokerID, CancelledQty: 50},
	}

	runConsumerToClose(t, broker, results)

	for _, originalResult := range results {
		parsedAccountID, parsed := accountOf(originalResult.OrderID)
		if !parsed {
			t.Fatalf("test setup bug: accountOf(%q) failed", originalResult.OrderID)
		}
		expectedShardIndex := hashAccount(parsedAccountID, shardCount)
		owningInbox := broker.shards[expectedShardIndex].inbox

		cmd := <-owningInbox // non-blocking in practice — the consumer already routed and exited
		applyResult, ok := cmd.(ApplyResult)
		if !ok {
			t.Fatalf("expected ApplyResult on shard %d inbox, got %T", expectedShardIndex, cmd)
		}
		if applyResult.AccountID != parsedAccountID {
			t.Errorf("ApplyResult.AccountID = %q, want %q (parsed from OrderID %q)",
				applyResult.AccountID, parsedAccountID, originalResult.OrderID)
		}
		if applyResult.Result != originalResult {
			t.Errorf("ApplyResult.Result = %+v, want %+v (verbatim forward)",
				applyResult.Result, originalResult)
		}
	}

	// Any shard that was NOT the owning shard for any of the 3 accounts
	// must be empty — proves nothing was mis-routed.
	owningShards := map[int]bool{}
	for _, originalResult := range results {
		parsedAccountID, _ := accountOf(originalResult.OrderID)
		owningShards[hashAccount(parsedAccountID, shardCount)] = true
	}
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		if owningShards[shardIndex] {
			continue
		}
		remaining := drainInbox(broker.shards[shardIndex].inbox)
		if len(remaining) != 0 {
			t.Errorf("shard %d inbox had %d unexpected commands: %+v", shardIndex, len(remaining), remaining)
		}
	}
}

// TestSettlementConsumer_SkipsForeignBrokerID — the cross-broker guard
// (BROKER_PHASE4 §2.3 orientation addendum). A result echoing a
// different broker's id must not be routed onto any of our shards; a
// following well-formed result for us must still land. Proves the
// drain-continues-after-skip property.
func TestSettlementConsumer_SkipsForeignBrokerID(t *testing.T) {
	const (
		ourBrokerID     contract.BrokerID = "B-ours"
		foreignBrokerID contract.BrokerID = "B-foreign"
		shardCount                        = 2
		inboxDepth                        = 8
	)
	broker := newConsumerHarness(t, ourBrokerID, shardCount, inboxDepth)

	results := []contract.EngineResult{
		{Outcome: contract.OutcomeMatched, OrderID: "foreign-acct:1", BrokerID: foreignBrokerID},
		{Outcome: contract.OutcomeMatched, OrderID: "ours-acct:1", BrokerID: ourBrokerID},
	}

	runConsumerToClose(t, broker, results)

	totalRouted := 0
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		totalRouted += len(drainInbox(broker.shards[shardIndex].inbox))
	}
	if totalRouted != 1 {
		t.Fatalf("routed command count = %d, want 1 (foreign-BrokerID result must be skipped)", totalRouted)
	}
}

// TestSettlementConsumer_SkipsMalformedOrderIDAndDrainsOn — P4-D7:
// unroutable OrderID is logged-and-skipped (loud error log, no panic),
// and the consumer continues draining so subsequent well-formed results
// still land.
func TestSettlementConsumer_SkipsMalformedOrderIDAndDrainsOn(t *testing.T) {
	const (
		brokerID   contract.BrokerID = "B-under-test"
		shardCount                   = 2
		inboxDepth                   = 8
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	// Each malformed shape exercised by accountOf's table test is a
	// potential break the consumer must swallow; we interleave them with
	// a valid result to prove drain-continues.
	results := []contract.EngineResult{
		{Outcome: contract.OutcomeMatched, OrderID: "no-colon-at-all", BrokerID: brokerID},
		{Outcome: contract.OutcomeRejected, OrderID: ":42", BrokerID: brokerID},
		{Outcome: contract.OutcomeCancelled, OrderID: "trailing-colon:", BrokerID: brokerID},
		{Outcome: contract.OutcomeMatched, OrderID: "valid-acct:1", BrokerID: brokerID},
	}

	runConsumerToClose(t, broker, results)

	totalRouted := 0
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		totalRouted += len(drainInbox(broker.shards[shardIndex].inbox))
	}
	if totalRouted != 1 {
		t.Fatalf("routed command count = %d, want 1 (3 malformed must skip, 1 valid must land)", totalRouted)
	}
}

// TestSettlementConsumer_IsOutcomeBlind — dual-branch cover per the
// memory rule. The consumer must forward every Outcome variant
// identically; it is pure transport (P4-D8). We exercise all six
// outcomes against one account; all six must land, in input order.
func TestSettlementConsumer_IsOutcomeBlind(t *testing.T) {
	const (
		brokerID   contract.BrokerID = "B-under-test"
		shardCount                   = 1 // single shard → all six land in one inbox in input order
		inboxDepth                   = 16
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	results := []contract.EngineResult{
		{Outcome: contract.OutcomeMatched, OrderID: "acct:1", BrokerID: brokerID,
			Match: &contract.MatchResult{}},
		{Outcome: contract.OutcomeDiverted, OrderID: "acct:2", BrokerID: brokerID},
		{Outcome: contract.OutcomeRejected, OrderID: "acct:3", BrokerID: brokerID,
			Rejection: &contract.Rejection{Reason: contract.RejectBadTick, Stage: "tick_grid"}},
		{Outcome: contract.OutcomeCancelled, OrderID: "acct:4", BrokerID: brokerID, CancelledQty: 10},
		{Outcome: contract.OutcomeCancelRejected, OrderID: "acct:5", BrokerID: brokerID},
		{Outcome: contract.OutcomeMakerFill, OrderID: "acct:6", BrokerID: brokerID,
			Match: &contract.MatchResult{}},
	}

	runConsumerToClose(t, broker, results)

	drained := drainInbox(broker.shards[0].inbox)
	if len(drained) != len(results) {
		t.Fatalf("routed count = %d, want %d (consumer must forward every outcome)",
			len(drained), len(results))
	}
	for index, cmd := range drained {
		applyResult, ok := cmd.(ApplyResult)
		if !ok {
			t.Fatalf("drained[%d] = %T, want ApplyResult", index, cmd)
		}
		if applyResult.Result.Outcome != results[index].Outcome {
			t.Errorf("drained[%d].Outcome = %v, want %v",
				index, applyResult.Result.Outcome, results[index].Outcome)
		}
	}
}

// TestSettlementConsumer_PreservesPerAccountOrder — many results for one
// account must land on its shard in exactly arrival order. Proves the
// per-account-order guarantee P4-D8 states ("single drain + funnel →
// serial on the one shard").
func TestSettlementConsumer_PreservesPerAccountOrder(t *testing.T) {
	const (
		brokerID    contract.BrokerID = "B-under-test"
		shardCount                    = 4
		inboxDepth                    = 256
		resultCount                   = 100
	)
	broker := newConsumerHarness(t, brokerID, shardCount, inboxDepth)

	results := make([]contract.EngineResult, resultCount)
	for index := 0; index < resultCount; index++ {
		// Encode the sequence in CancelledQty so order is observable
		// without peeking at OrderID's colon-encoding.
		results[index] = contract.EngineResult{
			Outcome:      contract.OutcomeCancelled,
			OrderID:      "ordered-acct:" + itoa(index),
			BrokerID:     brokerID,
			CancelledQty: int64(index),
		}
	}

	runConsumerToClose(t, broker, results)

	owningShardIndex := hashAccount("ordered-acct", shardCount)
	drained := drainInbox(broker.shards[owningShardIndex].inbox)
	if len(drained) != resultCount {
		t.Fatalf("drained count on owning shard = %d, want %d", len(drained), resultCount)
	}
	for index, cmd := range drained {
		applyResult := cmd.(ApplyResult)
		if applyResult.Result.CancelledQty != int64(index) {
			t.Fatalf("drained[%d].CancelledQty = %d, want %d (per-account order broken)",
				index, applyResult.Result.CancelledQty, index)
		}
	}
}

// TestSettlementConsumer_ExitsOnPrivateClose — the lifecycle guarantee:
// closing Private causes the consumer goroutine to return cleanly, no
// stop signal needed (BROKER_PHASE4 §2.5). Covered implicitly by every
// test above (runConsumerToClose waits on the exit signal), but called
// out as its own test so the contract is explicit and a regression in
// the exit path is diagnosed directly rather than via a 1-second timeout
// in a routing test.
func TestSettlementConsumer_ExitsOnPrivateClose(t *testing.T) {
	broker := newConsumerHarness(t, "B-under-test", 1, 1)

	privateStream := make(chan contract.EngineResult)
	consumerExited := make(chan struct{})
	go func() {
		defer close(consumerExited)
		broker.runSettlementConsumer(privateStream)
	}()

	close(privateStream)

	select {
	case <-consumerExited:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("consumer did not exit within 500ms of Private close")
	}
}

// itoa is a tiny allocation-free int → base10 string for the ordering
// test's OrderID construction. The test package avoids strconv in a
// one-off helper to keep the test file self-contained.
func itoa(number int) string {
	if number == 0 {
		return "0"
	}
	digits := make([]byte, 0, 20)
	negative := number < 0
	if negative {
		number = -number
	}
	for number > 0 {
		digits = append(digits, byte('0'+number%10))
		number /= 10
	}
	if negative {
		digits = append(digits, '-')
	}
	for left, right := 0, len(digits)-1; left < right; left, right = left+1, right-1 {
		digits[left], digits[right] = digits[right], digits[left]
	}
	return string(digits)
}
