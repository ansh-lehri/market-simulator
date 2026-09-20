package wire

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"simulator/contract"
)

// shardCollector drains every shard's inbound channel into a per-shard slice
// of received messages. Needed because unbuffered channels + no shard loop
// means the test goroutine cannot send-then-read on the same goroutine
// without deadlocking. One collector goroutine per shard keeps the receive
// side always ready.
type shardCollector struct {
	received  [][]contract.InboundMessage
	waitGroup sync.WaitGroup
	drainOnce sync.Once
}

// collectShards spawns one drain goroutine per shard. Each goroutine reads
// from shards[i].inbound until it closes and appends every message into
// received[i]. Call drain(t) AFTER ingress has stopped to close each shard
// inbound and wait for all collector goroutines to finish — that wait is the
// happens-before barrier that makes snapshot() safe.
func collectShards(t *testing.T, exchange *Exchange) *shardCollector {
	t.Helper()
	collector := &shardCollector{
		received: make([][]contract.InboundMessage, len(exchange.shards)),
	}
	for shardIndex, shard := range exchange.shards {
		shardIndex, shard := shardIndex, shard
		collector.waitGroup.Add(1)
		go func() {
			defer collector.waitGroup.Done()
			for msg := range shard.inbound {
				collector.received[shardIndex] = append(collector.received[shardIndex], msg)
			}
		}()
	}
	t.Cleanup(func() { collector.drain(t) })
	return collector
}

// drain waits for all collector goroutines to exit — which happens after
// ingress closes each shard inbound during its shutdown (Stage 3). Must be
// called AFTER stopIngress so the close-of-shard-inbounds has actually
// happened; otherwise the range loops are still parked and this blocks
// until the 1s timeout. The waitGroup.Wait is a happens-before barrier:
// any append the collector performed is visible on return.
func (collector *shardCollector) drain(t *testing.T) {
	t.Helper()
	collector.drainOnce.Do(func() {
		done := make(chan struct{})
		go func() {
			collector.waitGroup.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shard collectors did not exit within 1s (was ingress stopped first?)")
		}
	})
}

// snapshot returns a copy of the per-shard received slices. Only safe after
// drain(t) has returned.
func (collector *shardCollector) snapshot() [][]contract.InboundMessage {
	copied := make([][]contract.InboundMessage, len(collector.received))
	for i := range collector.received {
		copied[i] = append([]contract.InboundMessage(nil), collector.received[i]...)
	}
	return copied
}

// startIngress spawns ingress.run() in a goroutine and returns a stop
// function that closes the edge inbound channel once (sync.Once-guarded so
// tests and t.Cleanup can both call it) and waits for run() to return with
// a timeout.
func startIngress(t *testing.T, exchange *Exchange) func() {
	t.Helper()
	complete := make(chan struct{})
	go func() {
		exchange.ingress.run()
		close(complete)
	}()
	var closeOnce sync.Once
	stop := func() {
		closeOnce.Do(func() { close(exchange.inbound) })
		select {
		case <-complete:
		case <-time.After(time.Second):
			t.Fatal("ingress.run did not return within 1s of inbound close")
		}
	}
	t.Cleanup(stop)
	return stop
}

// makeExchange builds an exchange with numShards and one InstrumentSpec per
// id, all sharing the two-boundary schedule from exchange_test.go.
func makeExchange(t *testing.T, numShards int, ids ...contract.InstrumentID) *Exchange {
	t.Helper()
	specs := make([]InstrumentSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, makeSpec(id))
	}
	return NewExchange(numShards, makeSchedule(), specs)
}

// submit builds an InboundMessage carrying a minimal FundedOrder. Content is
// irrelevant to ingress (it routes on Instrument alone); only Kind and
// Instrument matter.
func submit(instrument contract.InstrumentID, orderID contract.OrderID, brokerID contract.BrokerID) contract.InboundMessage {
	return contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   brokerID,
		Instrument: instrument,
		Submit:     &contract.FundedOrder{OrderID: orderID, Instrument: instrument},
	}
}

func cancel(instrument contract.InstrumentID, orderID contract.OrderID, brokerID contract.BrokerID) contract.InboundMessage {
	return contract.InboundMessage{
		Kind:       contract.CancelOrder,
		BrokerID:   brokerID,
		Instrument: instrument,
		Cancel:     &contract.CancelRequest{OrderID: orderID},
	}
}

// TestIngress_RoutesByHash — every SubmitOrder lands on the shard identified
// by hashInstrument(instrument) % numShards, and on no other shard.
func TestIngress_RoutesByHash(t *testing.T) {
	ids := []contract.InstrumentID{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
		"K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
		"U", "V", "W", "X", "Y", "Z",
	}
	const numShards = 4
	exchange := makeExchange(t, numShards, ids...)
	collector := collectShards(t, exchange)
	stop := startIngress(t, exchange)

	for _, id := range ids {
		exchange.Inbound <- submit(id, "o-"+contract.OrderID(id), "B1")
	}
	stop()
	collector.drain(t)
	snapshot := collector.snapshot()

	for _, id := range ids {
		expectedShard := int(hashInstrument(id) % uint32(numShards))
		if !containsInstrument(snapshot[expectedShard], id) {
			t.Fatalf("instrument %q missing from shard %d (its hashed home)", id, expectedShard)
		}
		for other := 0; other < numShards; other++ {
			if other == expectedShard {
				continue
			}
			if containsInstrument(snapshot[other], id) {
				t.Fatalf("instrument %q appeared on shard %d (expected only on %d)", id, other, expectedShard)
			}
		}
	}
}

// TestIngress_SubmitAndCancelRouteByInstrumentOnly — Kind is not consulted;
// both variants for the same instrument arrive on the same shard.
func TestIngress_SubmitAndCancelRouteByInstrumentOnly(t *testing.T) {
	const numShards = 3
	exchange := makeExchange(t, numShards, "ACME")
	collector := collectShards(t, exchange)
	stop := startIngress(t, exchange)

	exchange.Inbound <- submit("ACME", "o-1", "B1")
	exchange.Inbound <- cancel("ACME", "o-1", "B1")
	stop()
	collector.drain(t)

	expectedShard := int(hashInstrument("ACME") % uint32(numShards))
	snapshot := collector.snapshot()
	if len(snapshot[expectedShard]) != 2 {
		t.Fatalf("shard %d received %d messages, want 2", expectedShard, len(snapshot[expectedShard]))
	}
	if snapshot[expectedShard][0].Kind != contract.SubmitOrder {
		t.Fatalf("first message on shard %d Kind = %v, want SubmitOrder", expectedShard, snapshot[expectedShard][0].Kind)
	}
	if snapshot[expectedShard][1].Kind != contract.CancelOrder {
		t.Fatalf("second message on shard %d Kind = %v, want CancelOrder", expectedShard, snapshot[expectedShard][1].Kind)
	}
	// Neither variant leaked to another shard.
	for other := 0; other < numShards; other++ {
		if other == expectedShard {
			continue
		}
		if len(snapshot[other]) != 0 {
			t.Fatalf("shard %d received %d messages, want 0 (all ACME traffic goes to shard %d)", other, len(snapshot[other]), expectedShard)
		}
	}
}

// TestIngress_MessageOrderPreservedPerInstrument — same-instrument messages
// arrive on their shard in send order. Ingress + unbuffered channels
// guarantee this; the test pins it.
func TestIngress_MessageOrderPreservedPerInstrument(t *testing.T) {
	const numShards = 4
	exchange := makeExchange(t, numShards, "ACME")
	collector := collectShards(t, exchange)
	stop := startIngress(t, exchange)

	sent := []contract.InboundMessage{
		submit("ACME", "o-1", "B1"),
		submit("ACME", "o-2", "B1"),
		cancel("ACME", "o-1", "B1"),
		submit("ACME", "o-3", "B1"),
		cancel("ACME", "o-2", "B1"),
	}
	for _, msg := range sent {
		exchange.Inbound <- msg
	}
	stop()
	collector.drain(t)

	shardID := int(hashInstrument("ACME") % uint32(numShards))
	got := collector.snapshot()[shardID]
	if !reflect.DeepEqual(got, sent) {
		t.Fatalf("order mismatch on shard %d\n  got:  %v\n  want: %v", shardID, got, sent)
	}
}

// TestIngress_RoutingDeterministic — the same input sequence on two fresh
// exchanges produces identical per-shard receive sequences.
func TestIngress_RoutingDeterministic(t *testing.T) {
	const numShards = 4
	ids := []contract.InstrumentID{"A", "B", "C", "D", "E", "F", "G", "H"}
	messages := []contract.InboundMessage{
		submit("A", "o-1", "B1"), submit("B", "o-2", "B1"), submit("C", "o-3", "B1"),
		submit("D", "o-4", "B1"), submit("E", "o-5", "B1"), submit("F", "o-6", "B1"),
		submit("G", "o-7", "B1"), submit("H", "o-8", "B1"), cancel("A", "o-1", "B1"),
		cancel("D", "o-4", "B1"), submit("B", "o-9", "B1"), submit("A", "o-10", "B1"),
		cancel("H", "o-8", "B1"), submit("C", "o-11", "B1"), submit("E", "o-12", "B1"),
		submit("F", "o-13", "B1"), submit("G", "o-14", "B1"), submit("B", "o-15", "B1"),
		cancel("B", "o-2", "B1"), submit("H", "o-16", "B1"),
	}

	run := func() [][]contract.InboundMessage {
		exchange := makeExchange(t, numShards, ids...)
		collector := collectShards(t, exchange)
		stop := startIngress(t, exchange)
		for _, msg := range messages {
			exchange.Inbound <- msg
		}
		stop()
		collector.drain(t)
		return collector.snapshot()
	}

	first := run()
	second := run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("routing not deterministic\n  first:  %v\n  second: %v", first, second)
	}
}

// TestIngress_MultiInstrumentInterleaved — per-instrument order preserved
// even when a shard receives traffic from multiple instruments interleaved
// with traffic for instruments on other shards.
func TestIngress_MultiInstrumentInterleaved(t *testing.T) {
	const numShards = 4
	ids := []contract.InstrumentID{"A", "B", "C"}
	exchange := makeExchange(t, numShards, ids...)
	collector := collectShards(t, exchange)
	stop := startIngress(t, exchange)

	sent := []contract.InboundMessage{
		submit("A", "o-1", "B1"),
		submit("B", "o-2", "B1"),
		submit("A", "o-3", "B1"),
		submit("C", "o-4", "B1"),
		submit("B", "o-5", "B1"),
		submit("A", "o-6", "B1"),
		submit("C", "o-7", "B1"),
	}
	for _, msg := range sent {
		exchange.Inbound <- msg
	}
	stop()
	collector.drain(t)

	snapshot := collector.snapshot()
	for _, id := range ids {
		expectedIDs := filterInstrumentOrderIDs(sent, id)
		shardID := int(hashInstrument(id) % uint32(numShards))
		gotIDs := extractOrderIDsForInstrument(snapshot[shardID], id)
		if !reflect.DeepEqual(gotIDs, expectedIDs) {
			t.Fatalf("per-instrument order for %q on shard %d\n  got:  %v\n  want: %v", id, shardID, gotIDs, expectedIDs)
		}
	}
}

// containsInstrument reports whether msgs contains at least one message with
// the given Instrument.
func containsInstrument(msgs []contract.InboundMessage, instrument contract.InstrumentID) bool {
	for _, msg := range msgs {
		if msg.Instrument == instrument {
			return true
		}
	}
	return false
}

// filterInstrumentOrderIDs returns the OrderIDs of messages whose Instrument
// matches, in send order. Reads OrderID from Submit or Cancel per Kind.
func filterInstrumentOrderIDs(msgs []contract.InboundMessage, instrument contract.InstrumentID) []contract.OrderID {
	out := []contract.OrderID{}
	for _, msg := range msgs {
		if msg.Instrument != instrument {
			continue
		}
		out = append(out, orderIDOf(msg))
	}
	return out
}

// extractOrderIDsForInstrument is the same as filterInstrumentOrderIDs but
// operates on a shard's received slice — separated for readability at the
// call sites.
func extractOrderIDsForInstrument(msgs []contract.InboundMessage, instrument contract.InstrumentID) []contract.OrderID {
	return filterInstrumentOrderIDs(msgs, instrument)
}

func orderIDOf(msg contract.InboundMessage) contract.OrderID {
	if msg.Kind == contract.SubmitOrder {
		return msg.Submit.OrderID
	}
	return msg.Cancel.OrderID
}

// TestIngress_ShutdownPropagatesToShardInbounds — closing the edge inbound
// makes ingress close every shard inbound in turn, so shard.run() loops can
// drain and exit (Stage 3 shutdown chain). Send a message, close inbound,
// and assert every shards[i].inbound is closed within a short timeout.
func TestIngress_ShutdownPropagatesToShardInbounds(t *testing.T) {
	const numShards = 3
	exchange := makeExchange(t, numShards, "A", "B", "C")

	// Drain each shard's inbound in a goroutine. The range loop exits when
	// the channel closes; we signal completion on a per-shard done channel.
	shardDone := make([]chan struct{}, numShards)
	for shardIndex, shard := range exchange.shards {
		shardIndex, shard := shardIndex, shard
		shardDone[shardIndex] = make(chan struct{})
		go func() {
			defer close(shardDone[shardIndex])
			for range shard.inbound {
			}
		}()
	}

	ingressDone := make(chan struct{})
	go func() {
		defer close(ingressDone)
		exchange.ingress.run()
	}()

	// One in-flight message just to confirm ingress is actively running.
	exchange.Inbound <- submit("A", "o-1", "B1")

	close(exchange.inbound)

	select {
	case <-ingressDone:
	case <-time.After(time.Second):
		t.Fatal("ingress did not exit within 1s of edge inbound close")
	}
	for shardIndex, done := range shardDone {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("shard %d inbound was not closed by ingress within 1s", shardIndex)
		}
	}
}
