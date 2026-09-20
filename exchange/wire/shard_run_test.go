package wire

import (
	"strings"
	"sync"
	"testing"

	"simulator/contract"
	"simulator/exchange/clock"
)

// TestShardRun_PanicsOnUnknownInstrument — invariant: ingress must never
// route a message to a shard that doesn't own the instrument. If it does,
// the shard panics rather than silently swallowing.
func TestShardRun_PanicsOnUnknownInstrument(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	shard := exchange.shards[0]

	panicked := make(chan any, 1)
	var runWG sync.WaitGroup
	runWG.Add(1)
	go func() {
		defer runWG.Done()
		defer func() { panicked <- recover() }()
		shard.run()
	}()

	shard.inbound <- contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   "B1",
		Instrument: "UNKNOWN",
		Submit:     &contract.FundedOrder{OrderID: "o-1", Instrument: "UNKNOWN"},
	}
	// The panic aborts shard.run(); range on inbound never returns to us,
	// but the deferred recover fires. Wait on runWG for the goroutine to
	// exit, then inspect the recovered value.
	runWG.Wait()
	recovered := <-panicked
	if recovered == nil {
		t.Fatalf("shard.run did not panic on unknown instrument")
	}
	message, ok := recovered.(string)
	if !ok || !strings.Contains(message, "does not own") {
		t.Fatalf("panic message = %v, want one containing 'does not own'", recovered)
	}
}

// TestShardRun_PanicsOnCancelKind — Stage 3 scope guard: cancel path is
// explicitly not implemented; a CancelOrder message panics loudly so the
// gap is not silently reached in production.
func TestShardRun_PanicsOnCancelKind(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	shard := exchange.shards[0]

	panicked := make(chan any, 1)
	var runWG sync.WaitGroup
	runWG.Add(1)
	go func() {
		defer runWG.Done()
		defer func() { panicked <- recover() }()
		shard.run()
	}()

	shard.inbound <- contract.InboundMessage{
		Kind:       contract.CancelOrder,
		BrokerID:   "B1",
		Instrument: "ACME",
		Cancel:     &contract.CancelRequest{OrderID: "o-1"},
	}
	runWG.Wait()
	recovered := <-panicked
	if recovered == nil {
		t.Fatalf("shard.run did not panic on CancelOrder")
	}
	message, ok := recovered.(string)
	if !ok || !strings.Contains(message, "Stage 4") {
		t.Fatalf("panic message = %v, want one referring to Stage 4", recovered)
	}
}
