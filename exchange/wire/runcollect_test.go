package wire

import (
	"sync"
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/emit"
)

// runAndCollect starts every shard's run() and ingress.run(), sends the
// given inbound messages, then orchestrates a clean shutdown and returns
// everything that landed on the public and private streams (in the exact
// order the emitter wrote them, per shard-serial guarantees).
//
// Shutdown chain — each step is a happens-before barrier for the next:
//  1. close(exchange.inbound)  → ingress's range loop exits.
//  2. ingress exit             → ingress closes each shard inbound.
//  3. shard.run() drains its inbound and exits.
//  4. All shards exited        → no more emitter writes possible.
//  5. close(exchange.public)   + close(exchange.private) — safe now.
//  6. collectors drain and exit; their sync.WaitGroup.Wait returns.
//
// The waits between steps are what make it safe to snapshot the collector
// slices at the end (see docs in ingress_test.go's shardCollector.drain).
func runAndCollect(
	t *testing.T,
	exchange *Exchange,
	messages []contract.InboundMessage,
) (publicEvents []emit.TapeEvent, privateResults []contract.EngineResult) {
	t.Helper()

	var collectorsWG sync.WaitGroup
	collectorsWG.Add(2)
	go func() {
		defer collectorsWG.Done()
		for event := range exchange.Public {
			publicEvents = append(publicEvents, event)
		}
	}()
	go func() {
		defer collectorsWG.Done()
		for result := range exchange.Private {
			privateResults = append(privateResults, result)
		}
	}()

	var shardsWG sync.WaitGroup
	for _, shard := range exchange.shards {
		shard := shard
		shardsWG.Add(1)
		go func() {
			defer shardsWG.Done()
			shard.run()
		}()
	}

	ingressDone := make(chan struct{})
	go func() {
		defer close(ingressDone)
		exchange.ingress.run()
	}()

	for _, message := range messages {
		exchange.Inbound <- message
	}

	close(exchange.inbound)
	if !waitClosed(ingressDone, time.Second) {
		t.Fatal("runAndCollect: ingress did not exit within 1s")
	}
	if !waitGroup(&shardsWG, time.Second) {
		t.Fatal("runAndCollect: shards did not exit within 1s")
	}
	close(exchange.public)
	close(exchange.private)
	if !waitGroup(&collectorsWG, time.Second) {
		t.Fatal("runAndCollect: collectors did not exit within 1s")
	}
	return publicEvents, privateResults
}

func waitClosed(done <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func waitGroup(waitGroup *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(done)
	}()
	return waitClosed(done, timeout)
}

// inboundSubmit wraps a fully-populated FundedOrder in an InboundMessage.
// Used by handle_submit tests where the order fields (Side, Type, Qty,
// LimitPrice, MaxSpend, Participant) matter.
func inboundSubmit(brokerID contract.BrokerID, order *contract.FundedOrder) contract.InboundMessage {
	return contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   brokerID,
		Instrument: order.Instrument,
		Submit:     order,
	}
}

// inboundCancel wraps a CancelRequest in an InboundMessage. Instrument is
// carried on the envelope because ingress routes on it; cancel needs no
// price, no funding — the OrderID alone identifies the resting order.
func inboundCancel(brokerID contract.BrokerID, instrument contract.InstrumentID, orderID contract.OrderID) contract.InboundMessage {
	return contract.InboundMessage{
		Kind:       contract.CancelOrder,
		BrokerID:   brokerID,
		Instrument: instrument,
		Cancel:     &contract.CancelRequest{OrderID: orderID},
	}
}
