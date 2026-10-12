package funnel

import (
	"log/slog"

	"simulator/contract"
)

// WireInstance attaches this broker to one exchange instance's private
// result stream by spawning the settlement consumer that drains it. One
// call per exchange instance, invoked right before the exchange is
// started so the consumer is already parked when the first result lands
// on the stream.
//
// Called from the market orchestrator (the conductor), not from inside
// the broker. The settlement consumer body is runSettlementConsumer in
// settlement_consumer.go — self-terminating when the exchange closes
// privateStream in its Stop() ordering, so there is no cancellation
// signal threaded through here.
//
// exchangeID is for logging and telemetry only; routing is driven by the
// AccountID encoded on each result's OrderID, exactly as the consumer
// has always done. Each wired instance gets its own goroutine, running
// in parallel with every other wired instance's consumer.
//
// Non-blocking: returns as soon as the consumer goroutine is spawned.
// The orchestrator may call WireInstance → exchange.Start → next step
// without yielding.
func (broker *Broker) WireInstance(exchangeID string, privateStream <-chan contract.EngineResult) {
	slog.Info("funnel: WireInstance spawning settlement consumer",
		"broker_id", broker.brokerID,
		"exchange_id", exchangeID,
	)
	go broker.runSettlementConsumer(privateStream)
}
