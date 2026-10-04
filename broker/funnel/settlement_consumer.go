package funnel

import (
	"log/slog"

	"simulator/contract"
)

// runSettlementConsumer drains one exchange instance's private result
// stream, resolves the owning AccountID from the echoed OrderID, and
// routes an ApplyResult push-command onto the owning shard. One goroutine
// per wired exchange instance; who spawns it with which instance's Private
// channel is Phase-7 WireInstance's call, not this body (BROKER_PHASE4
// §2.5, §6).
//
// Pure transport — no account state touched, no reply produced. The
// six-outcome mutation (handleApplyResult: Convert / Release /
// ReleaseExcess mapping) is Phase 5, point 3; the consumer only gets the
// result to the right shard and the shard settles it (P4-D5, P4-D8).
//
// Three cases are dropped without routing:
//
//  1. Foreign BrokerID. The exchange's Private channel is a single edge
//     shared across every broker connected to that instance
//     (exchange/example/main.go demonstrates the multi-broker shape),
//     and the exchange does not filter on our behalf. Our deployment is
//     one broker per process so the foreign case is zero today, but the
//     guard is cheap and surfaces a cross-broker leak loudly without
//     masking it. Debug-level — not an error, just noise.
//
//  2. Malformed OrderID. accountOf returns parsed=false (empty, missing
//     colon, leading or trailing colon). A broker-mint invariant
//     violation; killing the consumer would halt settlement for the
//     whole instance (far worse than skipping one unroutable result)
//     and there is no account to route the result to anyway. Error-log
//     so a real breakage is loud (P4-D7).
//
//  3. Nothing else. Every well-formed, this-broker result routes
//     verbatim — the consumer is outcome-blind by design (P4-D8). All
//     six Outcome values are the Phase-5 handler's problem.
//
// Lifecycle: self-terminating. The for-range exits when the exchange
// closes privateStream in Stop() (exchange/wire/exchange.go:205). No
// stop signal, no explicit gate. Tracking the goroutine's exit (a
// WaitGroup so broker shutdown can wait for settlement to drain after
// Private closes) is a Phase-7 lifecycle/infra concern — not in this
// body (BROKER_PHASE4 §6).
//
// Per-account order is preserved for free: a single consumer drains
// Private in arrival order and routes synchronously, so a single
// account's results all land on its one shard's inbox in that order and
// the shard settles them serially.
//
// Back-pressure: broker.route sends onto a buffered shard inbox; a
// wedged shard back-pressures this drain, which in turn back-pressures
// the exchange shard emitting the result (exchange Private is unbuffered,
// exchange/wire/exchange.go:64). Same discipline as the punch — shards
// drain fast, acceptable, not engineered around (BROKER_PHASE4 §2.5).
func (broker *Broker) runSettlementConsumer(privateStream <-chan contract.EngineResult) {
	slog.Info("funnel: settlement consumer started",
		"broker_id", broker.brokerID,
	)
	for result := range privateStream {
		if result.BrokerID != broker.brokerID {
			slog.Debug("funnel: settlement foreign BrokerID — skipped",
				"foreign_broker_id", result.BrokerID,
				"our_broker_id", broker.brokerID,
				"order_id", result.OrderID,
				"outcome", result.Outcome,
			)
			continue
		}
		accountID, parsed := accountOf(result.OrderID)
		if !parsed {
			slog.Error("funnel: settlement unroutable OrderID — skipped",
				"broker_id", broker.brokerID,
				"order_id", result.OrderID,
				"outcome", result.Outcome,
			)
			continue
		}
		slog.Debug("funnel: settlement routing ApplyResult",
			"broker_id", broker.brokerID,
			"account_id", accountID,
			"order_id", result.OrderID,
			"outcome", result.Outcome,
		)
		broker.route(ApplyResult{AccountID: accountID, Result: result})
	}
	slog.Info("funnel: settlement consumer exited (Private closed)",
		"broker_id", broker.brokerID,
	)
}
