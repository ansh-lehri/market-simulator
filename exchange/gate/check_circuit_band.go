package gate

import "simulator/contract"

// checkCircuitBand rejects a limit order whose price sits outside the
// instrument's frozen band [lower, upper] (inclusive). This is the
// reject-half of the circuit rule; the clamp-half runs during matching
// (EXCHANGE_15 §3). Market orders have no price to reject on — they are
// governed by the clamp at walk time.
//
// The bounds come from the same CircuitBand object the matcher consults
// via PriceAllowed, so admit-side and clamp-side agree by construction
// (EXCHANGE_15 §6 invariant 1).
func checkCircuitBand(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	if order.Type == contract.Market {
		return cont()
	}
	lower, upper := ctx.band.Bounds()
	if order.LimitPrice < lower || order.LimitPrice > upper {
		return reject(order, brokerID, contract.RejectBeyondCircuit, "circuit_band")
	}
	return cont()
}
