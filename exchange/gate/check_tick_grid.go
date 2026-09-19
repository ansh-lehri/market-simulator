package gate

import "simulator/contract"

// checkTickGrid enforces that a limit price sits on the instrument's
// tradeable tick grid. Market orders have no resting price to check and
// pass through. EXCHANGE_14 §6.4: the check is a real modulus — Ticks is a
// base integer unit and TickSize is the grid multiple.
func checkTickGrid(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	if order.Type == contract.Market {
		return cont()
	}
	if order.LimitPrice%ctx.rules.TickSize != 0 {
		return reject(order, brokerID, contract.RejectBadTick, "tick_grid")
	}
	return cont()
}
