package gate

import "simulator/contract"

// checkLiquidity rejects a market order that would face an empty opposite
// book — there is nothing to trade against and the order does not rest
// (Market never rests). Only market orders need this check; a limit against
// an empty book simply rests. Auction-routed orders skip it entirely: the
// auction clears in a batch and needs no live opposite book at admission
// (EXCHANGE_14 §6.3, decision).
func checkLiquidity(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	if ctx.route == routeAuction {
		return cont()
	}
	if order.Type != contract.Market {
		return cont()
	}
	quote := ctx.book.Quote()
	if order.Side == contract.Buy && !quote.HasAsk {
		return reject(order, brokerID, contract.RejectNoLiquidity, "liquidity")
	}
	if order.Side == contract.Sell && !quote.HasBid {
		return reject(order, brokerID, contract.RejectNoLiquidity, "liquidity")
	}
	return cont()
}
