package gate

import "simulator/contract"

// checkStructural is the pipeline's shape check: the order must carry sane
// values in its own fields. No collaborator, no external state. A Limit must
// carry a positive price, a Market must not carry one. Side and Type must
// name a known enum value. Stops are not a valid Type — they live on the
// broker; a triggered stop reaches the exchange as an ordinary Market or
// Limit (EXCHANGE_00 §1).
func checkStructural(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	if order.Qty <= 0 {
		return reject(order, brokerID, contract.RejectMalformed, "structural")
	}
	if order.Type != contract.Limit && order.Type != contract.Market {
		return reject(order, brokerID, contract.RejectMalformed, "structural")
	}
	if order.Type == contract.Limit && order.LimitPrice <= 0 {
		return reject(order, brokerID, contract.RejectMalformed, "structural")
	}
	if order.Type == contract.Market && order.LimitPrice != 0 {
		return reject(order, brokerID, contract.RejectMalformed, "structural")
	}
	if order.Side != contract.Buy && order.Side != contract.Sell {
		return reject(order, brokerID, contract.RejectMalformed, "structural")
	}
	return cont()
}
