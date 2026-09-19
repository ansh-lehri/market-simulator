package match

import (
	"simulator/contract"
)

// oppositeSide returns which side of the book the taker crosses into.
func oppositeSide(takerSide contract.Side) contract.Side {
	if takerSide == contract.Buy {
		return contract.Sell
	}
	return contract.Buy
}

// priceAcceptable reports whether a maker at makerPrice still crosses the
// taker's limit. Market takers always accept. EXCHANGE_12 §6.
func priceAcceptable(makerPrice contract.Ticks, order *contract.FundedOrder) bool {
	if order.Type == contract.Market {
		return true
	}
	if order.Side == contract.Buy {
		return makerPrice <= order.LimitPrice
	}
	return makerPrice >= order.LimitPrice
}

// runWalk drives the continuous-matching loop for one taker: on each
// iteration it reads the opposite side's best level, prints one trade at the
// maker's price, and asks the book to execute the fill. Returns the fills,
// the unfilled remainder, and any reason set during the walk. EXCHANGE_12 §6.
func (matcher *Matcher) runWalk(order *contract.FundedOrder) (
	trades []contract.Trade,
	remaining contract.Qty,
	walkReason contract.CancelReason,
) {
	remaining = order.Qty
	walkReason = contract.ReasonNone
	opposite := oppositeSide(order.Side)

	for remaining > 0 {
		bestLevel, hasLevel := matcher.book.Best(opposite)
		if !hasLevel {
			break
		}
		if !priceAcceptable(bestLevel.Price, order) {
			break
		}

		frontOrder := bestLevel.Orders.Front().Value.(*contract.RestingOrder)

		fillQty := remaining
		if frontOrder.Remaining < fillQty {
			fillQty = frontOrder.Remaining
		}

		trades = append(trades, contract.Trade{
			Seq:              matcher.seq.Next(),
			Instrument:       order.Instrument,
			Price:            bestLevel.Price,
			Qty:              fillQty,
			TakerOrder:       order.OrderID,
			MakerOrder:       frontOrder.OrderID,
			TakerSide:        order.Side,
			TakerParticipant: order.Participant,
			MakerParticipant: frontOrder.Participant,
		})

		matcher.book.ReduceFront(bestLevel, fillQty)
		remaining -= fillQty
	}
	return trades, remaining, walkReason
}
