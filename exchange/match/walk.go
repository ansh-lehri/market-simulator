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

// isMarketBuy reports whether the funded-cap rule applies to this order.
// Only Market BUY orders carry a MaxSpend reservation; Sell markets and any
// limit are exempt. EXCHANGE_12 §6 (e)/(h).
func isMarketBuy(order *contract.FundedOrder) bool {
	return order.Type == contract.Market && order.Side == contract.Buy
}

// runWalk drives the continuous-matching loop for one taker: on each
// iteration it reads the opposite side's best level, applies self-match
// prevention against the level's front, prints one trade at the maker's
// price, and asks the book to execute the fill. For Market BUY it also
// enforces the funded-cap: fillQty is capped by (MaxSpend-spent)/price
// (integer floor) at step (e), and the walk halts post-fill once spent has
// reached MaxSpend at step (h). Returns fills, unfilled remainder, and any
// reason set during the walk. EXCHANGE_12 §6.
func (matcher *Matcher) runWalk(order *contract.FundedOrder) (
	trades []contract.Trade,
	remaining contract.Qty,
	walkReason contract.CancelReason,
) {
	remaining = order.Qty
	walkReason = contract.ReasonNone
	opposite := oppositeSide(order.Side)
	spent := contract.Ticks(0)

walkLoop:
	for remaining > 0 {
		bestLevel, hasLevel := matcher.book.Best(opposite)
		if !hasLevel {
			break
		}
		if !priceAcceptable(bestLevel.Price, order) {
			break
		}

		frontOrder := bestLevel.Orders.Front().Value.(*contract.RestingOrder)

		// (c) self-match prevention. A self-match never prints — the STP
		// mode picks which side to cancel, or both. EXCHANGE_12 §6 (c),
		// §12 inv 4. GTC-limit takers cancelled here do NOT rest (§7).
		if frontOrder.Participant == order.Participant {
			switch order.STP {
			case contract.CancelIncoming:
				walkReason = contract.ReasonSelfMatch
				break walkLoop
			case contract.CancelResting:
				matcher.book.Cancel(frontOrder.OrderID)
				continue
			case contract.CancelBoth:
				matcher.book.Cancel(frontOrder.OrderID)
				walkReason = contract.ReasonSelfMatch
				break walkLoop
			}
		}

		fillQty := remaining
		if frontOrder.Remaining < fillQty {
			fillQty = frontOrder.Remaining
		}

		if isMarketBuy(order) {
			// (e) integer-floor affordability. Any residual cash below the
			// price of one unit at this level is unspendable — correct
			// behaviour, not a bug. EXCHANGE_12 §6 (e), §8.3.
			affordableQty := (order.MaxSpend - spent) / bestLevel.Price
			if affordableQty < fillQty {
				fillQty = affordableQty
			}
			if fillQty <= 0 {
				walkReason = contract.ReasonFundedCap
				break
			}
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
		spent += bestLevel.Price * fillQty

		if isMarketBuy(order) && spent >= order.MaxSpend && remaining > 0 {
			// (h) reservation exhausted after this fill; unfilled remainder
			// cancels with FundedCap. EXCHANGE_12 §6 (h).
			walkReason = contract.ReasonFundedCap
			break
		}
	}
	return trades, remaining, walkReason
}
