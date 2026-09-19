// Package match is the continuous-matching core (EXCHANGE_12): it takes one
// admitted order, decides every fill/rest/cancel by calling the book's
// mutators, and returns one MatchResult. It is the sole writer of last price;
// price and slippage are emergent from which resting orders were hit.
package match

import (
	"simulator/contract"
	"simulator/exchange/book"
)

// CircuitClamp is the reject/clamp seam owned by 1.5. The matcher calls
// PriceAllowed before printing at each level in the walk. Stubbed in-package
// (always allow) so match builds and tests standalone.
type CircuitClamp interface {
	PriceAllowed(price contract.Ticks) bool
}

// SeqSource is the per-instrument event sequence owned by 1.9. The matcher
// stamps every Trade.Seq from here. Stubbed in-package (monotonic counter) so
// match builds and tests standalone.
type SeqSource interface {
	Next() contract.Seq
}

// Matcher decides fills for one instrument's book. Single-writer by
// construction: it holds no locks and shares no state across shards.
type Matcher struct {
	book  *book.Book
	clamp CircuitClamp
	seq   SeqSource
}

func NewMatcher(orderBook *book.Book, clamp CircuitClamp, sequence SeqSource) *Matcher {
	if orderBook == nil {
		panic("match.NewMatcher: nil book")
	}
	if clamp == nil {
		panic("match.NewMatcher: nil clamp")
	}
	if sequence == nil {
		panic("match.NewMatcher: nil seq")
	}
	return &Matcher{book: orderBook, clamp: clamp, seq: sequence}
}

// Match runs the continuous-matching walk for one admitted order. brokerID
// and admissionSeq are carried in by the shard (from the InboundMessage
// envelope and the admission gate, respectively); they are read only when a
// GTC-limit remainder needs to be turned into a RestingOrder.
func (matcher *Matcher) Match(
	order *contract.FundedOrder,
	brokerID contract.BrokerID,
	admissionSeq contract.Seq,
) contract.MatchResult {
	if order == nil {
		panic("match.Match: nil order")
	}
	trades, remaining, walkReason := matcher.runWalk(order)
	return matcher.assembleResult(order, brokerID, admissionSeq, trades, remaining, walkReason)
}

// assembleResult finalises the MatchResult from the walk's outputs: writes
// last price when a trade printed, then disposes of any leftover quantity per
// EXCHANGE_12 §7.
func (matcher *Matcher) assembleResult(
	order *contract.FundedOrder,
	brokerID contract.BrokerID,
	admissionSeq contract.Seq,
	trades []contract.Trade,
	remaining contract.Qty,
	walkReason contract.CancelReason,
) contract.MatchResult {
	result := contract.MatchResult{Trades: trades}

	if len(trades) > 0 {
		finalPrice := trades[len(trades)-1].Price
		matcher.book.SetLastPrice(finalPrice)
		result.LastPrice = finalPrice
		result.LastPriceMoved = true
	}

	if remaining == 0 {
		return result
	}

	// EXCHANGE_12 §7: self-match cancellation of the incoming order never
	// rests, even for GTC-limit.
	if walkReason == contract.ReasonSelfMatch {
		result.CancelledQty = remaining
		result.CancelReason = walkReason
		return result
	}

	// EXCHANGE_12 §7 + EXCHANGE_15 §4: GTC-limit rests its remainder at its
	// own LimitPrice, including when the walk was clamp-halted.
	if order.Type == contract.Limit && order.TIF == contract.GTC {
		resting := &contract.RestingOrder{
			OrderID:     order.OrderID,
			Participant: order.Participant,
			BrokerID:    brokerID,
			Side:        order.Side,
			Price:       order.LimitPrice,
			Remaining:   remaining,
			Seq:         admissionSeq,
		}
		matcher.book.Insert(resting)
		result.Rested = resting
		return result
	}

	// Market / IOC never rest. Fill in a default reason when the walk didn't
	// set one: for a market order that means the opposite side emptied
	// (ReasonNoLiquidity); for an IOC-limit it means the walk exhausted the
	// cross-priced liquidity (ReasonIOCLeftover). EXCHANGE_12 §7.
	cancelReason := walkReason
	if cancelReason == contract.ReasonNone {
		if order.Type == contract.Market {
			cancelReason = contract.ReasonNoLiquidity
		} else {
			cancelReason = contract.ReasonIOCLeftover
		}
	}
	result.CancelledQty = remaining
	result.CancelReason = cancelReason
	return result
}
