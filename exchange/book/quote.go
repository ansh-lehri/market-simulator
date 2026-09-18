package book

import "simulator/contract"

// QuoteState is the derived projection of the book's top of book plus the last
// traded price. See EXCHANGE_11 §7.1. Each of the three quantities carries an
// explicit Has* flag: "no bid" is a distinct state from "best bid is 0", and
// integer ticks make 0 a representable price.
type QuoteState struct {
	BestBid      contract.Ticks
	BidQty       contract.Qty
	HasBid       bool

	BestAsk      contract.Ticks
	AskQty       contract.Qty
	HasAsk       bool

	LastPrice    contract.Ticks
	HasLastPrice bool
}

// refreshBid copies the current top of the bid side into the quote, or clears
// it if the side is empty. Called after any mutation on the bid side.
func (book *Book) refreshBid() {
	if best, ok := book.bids.Best(); ok {
		book.quote.BestBid = best.Price
		book.quote.BidQty = best.TotalQty
		book.quote.HasBid = true
		return
	}
	book.quote.BestBid = 0
	book.quote.BidQty = 0
	book.quote.HasBid = false
}

// refreshAsk copies the current top of the ask side into the quote, or clears
// it if the side is empty. Called after any mutation on the ask side.
func (book *Book) refreshAsk() {
	if best, ok := book.asks.Best(); ok {
		book.quote.BestAsk = best.Price
		book.quote.AskQty = best.TotalQty
		book.quote.HasAsk = true
		return
	}
	book.quote.BestAsk = 0
	book.quote.AskQty = 0
	book.quote.HasAsk = false
}

func (book *Book) refreshSideQuote(side contract.Side) {
	if side == contract.Buy {
		book.refreshBid()
		return
	}
	book.refreshAsk()
}
