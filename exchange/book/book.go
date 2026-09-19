package book

import (
	"container/list"
	"fmt"

	"simulator/contract"
)

// orderLocation is the index entry — the exact (side, level, node) triple that
// lets Cancel remove a resting order in O(1). Level pointer holds the queue;
// node is the doubly-linked list element inside level.Orders.
type orderLocation struct {
	side  contract.Side
	level *PriceLevel
	node  *list.Element
}

// Book is the exchange's core state for one instrument: two sorted sides, an
// index for O(1) cancel-by-id, and the derived QuoteState. See EXCHANGE_11 §5.
//
// Single-writer by construction; no mutexes. Every mutator keeps the quote
// consistent as a side effect so no caller has to remember to refresh it.
type Book struct {
	Instrument contract.InstrumentID
	asks       SideBook
	bids       SideBook
	index      map[contract.OrderID]*orderLocation
	quote      QuoteState
}

func NewBook(instrument contract.InstrumentID) *Book {
	return &Book{
		Instrument: instrument,
		asks:       newSideBook(Ascending),
		bids:       newSideBook(Descending),
		index:      make(map[contract.OrderID]*orderLocation),
	}
}

// Insert rests a maker order in the book. Precondition failures panic —
// admission is expected to have validated. See EXCHANGE_11 §6.
func (book *Book) Insert(order *contract.RestingOrder) {
	if order == nil {
		panic("book.Insert: nil order")
	}
	if order.OrderID == "" {
		panic("book.Insert: empty OrderID")
	}
	if order.Remaining <= 0 {
		panic(fmt.Sprintf("book.Insert: Remaining must be > 0, got %d", order.Remaining))
	}
	if order.Price <= 0 {
		panic(fmt.Sprintf("book.Insert: Price must be > 0, got %d", order.Price))
	}
	if _, exists := book.index[order.OrderID]; exists {
		panic(fmt.Sprintf("book.Insert: duplicate OrderID %q", order.OrderID))
	}

	side := book.sideBook(order.Side)
	level := side.Insert(order.Price)
	node := level.appendBack(order)
	book.index[order.OrderID] = &orderLocation{
		side:  order.Side,
		level: level,
		node:  node,
	}
	book.refreshSideQuote(order.Side)
}

// Cancel removes a resting order by id. Not-found is a normal outcome (the
// order may have already filled or been cancelled) and returns (nil, false) —
// see EXCHANGE_11 §6. Otherwise returns the removed order so the broker can
// release its leftover reservation.
func (book *Book) Cancel(id contract.OrderID) (*contract.RestingOrder, bool) {
	location, ok := book.index[id]
	if !ok {
		return nil, false
	}
	removed := location.level.removeNode(location.node)
	delete(book.index, id)
	if location.level.Orders.Len() == 0 {
		book.sideBook(location.side).RemoveLevel(location.level.Price)
	}
	book.refreshSideQuote(location.side)
	return removed, true
}

// ReduceFront applies a fill of `by` units against the front order of `level`.
// Called by the matching core as it walks a level. Precondition failures panic
// (matcher contract). See EXCHANGE_11 §6.
func (book *Book) ReduceFront(level *PriceLevel, by contract.Qty) {
	if level == nil {
		panic("book.ReduceFront: nil level")
	}
	if by <= 0 {
		panic(fmt.Sprintf("book.ReduceFront: by must be > 0, got %d", by))
	}
	frontNode := level.Orders.Front()
	if frontNode == nil {
		panic("book.ReduceFront: level is empty")
	}
	frontOrder := frontNode.Value.(*contract.RestingOrder)
	if by > frontOrder.Remaining {
		panic(fmt.Sprintf("book.ReduceFront: by=%d exceeds front.Remaining=%d for order %q",
			by, frontOrder.Remaining, frontOrder.OrderID))
	}

	side := frontOrder.Side
	if by == frontOrder.Remaining {
		removed := level.removeNode(frontNode)
		delete(book.index, removed.OrderID)
		if level.Orders.Len() == 0 {
			book.sideBook(side).RemoveLevel(level.Price)
		}
	} else {
		frontOrder.Remaining -= by
		level.TotalQty -= by
	}
	book.refreshSideQuote(side)
}

// Quote returns the current derived state by value. Callers cannot hold a
// pointer into the book's live quote — see EXCHANGE_11 §8.
func (book *Book) Quote() QuoteState {
	return book.quote
}

// Best returns the top price level of the given side (highest bid for Buy,
// lowest ask for Sell), or nil/false when that side is empty. It is the
// matcher's read-hook for the walk: the returned *PriceLevel is the same
// pointer that must be handed back to ReduceFront to execute a fill against
// it. See EXCHANGE_11 §5.3 and §6.
func (book *Book) Best(side contract.Side) (*PriceLevel, bool) {
	return book.sideBook(side).Best()
}

// SetLastPrice is the matcher's write hook for last price. It is the ONLY path
// that mutates LastPrice/HasLastPrice; no book mutator touches them. See
// EXCHANGE_11 §7.2 and the plan discussion. Price 0 is a bug (a trade never
// prints at 0) and panics.
func (book *Book) SetLastPrice(price contract.Ticks) {
	if price <= 0 {
		panic(fmt.Sprintf("book.SetLastPrice: price must be > 0, got %d", price))
	}
	book.quote.LastPrice = price
	book.quote.HasLastPrice = true
}

func (book *Book) sideBook(side contract.Side) SideBook {
	if side == contract.Buy {
		return book.bids
	}
	return book.asks
}
