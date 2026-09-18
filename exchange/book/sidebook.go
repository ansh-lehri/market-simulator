package book

import "simulator/contract"

// Direction picks which end of the price axis is "best" for this side.
// Ascending — best = lowest price (ask side; matcher wants the cheapest to sell).
// Descending — best = highest price (bid side; matcher wants the richest to buy).
type Direction int

const (
	Ascending Direction = iota
	Descending
)

// SideBook is one side of the order book: price levels kept sorted so that the
// best price is instantly available and matching can walk best→worst without
// caring what data structure sits underneath. The concrete choice is a
// benchmark decision — see EXCHANGE_11 §5.3.
//
// Iterate's visit callback returns true to continue and false to stop early.
type SideBook interface {
	Best() (*PriceLevel, bool)
	LevelAt(price contract.Ticks) (*PriceLevel, bool)
	Insert(price contract.Ticks) *PriceLevel
	RemoveLevel(price contract.Ticks)
	Iterate(visit func(*PriceLevel) bool)
}

// newSideBook returns the default concrete SideBook (a red-black tree, per
// EXCHANGE_11 §5.3). Callers depend on the interface; swapping the default is a
// one-line change at the construction site.
func newSideBook(direction Direction) SideBook {
	return newRBTreeSide(direction)
}
