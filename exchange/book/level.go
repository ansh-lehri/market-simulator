package book

import (
	"container/list"

	"simulator/contract"
)

// PriceLevel is the FIFO queue of resting orders at a single price.
// Orders.Front() is the oldest (fills first); PushBack appends the newest.
// TotalQty is the cached Σ Remaining over Orders — see EXCHANGE_11 §4.4.
type PriceLevel struct {
	Price    contract.Ticks
	Orders   *list.List
	TotalQty contract.Qty
}

func newLevel(price contract.Ticks) *PriceLevel {
	return &PriceLevel{
		Price:  price,
		Orders: list.New(),
	}
}

func (level *PriceLevel) appendBack(order *contract.RestingOrder) *list.Element {
	level.TotalQty += order.Remaining
	return level.Orders.PushBack(order)
}

func (level *PriceLevel) popFront() *contract.RestingOrder {
	front := level.Orders.Front()
	if front == nil {
		return nil
	}
	return level.removeNode(front)
}

func (level *PriceLevel) removeNode(node *list.Element) *contract.RestingOrder {
	order := node.Value.(*contract.RestingOrder)
	level.Orders.Remove(node)
	level.TotalQty -= order.Remaining
	return order
}
