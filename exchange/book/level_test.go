package book

import (
	"testing"

	"simulator/contract"
)

func makeOrder(orderID contract.OrderID, remaining contract.Qty) *contract.RestingOrder {
	return &contract.RestingOrder{
		OrderID:   orderID,
		Remaining: remaining,
	}
}

func assertTotalQtyMatches(t *testing.T, level *PriceLevel) {
	t.Helper()
	var sum contract.Qty
	for node := level.Orders.Front(); node != nil; node = node.Next() {
		sum += node.Value.(*contract.RestingOrder).Remaining
	}
	if sum != level.TotalQty {
		t.Fatalf("TotalQty invariant broken: cached=%d walked=%d", level.TotalQty, sum)
	}
}

func TestNewLevel(t *testing.T) {
	level := newLevel(10021)

	if level.Price != 10021 {
		t.Fatalf("Price: got %d want 10021", level.Price)
	}
	if level.Orders == nil {
		t.Fatal("Orders must be initialised, got nil")
	}
	if level.Orders.Len() != 0 {
		t.Fatalf("Orders.Len(): got %d want 0", level.Orders.Len())
	}
	if level.TotalQty != 0 {
		t.Fatalf("TotalQty: got %d want 0", level.TotalQty)
	}
}

func TestAppendBackSingle(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)

	node := level.appendBack(orderA)

	if node == nil {
		t.Fatal("appendBack returned nil element")
	}
	if node.Value.(*contract.RestingOrder) != orderA {
		t.Fatal("returned element does not point at the appended order")
	}
	if level.Orders.Len() != 1 {
		t.Fatalf("Len: got %d want 1", level.Orders.Len())
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderA {
		t.Fatal("Front is not orderA")
	}
	if level.TotalQty != 50 {
		t.Fatalf("TotalQty: got %d want 50", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)
}

func TestAppendBackFIFO(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	orderB := makeOrder("B", 30)
	orderC := makeOrder("C", 20)

	level.appendBack(orderA)
	level.appendBack(orderB)
	level.appendBack(orderC)

	if level.Orders.Len() != 3 {
		t.Fatalf("Len: got %d want 3", level.Orders.Len())
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderA {
		t.Fatal("Front must be the oldest (A)")
	}
	if level.Orders.Back().Value.(*contract.RestingOrder) != orderC {
		t.Fatal("Back must be the newest (C)")
	}
	if level.TotalQty != 100 {
		t.Fatalf("TotalQty: got %d want 100", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)
}

func TestPopFrontOnEmpty(t *testing.T) {
	level := newLevel(10021)

	popped := level.popFront()

	if popped != nil {
		t.Fatalf("popFront on empty: got %v want nil", popped)
	}
	if level.Orders.Len() != 0 {
		t.Fatalf("Len after empty pop: got %d want 0", level.Orders.Len())
	}
	if level.TotalQty != 0 {
		t.Fatalf("TotalQty after empty pop: got %d want 0", level.TotalQty)
	}
}

func TestPopFrontSingle(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	level.appendBack(orderA)

	popped := level.popFront()

	if popped != orderA {
		t.Fatal("popFront must return the front order")
	}
	if level.Orders.Len() != 0 {
		t.Fatalf("Len: got %d want 0", level.Orders.Len())
	}
	if level.TotalQty != 0 {
		t.Fatalf("TotalQty: got %d want 0", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)
}

func TestPopFrontFIFOOrder(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	orderB := makeOrder("B", 30)
	orderC := makeOrder("C", 20)
	level.appendBack(orderA)
	level.appendBack(orderB)
	level.appendBack(orderC)

	if got := level.popFront(); got != orderA {
		t.Fatalf("first pop: got %v want A", got)
	}
	if level.TotalQty != 50 {
		t.Fatalf("TotalQty after pop A: got %d want 50", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)

	if got := level.popFront(); got != orderB {
		t.Fatalf("second pop: got %v want B", got)
	}
	if level.TotalQty != 20 {
		t.Fatalf("TotalQty after pop B: got %d want 20", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)

	if got := level.popFront(); got != orderC {
		t.Fatalf("third pop: got %v want C", got)
	}
	if level.Orders.Len() != 0 {
		t.Fatalf("Len after draining: got %d want 0", level.Orders.Len())
	}
	if level.TotalQty != 0 {
		t.Fatalf("TotalQty after draining: got %d want 0", level.TotalQty)
	}
}

func TestRemoveNodeHead(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	orderB := makeOrder("B", 30)
	orderC := makeOrder("C", 20)
	nodeA := level.appendBack(orderA)
	level.appendBack(orderB)
	level.appendBack(orderC)

	removed := level.removeNode(nodeA)

	if removed != orderA {
		t.Fatal("removeNode(head) must return orderA")
	}
	if level.Orders.Len() != 2 {
		t.Fatalf("Len: got %d want 2", level.Orders.Len())
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderB {
		t.Fatal("new Front must be B")
	}
	if level.TotalQty != 50 {
		t.Fatalf("TotalQty: got %d want 50", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)
}

func TestRemoveNodeMiddle(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	orderB := makeOrder("B", 30)
	orderC := makeOrder("C", 20)
	level.appendBack(orderA)
	nodeB := level.appendBack(orderB)
	level.appendBack(orderC)

	removed := level.removeNode(nodeB)

	if removed != orderB {
		t.Fatal("removeNode(middle) must return orderB")
	}
	if level.Orders.Len() != 2 {
		t.Fatalf("Len: got %d want 2", level.Orders.Len())
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderA {
		t.Fatal("Front must still be A")
	}
	if level.Orders.Back().Value.(*contract.RestingOrder) != orderC {
		t.Fatal("Back must still be C")
	}
	if level.TotalQty != 70 {
		t.Fatalf("TotalQty: got %d want 70", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)
}

func TestRemoveNodeTail(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	orderB := makeOrder("B", 30)
	orderC := makeOrder("C", 20)
	level.appendBack(orderA)
	level.appendBack(orderB)
	nodeC := level.appendBack(orderC)

	removed := level.removeNode(nodeC)

	if removed != orderC {
		t.Fatal("removeNode(tail) must return orderC")
	}
	if level.Orders.Len() != 2 {
		t.Fatalf("Len: got %d want 2", level.Orders.Len())
	}
	if level.Orders.Back().Value.(*contract.RestingOrder) != orderB {
		t.Fatal("new Back must be B")
	}
	if level.TotalQty != 80 {
		t.Fatalf("TotalQty: got %d want 80", level.TotalQty)
	}
	assertTotalQtyMatches(t, level)
}

func TestRemoveNodeLeavingEmpty(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	nodeA := level.appendBack(orderA)

	removed := level.removeNode(nodeA)

	if removed != orderA {
		t.Fatal("removeNode must return orderA")
	}
	if level.Orders.Len() != 0 {
		t.Fatalf("Len: got %d want 0", level.Orders.Len())
	}
	if level.TotalQty != 0 {
		t.Fatalf("TotalQty: got %d want 0", level.TotalQty)
	}
}

func TestTotalQtyInvariantAcrossMixedOps(t *testing.T) {
	level := newLevel(10021)
	orderA := makeOrder("A", 50)
	orderB := makeOrder("B", 30)
	orderC := makeOrder("C", 20)
	orderD := makeOrder("D", 15)

	level.appendBack(orderA)
	assertTotalQtyMatches(t, level)

	nodeB := level.appendBack(orderB)
	assertTotalQtyMatches(t, level)

	level.appendBack(orderC)
	assertTotalQtyMatches(t, level)

	level.removeNode(nodeB)
	assertTotalQtyMatches(t, level)

	level.popFront()
	assertTotalQtyMatches(t, level)

	level.appendBack(orderD)
	assertTotalQtyMatches(t, level)

	level.popFront()
	assertTotalQtyMatches(t, level)

	level.popFront()
	assertTotalQtyMatches(t, level)

	if level.Orders.Len() != 0 {
		t.Fatalf("expected empty level, got Len=%d", level.Orders.Len())
	}
	if level.TotalQty != 0 {
		t.Fatalf("expected TotalQty=0, got %d", level.TotalQty)
	}
}
