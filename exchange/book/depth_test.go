package book

import (
	"reflect"
	"testing"

	"simulator/contract"
)

func TestTopN_NonPositiveN_Panics(t *testing.T) {
	book := NewBook("ACME")
	for _, badN := range []int{0, -1, -10} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic on n = %d", badN)
				}
			}()
			book.TopN(contract.Buy, badN)
		}()
	}
}

func TestTopN_EmptySide_ReturnsEmpty(t *testing.T) {
	book := NewBook("ACME")

	if got := book.TopN(contract.Buy, 10); len(got) != 0 {
		t.Fatalf("empty buy side: got %+v, want empty", got)
	}
	if got := book.TopN(contract.Sell, 10); len(got) != 0 {
		t.Fatalf("empty sell side: got %+v, want empty", got)
	}
}

// Bid side is best-first = highest price first (descending).
func TestTopN_Buy_BestFirstIsHighestPrice(t *testing.T) {
	book := NewBook("ACME")
	// Insert in arbitrary order — TopN must impose the sort.
	book.Insert(makeResting("b1", contract.Buy, 100, 5))
	book.Insert(makeResting("b2", contract.Buy, 105, 3))
	book.Insert(makeResting("b3", contract.Buy, 102, 7))
	book.Insert(makeResting("b4", contract.Buy, 108, 2))
	book.Insert(makeResting("b5", contract.Buy, 101, 4))

	got := book.TopN(contract.Buy, 3)
	want := []LevelSnapshot{
		{Price: 108, TotalQty: 2},
		{Price: 105, TotalQty: 3},
		{Price: 102, TotalQty: 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Buy TopN(3)\n got: %+v\nwant: %+v", got, want)
	}
}

// Ask side is best-first = lowest price first (ascending).
func TestTopN_Sell_BestFirstIsLowestPrice(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("s1", contract.Sell, 100, 5))
	book.Insert(makeResting("s2", contract.Sell, 105, 3))
	book.Insert(makeResting("s3", contract.Sell, 102, 7))
	book.Insert(makeResting("s4", contract.Sell, 108, 2))
	book.Insert(makeResting("s5", contract.Sell, 101, 4))

	got := book.TopN(contract.Sell, 3)
	want := []LevelSnapshot{
		{Price: 100, TotalQty: 5},
		{Price: 101, TotalQty: 4},
		{Price: 102, TotalQty: 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Sell TopN(3)\n got: %+v\nwant: %+v", got, want)
	}
}

// Fewer than N levels → all returned; slice length equals the number of
// distinct price levels, not n.
func TestTopN_FewerLevelsThanN_ReturnsAll(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("s1", contract.Sell, 100, 5))
	book.Insert(makeResting("s2", contract.Sell, 101, 4))
	book.Insert(makeResting("s3", contract.Sell, 102, 3))

	got := book.TopN(contract.Sell, 10)
	want := []LevelSnapshot{
		{Price: 100, TotalQty: 5},
		{Price: 101, TotalQty: 4},
		{Price: 102, TotalQty: 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Sell TopN(10) with 3 levels\n got: %+v\nwant: %+v", got, want)
	}
}

// Exactly N levels → all returned.
func TestTopN_ExactlyN_ReturnsAll(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("s1", contract.Sell, 100, 5))
	book.Insert(makeResting("s2", contract.Sell, 101, 4))
	book.Insert(makeResting("s3", contract.Sell, 102, 3))

	got := book.TopN(contract.Sell, 3)
	if len(got) != 3 {
		t.Fatalf("TopN(3) with 3 levels: len %d, want 3", len(got))
	}
}

// N = 1 collapses to the best price and stops (early-stop via Iterate's
// visit return-false).
func TestTopN_NEqualsOne_ReturnsBestOnly(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("b1", contract.Buy, 100, 5))
	book.Insert(makeResting("b2", contract.Buy, 108, 3))
	book.Insert(makeResting("b3", contract.Buy, 105, 7))

	got := book.TopN(contract.Buy, 1)
	want := []LevelSnapshot{{Price: 108, TotalQty: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Buy TopN(1)\n got: %+v\nwant: %+v", got, want)
	}
}

// Multiple resting orders at the same price collapse to one LevelSnapshot
// whose TotalQty equals Σ Remaining — this is the book's invariant 2 read
// unchanged.
func TestTopN_AggregatesTotalQtyPerLevel(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("s1", contract.Sell, 100, 5))
	book.Insert(makeResting("s2", contract.Sell, 100, 7))
	book.Insert(makeResting("s3", contract.Sell, 100, 3))
	book.Insert(makeResting("s4", contract.Sell, 101, 4))

	got := book.TopN(contract.Sell, 5)
	want := []LevelSnapshot{
		{Price: 100, TotalQty: 15},
		{Price: 101, TotalQty: 4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Sell TopN(5) with same-price orders\n got: %+v\nwant: %+v", got, want)
	}
}

// After Cancel removes the sole order at a level, that level disappears from
// TopN (level 100 emptied → next best sell is 101).
func TestTopN_LevelEmptiedByCancel_Disappears(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("s1", contract.Sell, 100, 5))
	book.Insert(makeResting("s2", contract.Sell, 101, 4))
	book.Insert(makeResting("s3", contract.Sell, 102, 3))

	if _, ok := book.Cancel("s1"); !ok {
		t.Fatal("Cancel(s1) failed")
	}

	got := book.TopN(contract.Sell, 5)
	want := []LevelSnapshot{
		{Price: 101, TotalQty: 4},
		{Price: 102, TotalQty: 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Sell TopN after Cancel\n got: %+v\nwant: %+v", got, want)
	}
}

// After ReduceFront takes part of the front order's quantity, the level
// remains and TotalQty reflects the reduction.
func TestTopN_LevelReducedByReduceFront_ReflectsRemaining(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("s1", contract.Sell, 100, 10))
	book.Insert(makeResting("s2", contract.Sell, 101, 4))

	best, ok := book.Best(contract.Sell)
	if !ok {
		t.Fatal("Best(Sell) empty")
	}
	book.ReduceFront(best, 3)

	got := book.TopN(contract.Sell, 5)
	want := []LevelSnapshot{
		{Price: 100, TotalQty: 7},
		{Price: 101, TotalQty: 4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Sell TopN after ReduceFront\n got: %+v\nwant: %+v", got, want)
	}
}

// Cross-side isolation: TopN on one side never surfaces the other side's
// levels, even when prices would otherwise collide numerically.
func TestTopN_DoesNotLeakAcrossSides(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("b1", contract.Buy, 100, 5))
	book.Insert(makeResting("s1", contract.Sell, 100, 7))
	book.Insert(makeResting("s2", contract.Sell, 105, 3))

	buys := book.TopN(contract.Buy, 5)
	wantBuys := []LevelSnapshot{{Price: 100, TotalQty: 5}}
	if !reflect.DeepEqual(buys, wantBuys) {
		t.Fatalf("Buy TopN leaked\n got: %+v\nwant: %+v", buys, wantBuys)
	}
	sells := book.TopN(contract.Sell, 5)
	wantSells := []LevelSnapshot{
		{Price: 100, TotalQty: 7},
		{Price: 105, TotalQty: 3},
	}
	if !reflect.DeepEqual(sells, wantSells) {
		t.Fatalf("Sell TopN leaked\n got: %+v\nwant: %+v", sells, wantSells)
	}
}
