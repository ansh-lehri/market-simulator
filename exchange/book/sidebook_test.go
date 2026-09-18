package book

import (
	"testing"

	"simulator/contract"
)

// collectIterationOrder returns the prices visited by Iterate, in order.
func collectIterationOrder(side SideBook) []contract.Ticks {
	var visited []contract.Ticks
	side.Iterate(func(level *PriceLevel) bool {
		visited = append(visited, level.Price)
		return true
	})
	return visited
}

func TestSideBook_EmptyBest(t *testing.T) {
	for _, direction := range []Direction{Ascending, Descending} {
		side := newSideBook(direction)
		best, ok := side.Best()
		if ok {
			t.Fatalf("direction=%v: Best on empty side must return (_, false); got price=%d", direction, best.Price)
		}
		if best != nil {
			t.Fatalf("direction=%v: Best on empty side must return nil PriceLevel", direction)
		}
	}
}

func TestSideBook_EmptyLevelAt(t *testing.T) {
	side := newSideBook(Ascending)
	if level, ok := side.LevelAt(10021); ok || level != nil {
		t.Fatalf("LevelAt on empty side: got (%v, %v), want (nil, false)", level, ok)
	}
}

func TestSideBook_EmptyIterate(t *testing.T) {
	side := newSideBook(Ascending)
	visits := 0
	side.Iterate(func(*PriceLevel) bool {
		visits++
		return true
	})
	if visits != 0 {
		t.Fatalf("Iterate on empty side visited %d levels, want 0", visits)
	}
}

func TestSideBook_EmptyRemoveIsNoOp(t *testing.T) {
	side := newSideBook(Ascending)
	side.RemoveLevel(10021) // must not panic on empty tree
	if _, ok := side.Best(); ok {
		t.Fatal("side must remain empty after RemoveLevel on empty")
	}
}

func TestSideBook_InsertIdempotent(t *testing.T) {
	side := newSideBook(Ascending)
	first := side.Insert(10021)
	second := side.Insert(10021)
	if first != second {
		t.Fatal("Insert called twice at the same price must return the same *PriceLevel (get-or-create)")
	}
	if first.Price != 10021 {
		t.Fatalf("returned level has Price=%d, want 10021", first.Price)
	}
	// Confirm mutation on the returned level persists through re-fetch.
	first.appendBack(makeOrder("X", 50))
	again, ok := side.LevelAt(10021)
	if !ok || again != first {
		t.Fatal("LevelAt must return the same pointer inserted earlier")
	}
	if again.TotalQty != 50 {
		t.Fatalf("state on returned level did not persist: TotalQty=%d, want 50", again.TotalQty)
	}
}

func TestSideBook_RemoveThenReinsert_ReturnsFreshLevel(t *testing.T) {
	// Once removed, the level pointer must NOT be resurrected — a subsequent
	// Insert at the same price returns a brand-new PriceLevel (state clean).
	side := newSideBook(Ascending)
	original := side.Insert(10021)
	original.appendBack(makeOrder("X", 50))
	side.RemoveLevel(10021)
	fresh := side.Insert(10021)
	if fresh == original {
		t.Fatal("Insert after RemoveLevel must return a NEW level, not the removed one")
	}
	if fresh.TotalQty != 0 || fresh.Orders.Len() != 0 {
		t.Fatalf("re-inserted level not empty: TotalQty=%d Len=%d", fresh.TotalQty, fresh.Orders.Len())
	}
}

func TestSideBook_BestReflectsDirection(t *testing.T) {
	cases := []struct {
		direction Direction
		wantBest  contract.Ticks
	}{
		{Ascending, 10018},
		{Descending, 10022},
	}
	prices := []contract.Ticks{10021, 10018, 10022, 10020, 10019}
	for _, tc := range cases {
		side := newSideBook(tc.direction)
		for _, price := range prices {
			side.Insert(price)
		}
		best, ok := side.Best()
		if !ok {
			t.Fatalf("direction=%v: Best must be present", tc.direction)
		}
		if best.Price != tc.wantBest {
			t.Fatalf("direction=%v: Best.Price=%d, want %d", tc.direction, best.Price, tc.wantBest)
		}
	}
}

func TestSideBook_IterateWalksBestToWorst(t *testing.T) {
	prices := []contract.Ticks{10021, 10018, 10022, 10020, 10019}
	cases := []struct {
		direction Direction
		want      []contract.Ticks
	}{
		{Ascending, []contract.Ticks{10018, 10019, 10020, 10021, 10022}},
		{Descending, []contract.Ticks{10022, 10021, 10020, 10019, 10018}},
	}
	for _, tc := range cases {
		side := newSideBook(tc.direction)
		for _, price := range prices {
			side.Insert(price)
		}
		got := collectIterationOrder(side)
		if !equalTicks(got, tc.want) {
			t.Fatalf("direction=%v: Iterate order = %v, want %v", tc.direction, got, tc.want)
		}
	}
}

func TestSideBook_IterateEarlyStop(t *testing.T) {
	side := newSideBook(Ascending)
	for _, price := range []contract.Ticks{10018, 10019, 10020, 10021, 10022} {
		side.Insert(price)
	}
	var visited []contract.Ticks
	side.Iterate(func(level *PriceLevel) bool {
		visited = append(visited, level.Price)
		return level.Price < 10020 // stop after visiting 10020
	})
	want := []contract.Ticks{10018, 10019, 10020}
	if !equalTicks(visited, want) {
		t.Fatalf("early-stop iteration = %v, want %v", visited, want)
	}
}

func TestSideBook_LevelAtHitAndMiss(t *testing.T) {
	side := newSideBook(Ascending)
	side.Insert(10020)
	side.Insert(10022)
	if _, ok := side.LevelAt(10020); !ok {
		t.Fatal("LevelAt(10020) must hit")
	}
	if _, ok := side.LevelAt(10022); !ok {
		t.Fatal("LevelAt(10022) must hit")
	}
	if _, ok := side.LevelAt(10021); ok {
		t.Fatal("LevelAt(10021) must miss (never inserted)")
	}
}

func TestSideBook_RemoveShiftsBest(t *testing.T) {
	side := newSideBook(Ascending)
	side.Insert(10020)
	side.Insert(10021)
	side.Insert(10022)

	best, _ := side.Best()
	if best.Price != 10020 {
		t.Fatalf("Best before remove = %d, want 10020", best.Price)
	}

	side.RemoveLevel(10020)
	best, _ = side.Best()
	if best.Price != 10021 {
		t.Fatalf("Best after removing top = %d, want 10021", best.Price)
	}

	side.RemoveLevel(10021)
	best, _ = side.Best()
	if best.Price != 10022 {
		t.Fatalf("Best after removing next = %d, want 10022", best.Price)
	}

	side.RemoveLevel(10022)
	if _, ok := side.Best(); ok {
		t.Fatal("Best on drained side must return (_, false)")
	}
}

func TestSideBook_RemoveAbsentPreservesShape(t *testing.T) {
	side := newSideBook(Ascending)
	side.Insert(10020)
	side.Insert(10022)
	side.RemoveLevel(10021) // absent — must be silent
	if got := collectIterationOrder(side); !equalTicks(got, []contract.Ticks{10020, 10022}) {
		t.Fatalf("iteration after no-op remove = %v, want [10020 10022]", got)
	}
}

// --- helpers -------------------------------------------------------------

func equalTicks(a, b []contract.Ticks) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}
