package book

import (
	"testing"

	"simulator/contract"
)

// --- helpers -------------------------------------------------------------

func makeResting(id contract.OrderID, side contract.Side, price contract.Ticks, remaining contract.Qty) *contract.RestingOrder {
	return &contract.RestingOrder{
		OrderID:   id,
		Side:      side,
		Price:     price,
		Remaining: remaining,
	}
}

func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic: %s", what)
		}
	}()
	fn()
}

func assertQuote(
	t *testing.T,
	quote QuoteState,
	hasBid bool, bestBid contract.Ticks, bidQty contract.Qty,
	hasAsk bool, bestAsk contract.Ticks, askQty contract.Qty,
) {
	t.Helper()
	if quote.HasBid != hasBid || quote.BestBid != bestBid || quote.BidQty != bidQty {
		t.Fatalf("bid mismatch: got (has=%v price=%d qty=%d), want (has=%v price=%d qty=%d)",
			quote.HasBid, quote.BestBid, quote.BidQty, hasBid, bestBid, bidQty)
	}
	if quote.HasAsk != hasAsk || quote.BestAsk != bestAsk || quote.AskQty != askQty {
		t.Fatalf("ask mismatch: got (has=%v price=%d qty=%d), want (has=%v price=%d qty=%d)",
			quote.HasAsk, quote.BestAsk, quote.AskQty, hasAsk, bestAsk, askQty)
	}
}

// --- B1: Insert into an empty side ---------------------------------------

func TestBook_B1_InsertIntoEmptySide(t *testing.T) {
	book := NewBook("ACME")
	order := makeResting("A", contract.Sell, 10021, 50)

	book.Insert(order)

	assertQuote(t, book.Quote(),
		false, 0, 0, // no bid
		true, 10021, 50,
	)
	if got := len(book.index); got != 1 {
		t.Fatalf("index len = %d, want 1", got)
	}
	// Level exists on the correct side.
	level, ok := book.asks.LevelAt(10021)
	if !ok {
		t.Fatal("level 10021 must exist on asks")
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != order {
		t.Fatal("level front must be the inserted order")
	}
}

// --- B2: Insert at an existing level -------------------------------------

func TestBook_B2_InsertAtExistingLevel(t *testing.T) {
	book := NewBook("ACME")
	first := makeResting("A", contract.Sell, 10021, 50)
	second := makeResting("B", contract.Sell, 10021, 30)

	book.Insert(first)
	book.Insert(second)

	assertQuote(t, book.Quote(),
		false, 0, 0,
		true, 10021, 80,
	)
	level, _ := book.asks.LevelAt(10021)
	if level.Orders.Front().Value.(*contract.RestingOrder) != first {
		t.Fatal("Front must remain A (oldest)")
	}
	if level.Orders.Back().Value.(*contract.RestingOrder) != second {
		t.Fatal("Back must be B (newest)")
	}
}

// --- B3: Same participant places a repeat order at the same price --------

func TestBook_B3_SameParticipantRepeatQueuesAtTail(t *testing.T) {
	// Participant "P1" places two orders at the same price. The second gets a
	// fresh OrderID and joins at the tail — no merging, no promotion.
	book := NewBook("ACME")
	first := &contract.RestingOrder{
		OrderID: "P1-1", Participant: "P1", Side: contract.Sell,
		Price: 10021, Remaining: 50, Seq: 1,
	}
	other := &contract.RestingOrder{
		OrderID: "P2-1", Participant: "P2", Side: contract.Sell,
		Price: 10021, Remaining: 30, Seq: 2,
	}
	firstRepeat := &contract.RestingOrder{
		OrderID: "P1-2", Participant: "P1", Side: contract.Sell,
		Price: 10021, Remaining: 20, Seq: 3,
	}

	book.Insert(first)
	book.Insert(other)
	book.Insert(firstRepeat)

	level, _ := book.asks.LevelAt(10021)
	// Expected order: P1-1, P2-1, P1-2.
	var seen []contract.OrderID
	for node := level.Orders.Front(); node != nil; node = node.Next() {
		seen = append(seen, node.Value.(*contract.RestingOrder).OrderID)
	}
	want := []contract.OrderID{"P1-1", "P2-1", "P1-2"}
	if len(seen) != len(want) {
		t.Fatalf("queue len = %d, want %d", len(seen), len(want))
	}
	for index := range seen {
		if seen[index] != want[index] {
			t.Fatalf("position %d: got %q want %q", index, seen[index], want[index])
		}
	}
	if level.TotalQty != 100 {
		t.Fatalf("TotalQty = %d, want 100", level.TotalQty)
	}
}

// --- B4: FIFO fill via ReduceFront (hand-worked scenario A) --------------

func TestBook_B4_ReduceFrontFIFOScenario(t *testing.T) {
	book := NewBook("ACME")
	orderA := makeResting("A", contract.Sell, 10021, 50)
	orderB := makeResting("B", contract.Sell, 10021, 30)
	orderC := makeResting("C", contract.Sell, 10021, 20)
	book.Insert(orderA)
	book.Insert(orderB)
	book.Insert(orderC)

	// Sanity: initial state matches the hand computation.
	assertQuote(t, book.Quote(), false, 0, 0, true, 10021, 100)

	level, _ := book.asks.LevelAt(10021)

	// Matcher iteration 1: buy taker wants 60; front=A(50); fillQty=min(60,50)=50.
	book.ReduceFront(level, 50)
	assertQuote(t, book.Quote(), false, 0, 0, true, 10021, 50)
	if _, stillIndexed := book.index["A"]; stillIndexed {
		t.Fatal("A must have been removed from the index on full consume")
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderB {
		t.Fatal("new front must be B")
	}

	// Matcher iteration 2: taker has 10 left; front=B(30); fillQty=min(10,30)=10.
	book.ReduceFront(level, 10)
	assertQuote(t, book.Quote(), false, 0, 0, true, 10021, 40)
	if orderB.Remaining != 20 {
		t.Fatalf("B.Remaining = %d, want 20", orderB.Remaining)
	}
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderB {
		t.Fatal("B must still be at front after partial fill")
	}
	if got := len(book.index); got != 2 {
		t.Fatalf("index len = %d, want 2 (B and C)", got)
	}
}

// --- B5: O(1) cancel from the middle of a level --------------------------

func TestBook_B5_CancelFromMiddleOfLevel(t *testing.T) {
	book := NewBook("ACME")
	orderA := makeResting("A", contract.Sell, 10021, 50)
	orderB := makeResting("B", contract.Sell, 10021, 30)
	orderC := makeResting("C", contract.Sell, 10021, 20)
	book.Insert(orderA)
	book.Insert(orderB)
	book.Insert(orderC)

	removed, ok := book.Cancel("B")

	if !ok || removed != orderB {
		t.Fatal("Cancel(B) must return orderB with ok=true")
	}
	level, _ := book.asks.LevelAt(10021)
	if level.Orders.Front().Value.(*contract.RestingOrder) != orderA {
		t.Fatal("Front must still be A")
	}
	if level.Orders.Back().Value.(*contract.RestingOrder) != orderC {
		t.Fatal("Back must still be C")
	}
	if level.TotalQty != 70 {
		t.Fatalf("TotalQty = %d, want 70", level.TotalQty)
	}
	// Top price hasn't moved, but AskQty did — quote must reflect it.
	assertQuote(t, book.Quote(), false, 0, 0, true, 10021, 70)
}

// --- B6: Cancel that empties a level and shifts the top (scenario B) -----

func TestBook_B6_CancelEmptiesLevelAndShiftsTop(t *testing.T) {
	book := NewBook("ACME")
	orderX := makeResting("X", contract.Buy, 10020, 100)
	orderY := makeResting("Y", contract.Buy, 10019, 50)
	book.Insert(orderX)
	book.Insert(orderY)

	assertQuote(t, book.Quote(), true, 10020, 100, false, 0, 0)

	removed, ok := book.Cancel("X")
	if !ok || removed != orderX {
		t.Fatal("Cancel(X) must return orderX with ok=true")
	}
	// Level 10020 emptied and was removed; top moved down to 10019.
	if _, present := book.bids.LevelAt(10020); present {
		t.Fatal("level 10020 must have been removed")
	}
	assertQuote(t, book.Quote(), true, 10019, 50, false, 0, 0)

	// Drain the last one — side goes empty.
	book.Cancel("Y")
	assertQuote(t, book.Quote(), false, 0, 0, false, 0, 0)
	if got := len(book.index); got != 0 {
		t.Fatalf("index len = %d, want 0", got)
	}
}

// --- B7: Cancel unknown OrderID ------------------------------------------

func TestBook_B7_CancelUnknownIsSilentMiss(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))
	preQuote := book.Quote()
	preIndexLen := len(book.index)

	removed, ok := book.Cancel("ghost")

	if ok || removed != nil {
		t.Fatalf("Cancel(unknown) = (%v, %v), want (nil, false)", removed, ok)
	}
	if book.Quote() != preQuote {
		t.Fatal("quote must be untouched by an unknown-id cancel")
	}
	if len(book.index) != preIndexLen {
		t.Fatal("index must be untouched by an unknown-id cancel")
	}

	// A second cancel of an already-cancelled id is also a silent miss.
	book.Cancel("A")
	if _, ok := book.Cancel("A"); ok {
		t.Fatal("second Cancel(A) must miss")
	}
}

// --- B8: ReduceFront full-consume vs partial -----------------------------

func TestBook_B8_ReduceFrontFullVsPartial(t *testing.T) {
	book := NewBook("ACME")
	orderA := makeResting("A", contract.Sell, 10021, 50)
	orderB := makeResting("B", contract.Sell, 10021, 30)
	book.Insert(orderA)
	book.Insert(orderB)

	// Partial: 20 of A consumed, A stays at front.
	book.ReduceFront(book.mustLevel(t, contract.Sell, 10021), 20)
	if orderA.Remaining != 30 {
		t.Fatalf("A.Remaining after partial = %d, want 30", orderA.Remaining)
	}
	if _, stillIndexed := book.index["A"]; !stillIndexed {
		t.Fatal("A must remain in the index after partial fill")
	}
	assertQuote(t, book.Quote(), false, 0, 0, true, 10021, 60)

	// Full: remaining 30 of A consumed, A removed, front becomes B.
	book.ReduceFront(book.mustLevel(t, contract.Sell, 10021), 30)
	if _, stillIndexed := book.index["A"]; stillIndexed {
		t.Fatal("A must be removed from the index on full consume")
	}
	if book.mustLevel(t, contract.Sell, 10021).Orders.Front().Value.(*contract.RestingOrder) != orderB {
		t.Fatal("new front must be B")
	}
	assertQuote(t, book.Quote(), false, 0, 0, true, 10021, 30)
}

// --- B9: SetLastPrice — bid/ask untouched --------------------------------

func TestBook_B9_SetLastPriceLeavesBidAskUntouched(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))
	book.Insert(makeResting("X", contract.Buy, 10020, 100))
	preQuote := book.Quote()
	if preQuote.HasLastPrice {
		t.Fatal("fresh book must not have last price")
	}

	book.SetLastPrice(10020)

	after := book.Quote()
	if !after.HasLastPrice || after.LastPrice != 10020 {
		t.Fatalf("last-price = (has=%v price=%d), want (true, 10020)", after.HasLastPrice, after.LastPrice)
	}
	// Bid/ask must be byte-identical to before.
	if after.HasBid != preQuote.HasBid || after.BestBid != preQuote.BestBid || after.BidQty != preQuote.BidQty {
		t.Fatal("bid side must be untouched by SetLastPrice")
	}
	if after.HasAsk != preQuote.HasAsk || after.BestAsk != preQuote.BestAsk || after.AskQty != preQuote.AskQty {
		t.Fatal("ask side must be untouched by SetLastPrice")
	}

	// A second SetLastPrice updates cleanly; HasLastPrice stays true.
	book.SetLastPrice(10022)
	after = book.Quote()
	if after.LastPrice != 10022 || !after.HasLastPrice {
		t.Fatalf("last-price after second set = (has=%v price=%d), want (true, 10022)", after.HasLastPrice, after.LastPrice)
	}
}

// --- cross-cutting scenarios --------------------------------------------

func TestBook_BothSidesIndependent(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10022, 50))
	book.Insert(makeResting("X", contract.Buy, 10020, 100))

	assertQuote(t, book.Quote(), true, 10020, 100, true, 10022, 50)

	// Cancelling on one side must not perturb the other.
	book.Cancel("A")
	assertQuote(t, book.Quote(), true, 10020, 100, false, 0, 0)

	book.Cancel("X")
	assertQuote(t, book.Quote(), false, 0, 0, false, 0, 0)
}

func TestBook_InsertBetterPriceShiftsBest(t *testing.T) {
	// Insert at a strictly better price than the current top and confirm the
	// quote follows. Both sides.
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10025, 50))
	assertQuote(t, book.Quote(), false, 0, 0, true, 10025, 50)

	book.Insert(makeResting("B", contract.Sell, 10023, 30)) // better ask (lower)
	assertQuote(t, book.Quote(), false, 0, 0, true, 10023, 30)

	book.Insert(makeResting("X", contract.Buy, 10018, 100))
	assertQuote(t, book.Quote(), true, 10018, 100, true, 10023, 30)

	book.Insert(makeResting("Y", contract.Buy, 10020, 40)) // better bid (higher)
	assertQuote(t, book.Quote(), true, 10020, 40, true, 10023, 30)
}

func TestBook_CancelMiddleLevelLeavesTopUnchanged(t *testing.T) {
	// Cancelling the sole order at a NON-best level should not change the top.
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10023, 50)) // best
	book.Insert(makeResting("B", contract.Sell, 10024, 30))
	book.Insert(makeResting("C", contract.Sell, 10025, 20))

	preQuote := book.Quote()
	book.Cancel("B") // removes the middle level entirely
	if book.Quote() != preQuote {
		t.Fatal("quote must be unchanged when cancelling from a non-best level")
	}
	// But the level itself must be gone.
	if _, present := book.asks.LevelAt(10024); present {
		t.Fatal("level 10024 must have been removed")
	}
}

func TestBook_QuoteReturnedByValue(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))

	snapshot := book.Quote()
	snapshot.BestAsk = 99999 // mutate the copy

	live := book.Quote()
	if live.BestAsk != 10021 {
		t.Fatalf("book state leaked through Quote(): BestAsk=%d, want 10021", live.BestAsk)
	}
}

// --- Best: matcher read-hook for the walk --------------------------------

func TestBook_Best_EmptySideReturnsFalse(t *testing.T) {
	book := NewBook("ACME")

	if level, ok := book.Best(contract.Buy); ok || level != nil {
		t.Fatalf("empty bid side: got (%v, %v), want (nil, false)", level, ok)
	}
	if level, ok := book.Best(contract.Sell); ok || level != nil {
		t.Fatalf("empty ask side: got (%v, %v), want (nil, false)", level, ok)
	}
}

func TestBook_Best_ReturnsExtremeLevel(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("B1", contract.Buy, 10018, 5))
	book.Insert(makeResting("B2", contract.Buy, 10020, 3)) // higher bid = better
	book.Insert(makeResting("A1", contract.Sell, 10025, 4))
	book.Insert(makeResting("A2", contract.Sell, 10022, 6)) // lower ask = better

	bestBid, hasBid := book.Best(contract.Buy)
	if !hasBid {
		t.Fatal("expected best bid to be present")
	}
	if bestBid.Price != 10020 {
		t.Errorf("best bid price: got %d, want 10020", bestBid.Price)
	}

	bestAsk, hasAsk := book.Best(contract.Sell)
	if !hasAsk {
		t.Fatal("expected best ask to be present")
	}
	if bestAsk.Price != 10022 {
		t.Errorf("best ask price: got %d, want 10022", bestAsk.Price)
	}
}

func TestBook_Best_PointerFeedsReduceFront(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A1", contract.Sell, 10022, 6))

	level, ok := book.Best(contract.Sell)
	if !ok {
		t.Fatal("expected best ask")
	}
	// Same pointer round-trips through ReduceFront without needing a lookup.
	book.ReduceFront(level, 4)

	if level.TotalQty != 2 {
		t.Errorf("level.TotalQty after ReduceFront: got %d, want 2", level.TotalQty)
	}
	quote := book.Quote()
	if quote.AskQty != 2 {
		t.Errorf("quote.AskQty: got %d, want 2", quote.AskQty)
	}
}

// --- panic tests: preconditions ------------------------------------------

func TestBook_Insert_PanicsOnDuplicateID(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))
	preQuote := book.Quote()
	preIndexLen := len(book.index)

	mustPanic(t, "duplicate OrderID", func() {
		book.Insert(makeResting("A", contract.Sell, 10022, 20))
	})

	// Validate-before-mutate: the failed Insert must NOT have changed anything.
	if book.Quote() != preQuote {
		t.Fatal("quote must be unchanged after a panicked Insert")
	}
	if len(book.index) != preIndexLen {
		t.Fatal("index must be unchanged after a panicked Insert")
	}
	if _, present := book.asks.LevelAt(10022); present {
		t.Fatal("no level for the rejected price must have been created")
	}
}

func TestBook_Insert_PanicsOnZeroRemaining(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "Remaining=0", func() {
		book.Insert(makeResting("A", contract.Sell, 10021, 0))
	})
}

func TestBook_Insert_PanicsOnNegativeRemaining(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "Remaining<0", func() {
		book.Insert(makeResting("A", contract.Sell, 10021, -1))
	})
}

func TestBook_Insert_PanicsOnZeroPrice(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "Price=0", func() {
		book.Insert(makeResting("A", contract.Sell, 0, 50))
	})
}

func TestBook_Insert_PanicsOnEmptyOrderID(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "empty OrderID", func() {
		book.Insert(makeResting("", contract.Sell, 10021, 50))
	})
}

func TestBook_Insert_PanicsOnNilOrder(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "nil order", func() { book.Insert(nil) })
}

func TestBook_ReduceFront_PanicsOnZeroBy(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))
	level, _ := book.asks.LevelAt(10021)
	mustPanic(t, "by=0", func() { book.ReduceFront(level, 0) })
}

func TestBook_ReduceFront_PanicsOnOverfill(t *testing.T) {
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))
	level, _ := book.asks.LevelAt(10021)
	mustPanic(t, "by > front.Remaining", func() { book.ReduceFront(level, 51) })
}

func TestBook_ReduceFront_PanicsOnNilLevel(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "nil level", func() { book.ReduceFront(nil, 1) })
}

func TestBook_SetLastPrice_PanicsOnZero(t *testing.T) {
	book := NewBook("ACME")
	mustPanic(t, "SetLastPrice(0)", func() { book.SetLastPrice(0) })
	mustPanic(t, "SetLastPrice(negative)", func() { book.SetLastPrice(-1) })
}

// --- helper on *Book used by B8 ------------------------------------------

// mustLevel is a test-only convenience that fails the test if the level
// doesn't exist. Keeps B8 free of the noise of double-return unpacking.
func (book *Book) mustLevel(t *testing.T, side contract.Side, price contract.Ticks) *PriceLevel {
	t.Helper()
	level, ok := book.sideBook(side).LevelAt(price)
	if !ok {
		t.Fatalf("expected a level at price %d on side %v", price, side)
	}
	return level
}
