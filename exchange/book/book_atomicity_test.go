package book

import (
	"reflect"
	"sort"
	"testing"

	"simulator/contract"
)

// This file tests the validate-before-mutate discipline from CLAUDE.md §Errors
// vs panics: "Never partially mutate: validate fully before the first field
// write; on any error, state is untouched." Every mutator that panics on a bad
// precondition must leave the book byte-identical to how it was before the
// call. The tests here set up a non-trivial book state, attempt an illegal
// call, and assert not only that it panicked but that state is unchanged.

// bookSnapshot is a byte-comparable capture of everything observable about a
// Book: the quote, the set of live order ids, and the FIFO-preserving state
// of every level on both sides. Reused snapshotSide from book_property_test.go.
type bookSnapshot struct {
	quote    QuoteState
	indexIDs []contract.OrderID
	ask      []levelSnapshot
	bid      []levelSnapshot
}

func snapshotBook(book *Book) bookSnapshot {
	ids := make([]contract.OrderID, 0, len(book.index))
	for id := range book.index {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return bookSnapshot{
		quote:    book.Quote(),
		indexIDs: ids,
		ask:      snapshotSide(book.asks),
		bid:      snapshotSide(book.bids),
	}
}

// mustPanicWithNoStateChange snapshots the book, runs the action inside a
// panic recovery, and asserts (a) the action panicked and (b) the book is
// byte-identical to the pre-call snapshot. Failure of either fails the test.
func mustPanicWithNoStateChange(t *testing.T, book *Book, description string, action func()) {
	t.Helper()
	before := snapshotBook(book)
	defer func() {
		t.Helper()
		if recover() == nil {
			t.Fatalf("expected panic: %s", description)
		}
		after := snapshotBook(book)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("state changed after panic %q:\n  before: %+v\n  after:  %+v",
				description, before, after)
		}
	}()
	action()
}

// seedTypicalBook returns a fresh book with a mix of state — both sides
// populated with multiple levels and multiple orders per level, plus a last
// price already set. Gives any partial mutation somewhere concrete to leak.
func seedTypicalBook(t *testing.T) *Book {
	t.Helper()
	book := NewBook("ACME")
	book.Insert(makeResting("A", contract.Sell, 10021, 50))
	book.Insert(makeResting("B", contract.Sell, 10021, 30))
	book.Insert(makeResting("C", contract.Sell, 10023, 20))
	book.Insert(makeResting("X", contract.Buy, 10020, 100))
	book.Insert(makeResting("Y", contract.Buy, 10018, 40))
	book.SetLastPrice(10021)
	return book
}

// --- Insert atomicity ---------------------------------------------------

func TestBook_Atomicity_InsertNilOrder(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "nil order", func() {
		book.Insert(nil)
	})
}

func TestBook_Atomicity_InsertEmptyOrderID(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "empty OrderID", func() {
		book.Insert(makeResting("", contract.Sell, 10022, 15))
	})
}

func TestBook_Atomicity_InsertZeroRemaining(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "Remaining=0", func() {
		book.Insert(makeResting("NEW", contract.Sell, 10022, 0))
	})
}

func TestBook_Atomicity_InsertNegativeRemaining(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "Remaining<0", func() {
		book.Insert(makeResting("NEW", contract.Sell, 10022, -1))
	})
}

func TestBook_Atomicity_InsertZeroPrice(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "Price=0", func() {
		book.Insert(makeResting("NEW", contract.Sell, 0, 15))
	})
}

func TestBook_Atomicity_InsertNegativePrice(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "Price<0", func() {
		book.Insert(makeResting("NEW", contract.Sell, -10, 15))
	})
}

func TestBook_Atomicity_InsertDuplicateOrderID(t *testing.T) {
	book := seedTypicalBook(t)
	// The duplicate check is the LAST precondition — any earlier bail-out
	// wouldn't exercise the get-past-price-checks path.
	mustPanicWithNoStateChange(t, book, "duplicate OrderID", func() {
		book.Insert(makeResting("A", contract.Buy, 10025, 10))
	})
}

// --- ReduceFront atomicity ----------------------------------------------

func TestBook_Atomicity_ReduceFrontZeroBy(t *testing.T) {
	book := seedTypicalBook(t)
	level, _ := book.asks.LevelAt(10021)
	mustPanicWithNoStateChange(t, book, "by=0", func() {
		book.ReduceFront(level, 0)
	})
}

func TestBook_Atomicity_ReduceFrontNegativeBy(t *testing.T) {
	book := seedTypicalBook(t)
	level, _ := book.asks.LevelAt(10021)
	mustPanicWithNoStateChange(t, book, "by<0", func() {
		book.ReduceFront(level, -5)
	})
}

func TestBook_Atomicity_ReduceFrontOverfill(t *testing.T) {
	book := seedTypicalBook(t)
	level, _ := book.asks.LevelAt(10021) // front is A(50)
	mustPanicWithNoStateChange(t, book, "by > front.Remaining", func() {
		book.ReduceFront(level, 51)
	})
}

func TestBook_Atomicity_ReduceFrontOverfillByOne(t *testing.T) {
	// Boundary: by = front.Remaining + 1 is the tightest overfill; verify
	// it's caught atomically the same way as a larger overfill.
	book := seedTypicalBook(t)
	level, _ := book.bids.LevelAt(10018) // front is Y(40)
	mustPanicWithNoStateChange(t, book, "by = front.Remaining+1", func() {
		book.ReduceFront(level, 41)
	})
}

func TestBook_Atomicity_ReduceFrontNilLevel(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "nil level", func() {
		book.ReduceFront(nil, 10)
	})
}

func TestBook_Atomicity_ReduceFrontStaleEmptyLevel(t *testing.T) {
	// A caller holds a pointer to a level that has since been fully drained
	// and removed from the tree. ReduceFront must panic on the empty-front
	// check without mutating anything — a partial mutation here (e.g. deleting
	// an unrelated index entry) would be a serious bug.
	book := seedTypicalBook(t)
	staleLevel, _ := book.asks.LevelAt(10023) // sole order C(20)
	book.Cancel("C")                          // level drained; SideBook removes it
	mustPanicWithNoStateChange(t, book, "stale/empty level", func() {
		book.ReduceFront(staleLevel, 1)
	})
}

// --- SetLastPrice atomicity ---------------------------------------------

func TestBook_Atomicity_SetLastPriceZero(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "SetLastPrice(0)", func() {
		book.SetLastPrice(0)
	})
}

func TestBook_Atomicity_SetLastPriceNegative(t *testing.T) {
	book := seedTypicalBook(t)
	mustPanicWithNoStateChange(t, book, "SetLastPrice(-1)", func() {
		book.SetLastPrice(-1)
	})
}
