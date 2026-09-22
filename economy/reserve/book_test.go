package reserve

import (
	"reflect"
	"testing"

	"simulator/contract"
)

// The book is a private map of OrderID → *ReservationRecord. These tests
// pin the three primitive operations (put / get / remove) at the shape
// stage 2 depends on: get returns nil (not a zero value) on miss, remove
// on an unknown key is a no-op, and stored records are the same pointer
// out (not a copy) so the service can mutate them in place across their
// lifecycle.

func newRecordFixture(orderID OrderID) *ReservationRecord {
	return &ReservationRecord{
		OrderID:       orderID,
		AccountID:     "acct-1",
		SessionID:     "sess-1",
		InstrumentID:  contract.InstrumentID("ACME"),
		Side:          contract.Buy,
		IsMarket:      false,
		ReservedCash:  10_000,
		RemainingQty:  100,
		OrigQty:       100,
	}
}

// put + get round-trips the SAME pointer — service mutations to Reserved*
// / RemainingQty / flags must be visible on subsequent gets, which requires
// the book to hold the pointer, not a copy.
func TestBook_PutThenGetReturnsSamePointer(t *testing.T) {
	book := newReservationBook()
	record := newRecordFixture("order-A")

	book.put(record)

	got := book.get("order-A")
	if got == nil {
		t.Fatal("get returned nil after put — record missing")
	}
	if got != record {
		t.Errorf("get returned a different pointer than was put: got %p, want %p", got, record)
	}
	// Deep-equal for shape confirmation (belt + braces on the pointer check).
	if !reflect.DeepEqual(*got, *record) {
		t.Errorf("get returned record with mutated fields: got %+v, want %+v", *got, *record)
	}
}

// A get on an OrderID that was never put returns nil. This is the exact
// signal stage 2's service layer turns into an invariant-panic at
// Convert / Release / ReleaseExcess ("no reservation record for order").
func TestBook_GetUnknownOrderReturnsNil(t *testing.T) {
	book := newReservationBook()

	if got := book.get("nope"); got != nil {
		t.Errorf("get on unknown OrderID returned %+v, want nil", got)
	}
}

// A fresh book contains nothing — get on any key returns nil. Guards
// against a future edit that pre-seeds the map with sentinel entries.
func TestBook_FreshBookIsEmpty(t *testing.T) {
	book := newReservationBook()

	if len(book.records) != 0 {
		t.Errorf("newReservationBook contained %d records, want 0", len(book.records))
	}
	if got := book.get("any"); got != nil {
		t.Errorf("get on fresh book returned %+v, want nil", got)
	}
}

// remove clears the entry so a subsequent get returns nil. Stage 2 relies
// on this at the terminal paths (Convert fully-drained, Release full,
// ReleaseExcess completion) to prevent a second lifecycle call from
// finding a stale record.
func TestBook_RemoveClearsEntry(t *testing.T) {
	book := newReservationBook()
	record := newRecordFixture("order-A")
	book.put(record)

	book.remove("order-A")

	if got := book.get("order-A"); got != nil {
		t.Errorf("get after remove returned %+v, want nil", got)
	}
	if len(book.records) != 0 {
		t.Errorf("book contained %d records after remove, want 0", len(book.records))
	}
}

// remove on an unknown OrderID is a no-op — matches Go's delete-on-map
// semantics, and matches the doc's ReleaseExcess "may already be gone if
// Convert cleared it at a clean zero — nothing to true up" path (§4.2).
// Must not panic, must not corrupt other entries.
func TestBook_RemoveUnknownIsNoOp(t *testing.T) {
	book := newReservationBook()
	kept := newRecordFixture("order-A")
	book.put(kept)

	// remove a key that was never put — must not panic, must not affect "order-A".
	book.remove("never-put")

	if got := book.get("order-A"); got != kept {
		t.Errorf("remove of unknown key disturbed the kept entry: got %p, want %p", got, kept)
	}
	if len(book.records) != 1 {
		t.Errorf("book contained %d records, want 1", len(book.records))
	}
}

// put on an existing OrderID overwrites. The service layer will panic
// on a duplicate reserve, but the book itself must not: the same-key
// overwrite is a well-defined map operation, and locking it here would
// force the service to pre-check every put.
func TestBook_PutSameKeyOverwrites(t *testing.T) {
	book := newReservationBook()
	first := newRecordFixture("order-A")
	first.ReservedCash = 1_000
	book.put(first)

	second := newRecordFixture("order-A")
	second.ReservedCash = 2_000
	book.put(second)

	got := book.get("order-A")
	if got != second {
		t.Errorf("get returned first-put pointer, want second-put: got=%p first=%p second=%p", got, first, second)
	}
	if got.ReservedCash != 2_000 {
		t.Errorf("get returned ReservedCash=%d after overwrite, want 2000", got.ReservedCash)
	}
	if len(book.records) != 1 {
		t.Errorf("book contained %d records after overwrite, want 1", len(book.records))
	}
}

// Multiple distinct OrderIDs coexist and are looked up independently.
// Confirms the book scopes by OrderID, not by anything on the record
// (AccountID / SessionID / InstrumentID are link-back ids, not keys).
func TestBook_MultipleOrdersIndependent(t *testing.T) {
	book := newReservationBook()
	recordA := newRecordFixture("order-A")
	recordB := newRecordFixture("order-B")
	recordB.AccountID = "acct-2" // different account, same book (whole-economy scope)
	book.put(recordA)
	book.put(recordB)

	if got := book.get("order-A"); got != recordA {
		t.Errorf("get(order-A) returned %p, want %p", got, recordA)
	}
	if got := book.get("order-B"); got != recordB {
		t.Errorf("get(order-B) returned %p, want %p", got, recordB)
	}

	// Removing one must not disturb the other.
	book.remove("order-A")
	if got := book.get("order-A"); got != nil {
		t.Errorf("get(order-A) after remove returned %+v, want nil", got)
	}
	if got := book.get("order-B"); got != recordB {
		t.Errorf("get(order-B) after remove of order-A returned %p, want %p", got, recordB)
	}
}
