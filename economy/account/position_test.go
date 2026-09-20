package account

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// Fixed timestamps for lot fixtures — pinning EntryAt keeps golden equality
// checks stable and mirrors the "hand takes clock as an argument" rule
// (determinism > convenience).
var (
	t1 = time.Unix(1_000, 0).UTC()
	t2 = time.Unix(2_000, 0).UTC()
	t3 = time.Unix(3_000, 0).UTC()
)

// snapshotPosition takes a deep copy of a Position so a later DeepEqual can
// detect any mutation — including in-place lot edits (RemainingQty decrement)
// that a shallow copy would hide because the slice header still points at
// the same backing array.
func snapshotPosition(position Position) Position {
	snap := position
	if position.OpenLots != nil {
		snap.OpenLots = make([]Lot, len(position.OpenLots))
		copy(snap.OpenLots, position.OpenLots)
	}
	return snap
}

// assertPosition compares Available, Reserved, and lot-by-lot; separates
// "empty" from "nil" cleanly (slicing a length-1 slice with [1:] leaves a
// non-nil zero-length slice that reflect.DeepEqual would treat as unequal
// to a nil expected value).
func assertPosition(t *testing.T, got, want Position) {
	t.Helper()
	if got.Available != want.Available {
		t.Errorf("Available = %d, want %d", got.Available, want.Available)
	}
	if got.Reserved != want.Reserved {
		t.Errorf("Reserved = %d, want %d", got.Reserved, want.Reserved)
	}
	if len(got.OpenLots) != len(want.OpenLots) {
		t.Fatalf("OpenLots len = %d, want %d\n got=%+v\nwant=%+v", len(got.OpenLots), len(want.OpenLots), got.OpenLots, want.OpenLots)
	}
	for index := range got.OpenLots {
		if got.OpenLots[index] != want.OpenLots[index] {
			t.Errorf("OpenLots[%d] = %+v, want %+v", index, got.OpenLots[index], want.OpenLots[index])
		}
	}
}

// -----------------------------------------------------------------------------
// MovePosition
// -----------------------------------------------------------------------------

func TestMovePosition_LegalEdges(t *testing.T) {
	tests := []struct {
		name       string
		start      Position
		from, to   PosBucket
		units      int64
		wantAvail  int64
		wantReserv int64
	}{
		{"reserve (lock): partial", Position{Available: 100, Reserved: 20}, PosAvailable, PosReserved, 40, 60, 60},
		{"reserve (lock): drain source", Position{Available: 100, Reserved: 20}, PosAvailable, PosReserved, 100, 0, 120},
		{"release (unlock): partial", Position{Available: 10, Reserved: 50}, PosReserved, PosAvailable, 20, 30, 30},
		{"release (unlock): drain source", Position{Available: 10, Reserved: 50}, PosReserved, PosAvailable, 50, 60, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			position := testCase.start
			if err := position.MovePosition(testCase.from, testCase.to, testCase.units); err != nil {
				t.Fatalf("MovePosition returned %v, want nil", err)
			}
			if position.Available != testCase.wantAvail {
				t.Errorf("Available = %d, want %d", position.Available, testCase.wantAvail)
			}
			if position.Reserved != testCase.wantReserv {
				t.Errorf("Reserved = %d, want %d", position.Reserved, testCase.wantReserv)
			}
			// unit-total unchanged (conserving move)
			if position.Available+position.Reserved != testCase.start.Available+testCase.start.Reserved {
				t.Errorf("unit conservation broken: before=%d after=%d", testCase.start.Available+testCase.start.Reserved, position.Available+position.Reserved)
			}
		})
	}
}

func TestMovePosition_Insufficient(t *testing.T) {
	tests := []struct {
		name     string
		start    Position
		from, to PosBucket
		units    int64
		wantHave int64
	}{
		{"reserve overshoot", Position{Available: 10, Reserved: 0}, PosAvailable, PosReserved, 11, 10},
		{"release overshoot", Position{Available: 0, Reserved: 5}, PosReserved, PosAvailable, 6, 5},
		{"reserve from empty", Position{Available: 0, Reserved: 100}, PosAvailable, PosReserved, 1, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := snapshotPosition(testCase.start)
			position := testCase.start
			err := position.MovePosition(testCase.from, testCase.to, testCase.units)
			if !errors.Is(err, ErrInsufficientUnits) {
				t.Errorf("errors.Is(err, ErrInsufficientUnits) = false, err=%v", err)
			}
			var detail ErrInsufficientPosition
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrInsufficientPosition, err=%v", err)
			}
			if detail.Have != testCase.wantHave || detail.Want != testCase.units {
				t.Errorf("detail = %+v, want {%d %d}", detail, testCase.wantHave, testCase.units)
			}
			if !reflect.DeepEqual(position, snapshot) {
				t.Errorf("position mutated on error: got %+v, want %+v", position, snapshot)
			}
		})
	}
}

// The double-commit guard is where "can't sell the same units twice" becomes
// structural. Locks it as a scenario, not just a table entry.
func TestMovePosition_DoubleCommitGuard(t *testing.T) {
	position := Position{Available: 100, Reserved: 0}

	// First resting sell reserves 60.
	if err := position.MovePosition(PosAvailable, PosReserved, 60); err != nil {
		t.Fatalf("first reserve failed: %v", err)
	}
	if position.Available != 40 || position.Reserved != 60 {
		t.Fatalf("after first reserve: got %+v, want {40 60 []}", position)
	}

	// Second resting sell tries to reserve another 60 — only 40 remain in
	// Available; the guard must fire and state must NOT drift.
	snapshot := snapshotPosition(position)
	err := position.MovePosition(PosAvailable, PosReserved, 60)
	if !errors.Is(err, ErrInsufficientUnits) {
		t.Errorf("expected ErrInsufficientUnits, got %v", err)
	}
	var detail ErrInsufficientPosition
	if !errors.As(err, &detail) {
		t.Fatalf("errors.As failed, err=%v", err)
	}
	if detail.Have != 40 || detail.Want != 60 {
		t.Errorf("detail = %+v, want {40 60}", detail)
	}
	if !reflect.DeepEqual(position, snapshot) {
		t.Errorf("position mutated on error: got %+v, want %+v", position, snapshot)
	}
}

func TestMovePosition_IllegalEdges(t *testing.T) {
	tests := []struct {
		name     string
		from, to PosBucket
	}{
		{"same-bucket PosAvailable", PosAvailable, PosAvailable},
		{"same-bucket PosReserved", PosReserved, PosReserved},
	}
	start := Position{Available: 100, Reserved: 50, OpenLots: []Lot{{EntryPrice: 500, EntryAt: t1, RemainingQty: 150}}}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := snapshotPosition(start)
			position := snapshotPosition(start) // fresh, independent lot slice
			err := position.MovePosition(testCase.from, testCase.to, 10)
			if !errors.Is(err, ErrIllegalOperation) {
				t.Errorf("errors.Is(err, ErrIllegalOperation) = false, err=%v", err)
			}
			var detail ErrIllegalPosMove
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrIllegalPosMove, err=%v", err)
			}
			if detail.From != testCase.from || detail.To != testCase.to {
				t.Errorf("detail = %+v, want {%s %s}", detail, testCase.from, testCase.to)
			}
			if !reflect.DeepEqual(position, snapshot) {
				t.Errorf("position mutated on error: got %+v, want %+v", position, snapshot)
			}
		})
	}
}

func TestMovePosition_NonPositiveUnits(t *testing.T) {
	start := Position{Available: 100, Reserved: 50, OpenLots: []Lot{{500, t1, 150}}}
	for _, units := range []int64{0, -1, -1_000_000} {
		snapshot := snapshotPosition(start)
		position := snapshotPosition(start)
		err := position.MovePosition(PosAvailable, PosReserved, units)
		if !errors.Is(err, ErrNonPositiveUnits) {
			t.Errorf("units=%d: errors.Is(err, ErrNonPositiveUnits) = false, err=%v", units, err)
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("units=%d: category chain broken, err=%v", units, err)
		}
		if !reflect.DeepEqual(position, snapshot) {
			t.Errorf("units=%d: position mutated on error", units)
		}
	}
}

// OpenLots is untouched by any MovePosition — reservation moves counts, never
// lots. This test fixes that against a future edit that "helpfully" starts
// matching lots at reserve time (which would be wrong per doc §1.2).
func TestMovePosition_OpenLotsUntouched(t *testing.T) {
	original := []Lot{
		{EntryPrice: 500, EntryAt: t1, RemainingQty: 50},
		{EntryPrice: 502, EntryAt: t2, RemainingQty: 50},
	}
	position := Position{Available: 100, Reserved: 0, OpenLots: original}
	lotsSnapshot := make([]Lot, len(original))
	copy(lotsSnapshot, original)

	// Reserve 40 units, then release 20 — two legal moves; lots must stay identical.
	if err := position.MovePosition(PosAvailable, PosReserved, 40); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := position.MovePosition(PosReserved, PosAvailable, 20); err != nil {
		t.Fatalf("release: %v", err)
	}
	if !reflect.DeepEqual(position.OpenLots, lotsSnapshot) {
		t.Errorf("OpenLots mutated by MovePosition: got %+v, want %+v", position.OpenLots, lotsSnapshot)
	}
}

// -----------------------------------------------------------------------------
// CreditPosition
// -----------------------------------------------------------------------------

func TestCreditPosition_SingleCreditIntoEmpty(t *testing.T) {
	position := Position{}
	if err := position.CreditPosition(50, 500, t1); err != nil {
		t.Fatalf("CreditPosition returned %v, want nil", err)
	}
	assertPosition(t, position, Position{
		Available: 50,
		Reserved:  0,
		OpenLots:  []Lot{{EntryPrice: 500, EntryAt: t1, RemainingQty: 50}},
	})
}

// Sequential credits at different prices/times must land at the TAIL in
// arrival order — reversing would be LIFO, breaking cost basis + realized P&L.
func TestCreditPosition_MultipleCreditsAppendAtTailInArrivalOrder(t *testing.T) {
	position := Position{}
	credits := []struct {
		units      int64
		entryPrice int64
		entryAt    time.Time
	}{
		{30, 500, t1},
		{20, 505, t2},
		{50, 498, t3},
	}
	for _, credit := range credits {
		if err := position.CreditPosition(credit.units, credit.entryPrice, credit.entryAt); err != nil {
			t.Fatalf("credit %+v returned %v", credit, err)
		}
	}
	assertPosition(t, position, Position{
		Available: 100,
		Reserved:  0,
		OpenLots: []Lot{
			{EntryPrice: 500, EntryAt: t1, RemainingQty: 30}, // oldest first
			{EntryPrice: 505, EntryAt: t2, RemainingQty: 20},
			{EntryPrice: 498, EntryAt: t3, RemainingQty: 50}, // newest last
		},
	})
}

// Coupling holds after every credit: Available+Reserved == Σ RemainingQty.
// A property test in stage 5 fuzzes this — here we lock the deterministic case.
func TestCreditPosition_PreservesCouplingInvariant(t *testing.T) {
	position := Position{Available: 10, Reserved: 5, OpenLots: []Lot{{500, t1, 15}}}
	if err := position.CreditPosition(25, 510, t2); err != nil {
		t.Fatalf("credit failed: %v", err)
	}
	if got := position.Available + position.Reserved; got != sumRemaining(position.OpenLots) {
		t.Errorf("coupling broken: A+R=%d Σlots=%d", got, sumRemaining(position.OpenLots))
	}
}

func TestCreditPosition_NonPositiveUnits(t *testing.T) {
	start := Position{Available: 10, Reserved: 5, OpenLots: []Lot{{500, t1, 15}}}
	for _, units := range []int64{0, -1, -1_000_000} {
		snapshot := snapshotPosition(start)
		position := snapshotPosition(start)
		err := position.CreditPosition(units, 500, t2)
		if !errors.Is(err, ErrNonPositiveUnits) {
			t.Errorf("units=%d: expected ErrNonPositiveUnits, got %v", units, err)
		}
		if !reflect.DeepEqual(position, snapshot) {
			t.Errorf("units=%d: state mutated on error", units)
		}
	}
}

func TestCreditPosition_BadPrice(t *testing.T) {
	start := Position{Available: 10, Reserved: 5, OpenLots: []Lot{{500, t1, 15}}}
	for _, entryPrice := range []int64{0, -1, -999} {
		snapshot := snapshotPosition(start)
		position := snapshotPosition(start)
		err := position.CreditPosition(50, entryPrice, t2)
		if !errors.Is(err, ErrBadPrice) {
			t.Errorf("entryPrice=%d: expected ErrBadPrice, got %v", entryPrice, err)
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("entryPrice=%d: category chain broken, err=%v", entryPrice, err)
		}
		if !reflect.DeepEqual(position, snapshot) {
			t.Errorf("entryPrice=%d: state mutated on error", entryPrice)
		}
	}
}

// -----------------------------------------------------------------------------
// RemovePosition — golden FIFO cases (hand-computed, pinned as expected values)
// -----------------------------------------------------------------------------

func TestRemovePosition_GoldenFIFO(t *testing.T) {
	tests := []struct {
		name        string
		start       Position
		sellUnits   int64
		wantMatched []LotMatch
		wantEnd     Position
	}{
		{
			// Doc §1.4 worked example: 60 sold, spans two lots.
			name:      "A: mid-lot span across 2 lots (doc §1.4 example)",
			start:     Position{Available: 0, Reserved: 100, OpenLots: []Lot{{500, t1, 50}, {502, t2, 50}}},
			sellUnits: 60,
			wantMatched: []LotMatch{
				{EntryPrice: 500, Qty: 50, EntryAt: t1},
				{EntryPrice: 502, Qty: 10, EntryAt: t2},
			},
			wantEnd: Position{Available: 0, Reserved: 40, OpenLots: []Lot{{502, t2, 40}}},
		},
		{
			name:      "B: span across 3 lots ending mid-lot",
			start:     Position{Available: 0, Reserved: 100, OpenLots: []Lot{{50, t1, 30}, {55, t2, 20}, {60, t3, 50}}},
			sellUnits: 70,
			wantMatched: []LotMatch{
				{EntryPrice: 50, Qty: 30, EntryAt: t1},
				{EntryPrice: 55, Qty: 20, EntryAt: t2},
				{EntryPrice: 60, Qty: 20, EntryAt: t3},
			},
			wantEnd: Position{Available: 0, Reserved: 30, OpenLots: []Lot{{60, t3, 30}}},
		},
		{
			name:      "C: exact-boundary consumes whole first lot",
			start:     Position{Available: 20, Reserved: 70, OpenLots: []Lot{{100, t1, 30}, {110, t2, 60}}},
			sellUnits: 30,
			wantMatched: []LotMatch{
				{EntryPrice: 100, Qty: 30, EntryAt: t1},
			},
			wantEnd: Position{Available: 20, Reserved: 40, OpenLots: []Lot{{110, t2, 60}}},
		},
		{
			name:      "D: full drain — all lots consumed",
			start:     Position{Available: 0, Reserved: 100, OpenLots: []Lot{{100, t1, 30}, {110, t2, 70}}},
			sellUnits: 100,
			wantMatched: []LotMatch{
				{EntryPrice: 100, Qty: 30, EntryAt: t1},
				{EntryPrice: 110, Qty: 70, EntryAt: t2},
			},
			wantEnd: Position{Available: 0, Reserved: 0, OpenLots: []Lot{}},
		},
		{
			name:      "E: partial into a single lot leaves lot with remainder",
			start:     Position{Available: 0, Reserved: 50, OpenLots: []Lot{{200, t1, 50}}},
			sellUnits: 20,
			wantMatched: []LotMatch{
				{EntryPrice: 200, Qty: 20, EntryAt: t1},
			},
			wantEnd: Position{Available: 0, Reserved: 30, OpenLots: []Lot{{200, t1, 30}}},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			position := snapshotPosition(testCase.start) // independent lot slice per subtest
			matched, err := position.RemovePosition(testCase.sellUnits)
			if err != nil {
				t.Fatalf("RemovePosition returned %v, want nil", err)
			}
			if !reflect.DeepEqual(matched, testCase.wantMatched) {
				t.Errorf("matched = %+v\nwant %+v", matched, testCase.wantMatched)
			}
			assertPosition(t, position, testCase.wantEnd)
			// §0.6 coupling holds post-call.
			if position.Available+position.Reserved != sumRemaining(position.OpenLots) {
				t.Errorf("coupling broken post-call: A+R=%d Σlots=%d", position.Available+position.Reserved, sumRemaining(position.OpenLots))
			}
		})
	}
}

func TestRemovePosition_InsufficientReserved(t *testing.T) {
	tests := []struct {
		name     string
		start    Position
		units    int64
		wantHave int64
	}{
		{"reserved=5 sell 6", Position{Available: 0, Reserved: 5, OpenLots: []Lot{{100, t1, 5}}}, 6, 5},
		{"reserved=0 sell 1", Position{Available: 0, Reserved: 0}, 1, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := snapshotPosition(testCase.start)
			position := snapshotPosition(testCase.start)
			matched, err := position.RemovePosition(testCase.units)
			if !errors.Is(err, ErrInsufficientUnits) {
				t.Errorf("errors.Is(err, ErrInsufficientUnits) = false, err=%v", err)
			}
			var detail ErrInsufficientReserved
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrInsufficientReserved, err=%v", err)
			}
			if detail.Have != testCase.wantHave || detail.Want != testCase.units {
				t.Errorf("detail = %+v, want {%d %d}", detail, testCase.wantHave, testCase.units)
			}
			if matched != nil {
				t.Errorf("matched must be nil on error, got %+v", matched)
			}
			if !reflect.DeepEqual(position, snapshot) {
				t.Errorf("position mutated on error: got %+v, want %+v", position, snapshot)
			}
		})
	}
}

// Critical corner (doc §1.4): even if Available would cover the sell, the
// guard checks Reserved. Units in flight live in Reserved, not Available.
// A future edit that "helpfully" checks Available+Reserved would flip this
// test red.
func TestRemovePosition_ChecksReservedNotAvailable(t *testing.T) {
	position := Position{
		Available: 100, // plenty here
		Reserved:  5,   // but only 5 locked for a resting sell
		OpenLots:  []Lot{{100, t1, 105}},
	}
	snapshot := snapshotPosition(position)
	_, err := position.RemovePosition(6)
	if !errors.Is(err, ErrInsufficientUnits) {
		t.Errorf("expected ErrInsufficientUnits, got %v", err)
	}
	var detail ErrInsufficientReserved
	if !errors.As(err, &detail) {
		t.Fatalf("errors.As failed, err=%v", err)
	}
	if detail.Have != 5 || detail.Want != 6 {
		t.Errorf("detail = %+v, want {5 6} — must report Reserved, NOT Available", detail)
	}
	if !reflect.DeepEqual(position, snapshot) {
		t.Errorf("position mutated on error")
	}
}

func TestRemovePosition_NonPositiveUnits(t *testing.T) {
	start := Position{Available: 0, Reserved: 100, OpenLots: []Lot{{500, t1, 100}}}
	for _, units := range []int64{0, -1, -1_000_000} {
		snapshot := snapshotPosition(start)
		position := snapshotPosition(start)
		matched, err := position.RemovePosition(units)
		if !errors.Is(err, ErrNonPositiveUnits) {
			t.Errorf("units=%d: expected ErrNonPositiveUnits, got %v", units, err)
		}
		if matched != nil {
			t.Errorf("units=%d: matched must be nil on error, got %+v", units, matched)
		}
		if !reflect.DeepEqual(position, snapshot) {
			t.Errorf("units=%d: state mutated on error", units)
		}
	}
}
