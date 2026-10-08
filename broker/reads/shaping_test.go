package reads

import (
	"testing"

	"simulator/broker/funnel"
	"simulator/contract"
)

// Cover both branches plus the defensive "unknown" sink — if a zero-
// value or out-of-range Side ever leaks into a view, the response must
// surface "unknown" loudly rather than silently defaulting to "buy".
func TestSideString(t *testing.T) {
	cases := []struct {
		side contract.Side
		want string
	}{
		{contract.Buy, "buy"},
		{contract.Sell, "sell"},
		{contract.Side(42), "unknown"},
	}
	for _, tc := range cases {
		if got := sideString(tc.side); got != tc.want {
			t.Errorf("sideString(%v) = %q, want %q", tc.side, got, tc.want)
		}
	}
}

// Every live OrderStatus value plus the defensive "unknown" sink for
// the zero value.
func TestStatusString(t *testing.T) {
	cases := []struct {
		status funnel.OrderStatus
		want   string
	}{
		{funnel.OrderStatusWorking, "working"},
		{funnel.OrderStatusPartiallyFilled, "partially_filled"},
		{funnel.OrderStatusFilled, "filled"},
		{funnel.OrderStatusCancelled, "cancelled"},
		{funnel.OrderStatusRejected, "rejected"},
		{funnel.OrderStatusUnknown, "unknown"},
	}
	for _, tc := range cases {
		if got := statusString(tc.status); got != tc.want {
			t.Errorf("statusString(%v) = %q, want %q", tc.status, got, tc.want)
		}
	}
}

// An empty positions list must serialize as [] not null — the JSON
// encoder emits "null" for a nil slice, and clients iterate the array
// without type-checking it.
func TestShapePositions_EmptyInputReturnsEmptySlice(t *testing.T) {
	got := shapePositions(nil)
	if got == nil {
		t.Fatalf("shapePositions(nil) returned nil; must return [] for the HTTP encoder")
	}
	if len(got) != 0 {
		t.Fatalf("shapePositions(nil) returned %d rows, want 0", len(got))
	}
}

// Same for the orders list — empty working set serializes as [], not null.
func TestShapeOrders_EmptyInputReturnsEmptySlice(t *testing.T) {
	got := shapeOrders(nil)
	if got == nil {
		t.Fatalf("shapeOrders(nil) returned nil; must return []")
	}
	if len(got) != 0 {
		t.Fatalf("shapeOrders(nil) returned %d rows, want 0", len(got))
	}
}

// filled_qty is derived here at the response edge, not stored on the
// funnel view. Guards against a drift where someone adds a FilledQty
// field upstream and the two sources go out of sync.
func TestShapeOrders_DerivesFilledQty(t *testing.T) {
	rows := shapeOrders([]funnel.OpenOrderView{
		{OrderID: "o-1", Side: contract.Buy, OrigQty: 10, RemainingQty: 3, Status: funnel.OrderStatusPartiallyFilled},
	})
	if rows[0]["filled_qty"].(int64) != 7 {
		t.Fatalf("filled_qty = %v, want 7", rows[0]["filled_qty"])
	}
}

// No shaper may leak a "unit" / currency field — money across every
// read response is a bare int64 with no label.
func TestShapers_NoUnitField(t *testing.T) {
	balance := shapeBalance(funnel.BalanceSnapshot{Present: true})
	if _, has := balance["unit"]; has {
		t.Error("shapeBalance leaked unit field")
	}
	positions := shapePositions([]funnel.PositionView{{Instrument: "x"}})
	if _, has := positions[0]["unit"]; has {
		t.Error("shapePositions leaked unit field")
	}
	orders := shapeOrders([]funnel.OpenOrderView{{OrderID: "x"}})
	if _, has := orders[0]["unit"]; has {
		t.Error("shapeOrders leaked unit field")
	}
}
