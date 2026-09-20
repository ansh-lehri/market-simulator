package cost

import "simulator/contract"

// CostModule is the one interface every country implements. Callers (5.2's
// Convert / Release, 5.4's realization) bind only to this — swapping the
// country implementation cannot break a caller because the inputs and
// outputs are identical for every country.
type CostModule interface {
	// ComputeFillCost returns the per-fill turnover / statutory charges.
	// Charged once per fill.
	ComputeFillCost(input FillCostInput) CostBreakdown

	// ComputeOrderCost returns the per-executed-order charge (e.g. flat
	// brokerage). Charged once at the order's terminal step; the "fire
	// exactly once" guard lives in the caller, not here.
	ComputeOrderCost(input OrderCostInput) CostBreakdown
}

// FillCostInput is the per-fill input. Every country receives the same
// shape; a country ignores fields it does not use.
type FillCostInput struct {
	Side         contract.Side
	FillQty      int64 // units filled in this fill (non-negative)
	MakerPrice   int64 // integer ticks
	IsAutosquare bool  // set when this fill is a forced session-end close
}

// OrderCostInput is the per-order input, delivered at the order's terminal
// step. Every country receives the same shape; a country ignores fields it
// does not use.
type OrderCostInput struct {
	EverFilled bool          // false => the order never filled; per-order charge is zero
	TotalQty   int64         // original order qty; for markets that tier by size
	Side       contract.Side // for markets whose per-order fee is side-aware
}

// CostBreakdown is the country-agnostic output. Line names are data inside
// the Lines map (India: stt/stamp/exchange/sebi/gst/autosquare_fee/brokerage;
// other countries emit their own keys). Total is the only universal field
// callers depend on.
type CostBreakdown struct {
	Lines map[string]int64
	Total int64
}

// Rate expresses a ratio Num/Den in integer form, applied via applyRate.
// Example: 0.025% is Rate{25, 100_000}.
type Rate struct {
	Num int64
	Den int64
}
