package cost

import "simulator/contract"

// India cost-line names. Kept as unexported constants inside this file so
// they cannot leak into the generic surface; the map keys themselves are
// the India-facing contract with downstream (5.4's assembly).
const (
	lineSTT           = "stt"
	lineStamp         = "stamp"
	lineExchange      = "exchange"
	lineSEBI          = "sebi"
	lineGST           = "gst"
	lineAutosquareFee = "autosquare_fee"
	lineBrokerage     = "brokerage"
)

// IndiaRates is the injected India rulebook (STT / exchange / SEBI / stamp
// / GST / brokerage / autosquare-fee). Populated by wiring and handed to
// IndiaCostModule at construction; this package defines the shape, not
// the values-at-runtime.
type IndiaRates struct {
	STT           Rate  // sell-side only
	Exchange      Rate  // both sides
	SEBI          Rate  // both sides
	Stamp         Rate  // buy-side only
	GST           Rate  // applied to a taxable base (see ComputeFillCost / ComputeOrderCost)
	Brokerage     int64 // flat per-executed-order charge, in paise
	AutosquareFee int64 // CST-5 forced-close fee, in paise
}

// IndiaCostModule implements CostModule for India intraday equity. It is a
// pure value: no I/O, no state beyond the injected rulebook.
type IndiaCostModule struct {
	rates IndiaRates
}

// Compile-time assertion that IndiaCostModule satisfies CostModule.
var _ CostModule = IndiaCostModule{}

// NewIndiaCostModule constructs an India cost module from an injected
// rulebook. The rulebook is copied by value into the module; the caller's
// IndiaRates value can be reused or discarded freely afterwards.
func NewIndiaCostModule(rates IndiaRates) IndiaCostModule {
	return IndiaCostModule{rates: rates}
}

// ComputeFillCost returns the per-fill India charges. Side-aware: STT is
// sell-only, stamp is buy-only, exchange and SEBI apply on both sides.
// GST at fill time is 18% of (exchange + sebi), plus autosquare_fee when
// IsAutosquare is set — the autosquare-in-GST-base rule matches Zerodha's
// real behavior and is a deliberate deviation from doc §5.
func (module IndiaCostModule) ComputeFillCost(input FillCostInput) CostBreakdown {
	lines := map[string]int64{}
	turnover := input.MakerPrice * input.FillQty

	if input.Side == contract.Sell {
		lines[lineSTT] = applyRate(turnover, module.rates.STT)
	}
	if input.Side == contract.Buy {
		lines[lineStamp] = applyRate(turnover, module.rates.Stamp)
	}
	lines[lineExchange] = applyRate(turnover, module.rates.Exchange)
	lines[lineSEBI] = applyRate(turnover, module.rates.SEBI)

	gstBase := lines[lineExchange] + lines[lineSEBI]
	if input.IsAutosquare {
		lines[lineAutosquareFee] = module.rates.AutosquareFee
		gstBase += module.rates.AutosquareFee
	}
	lines[lineGST] = applyRate(gstBase, module.rates.GST)

	return CostBreakdown{Lines: lines, Total: sum(lines)}
}

// ComputeOrderCost returns the per-executed-order India charge: flat ₹20
// brokerage plus 18% GST on the brokerage. Charged once at the order's
// terminal step; an order that never filled pays zero. TotalQty and Side
// are ignored by India (present on the input for markets that need them).
//
// This method only *computes*; the fire-exactly-once guard lives in the
// caller (5.2's Convert / Release via the BrokerageCharged flag on the
// reservation record). Calling this twice with EverFilled=true returns the
// full brokerage twice — that is a caller bug, not this method's concern.
func (module IndiaCostModule) ComputeOrderCost(input OrderCostInput) CostBreakdown {
	if !input.EverFilled {
		return CostBreakdown{Lines: map[string]int64{}, Total: 0}
	}
	lines := map[string]int64{
		lineBrokerage: module.rates.Brokerage,
		lineGST:       applyRate(module.rates.Brokerage, module.rates.GST),
	}
	return CostBreakdown{Lines: lines, Total: sum(lines)}
}
