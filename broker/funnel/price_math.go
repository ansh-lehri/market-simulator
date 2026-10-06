package funnel

import "simulator/contract"

// maxSpend is the FundedOrder.MaxSpend value stamped on a market-buy punch
// (BROKER_PHASE5 §1.5 P5-D3). It is also what reserve.Order's requiredCash
// evaluates to for the same inputs — bestAsk*qty + slippage buffer — so
// `reserved == MaxSpend` by construction and the exchange's funded-cap walk
// cannot overspend what the broker reserved.
//
// Returns 0 when invoked on a limit or sell path (bestAsk == 0, buffer == 0);
// callers do not need to branch — the result is harmless on FundedOrder when
// Type != Market.
//
// Pure function: integer math, no I/O, no logger.
func maxSpend(bestAsk contract.Ticks, qty contract.Qty, buffer int64) contract.Ticks {
	return bestAsk*qty + buffer
}

// slippageBuffer is the extra cash the broker reserves on top of
// bestAsk*qty, so a market-buy walk that climbs past bestAsk is still funded
// (BROKER_PHASE5 §1.4 "slippageBuffer sizing", P5-D8).
//
// Formula: notional * bps / 10000, integer truncation (bps = basis points =
// 1/10000). bps == 0 short-circuits to zero to make the "no buffer" case
// obviously allocation-free. A negative bps is a configuration bug and the
// result is undefined — bringup validates MoneyConfig.SlippageBufferBps >= 0.
//
// The dial lives on MoneyConfig.SlippageBufferBps: bps=0 is the current
// default while the market-data cache (B10) is unbuilt, so market-buy
// orders currently reject at mktCache.BestAsk anyway. When a real cache
// lands, bringup pins a volatility-based bps and this function's
// behaviour changes with the dial — no code edit needed here.
//
// Pure function: integer math, no I/O, no logger.
func slippageBuffer(bestAsk contract.Ticks, qty contract.Qty, bps int64) int64 {
	if bps == 0 {
		return 0
	}
	return bestAsk * qty * bps / 10000
}
