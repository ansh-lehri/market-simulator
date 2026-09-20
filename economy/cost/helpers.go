package cost

// applyRate returns value*rate rounded half-up as an integer. The "+ Den/2"
// step is what makes rounding half-up (a fractional 0.5 rounds up to 1 rather
// than truncating to 0). Every rate line in every country routes through
// this single helper so results are bit-identical across runs and packages.
func applyRate(value int64, rate Rate) int64 {
	return (value*rate.Num + rate.Den/2) / rate.Den
}

// sum totals the values of a Lines map. The country-agnostic Total field in
// CostBreakdown is always sum(Lines).
func sum(lines map[string]int64) int64 {
	var total int64
	for _, amount := range lines {
		total += amount
	}
	return total
}
