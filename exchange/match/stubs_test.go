package match

import "simulator/contract"

// allowAllClamp is the in-package CircuitClamp stub used until 1.5 lands.
type allowAllClamp struct{}

func (allowAllClamp) PriceAllowed(price contract.Ticks) bool { return true }

// bandedClamp is the in-package CircuitClamp stub used to exercise the
// clamp-halt code path in tests. Its shape mirrors what 1.5's CircuitBand
// will look like: a fixed inclusive [lower, upper] range.
type bandedClamp struct {
	lower contract.Ticks
	upper contract.Ticks
}

func (clamp bandedClamp) PriceAllowed(price contract.Ticks) bool {
	return price >= clamp.lower && price <= clamp.upper
}

// monotonicSeq is the in-package SeqSource stub used until 1.9 lands.
type monotonicSeq struct{ counter uint64 }

func (source *monotonicSeq) Next() contract.Seq {
	source.counter++
	return contract.Seq(source.counter)
}
