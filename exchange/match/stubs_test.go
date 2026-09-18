package match

import "simulator/contract"

// allowAllClamp is the in-package CircuitClamp stub used until 1.5 lands.
type allowAllClamp struct{}

func (allowAllClamp) PriceAllowed(price contract.Ticks) bool { return true }

// monotonicSeq is the in-package SeqSource stub used until 1.9 lands.
type monotonicSeq struct{ counter uint64 }

func (source *monotonicSeq) Next() contract.Seq {
	source.counter++
	return contract.Seq(source.counter)
}
