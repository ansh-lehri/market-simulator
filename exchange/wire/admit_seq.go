package wire

import (
	"simulator/contract"
	"simulator/exchange/match"
)

// admitSeqCounter adapts the per-instrument admission counter stored on
// InstrumentState to the match.SeqSource interface the gate's stampSequence
// stage consumes. It holds a pointer to InstrumentState.admitSeq so callers
// see monotonic increments as the instrument accepts orders.
//
// Pointer stability: newInstrumentState assigns the counter after the
// containing struct is heap-allocated, so &state.admitSeq is stable for the
// instrument's life. InstrumentState must never be copied by value — a copy
// would leave admitSeqSource pointing at the original's field.
type admitSeqCounter struct {
	counter *uint64
}

// Compile-time check: admitSeqCounter satisfies match.SeqSource.
var _ match.SeqSource = admitSeqCounter{}

// Next advances the counter and returns the new value. Called once per order
// at the stampSequence stage; the stamped value carries order time priority
// within one book.
func (source admitSeqCounter) Next() contract.Seq {
	*source.counter++
	return contract.Seq(*source.counter)
}
