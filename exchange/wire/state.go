package wire

import (
	"time"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/circuit"
	"simulator/exchange/emit"
	"simulator/exchange/gate"
	"simulator/exchange/match"
)

// InstrumentSpec is the caller-supplied per-instrument input to NewExchange.
// Layer 7 composes these from the rulebook and today's session parameters
// (reference price, band width, tick size, OHLCV cadence).
type InstrumentSpec struct {
	Instrument    contract.InstrumentID
	Reference     contract.Ticks
	BandBps       int
	Rules         gate.Rulebook
	OHLCVInterval time.Duration
	OHLCVStart    time.Time
}

// InstrumentState bundles every per-instrument component (EXCHANGE_10 §3).
// Owned by exactly one Shard and touched only from that shard's serial loop,
// so nothing inside needs a lock.
//
// Must not be copied by value: admitSeqSource holds &admitSeq, so a copy
// would leave the source pointing at the original's counter field.
type InstrumentState struct {
	instrument     contract.InstrumentID
	book           *book.Book
	matcher        *match.Matcher
	band           *circuit.CircuitBand
	auction        gate.AuctionBatch
	ohlcv          *emit.OHLCVAggregator
	emitter        *emit.Emitter
	admitSeq       uint64
	admitSeqSource admitSeqCounter
	rules          gate.Rulebook
}

// newInstrumentState builds one instrument's bundled state. The two shared
// outbound channels (public tape, private stream) are handed in so every
// instrument on every shard writes the same two edge channels.
//
// Downstream constructors panic on their own preconditions (nil channels,
// non-positive Reference / BandBps / OHLCVInterval, etc.); this function
// adds no validation of its own.
func newInstrumentState(
	spec InstrumentSpec,
	publicTape chan<- emit.TapeEvent,
	privateStream chan<- contract.EngineResult,
) *InstrumentState {
	orderBook := book.NewBook(spec.Instrument)
	band := circuit.NewCircuitBand(spec.Reference, spec.BandBps)
	emitter := emit.NewEmitter(spec.Instrument, publicTape, privateStream)
	matcher := match.NewMatcher(orderBook, band, emitter)
	ohlcv := emit.NewOHLCVAggregator(spec.Instrument, spec.OHLCVInterval, spec.OHLCVStart, spec.Reference)

	state := &InstrumentState{
		instrument: spec.Instrument,
		book:       orderBook,
		matcher:    matcher,
		band:       band,
		auction:    deferredAuctionBatch{},
		ohlcv:      ohlcv,
		emitter:    emitter,
		rules:      spec.Rules,
	}
	// Assign admitSeqSource AFTER the struct is heap-allocated so counter
	// points at the field's final address. Copying InstrumentState by value
	// would break this invariant — see the type's doc comment.
	state.admitSeqSource = admitSeqCounter{counter: &state.admitSeq}
	return state
}
