package wire

import (
	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// Shard is one worker owning many instruments (EXCHANGE_10 §4). Its run
// loop drives the serial per-instrument work; instruments are added to
// instrs at construction time in NewExchange and the map is not resized
// while the loop runs (mid-life instrument addition is out of scope for the
// prototype per EXCHANGE_10 §8).
//
// phase is widened from the concrete *clock.SessionClock to the
// SessionPhase interface: the shard only reads the phase (via gate.Admit),
// never writes it; the writer is the monitor goroutine (Stage 5), which
// keeps the concrete clock on Exchange. Widening here also lets tests inject
// a stub phase without touching the real clock.
type Shard struct {
	id      int
	inbound chan contract.InboundMessage
	instrs  map[contract.InstrumentID]*InstrumentState
	phase   clock.SessionPhase
	public  chan<- emit.TapeEvent
	private chan<- contract.EngineResult
}

func newShard(
	id int,
	inbound chan contract.InboundMessage,
	phase clock.SessionPhase,
	publicTape chan<- emit.TapeEvent,
	privateStream chan<- contract.EngineResult,
) *Shard {
	return &Shard{
		id:      id,
		inbound: inbound,
		instrs:  make(map[contract.InstrumentID]*InstrumentState),
		phase:   phase,
		public:  publicTape,
		private: privateStream,
	}
}
