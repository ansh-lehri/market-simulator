package wire

import (
	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// Shard is one worker owning many instruments (EXCHANGE_10 §4). One
// goroutine will drive the shard's serial loop in a later stage; Stage 1
// constructs the struct but does not start any goroutine.
//
// Fields are exchange-internal (lowercase); nothing outside the wire package
// reads or mutates a Shard directly. Instruments are added to instrs at
// construction time in NewExchange; the map is not resized while the loop
// runs (mid-life instrument addition is out of scope for the prototype per
// EXCHANGE_10 §8).
type Shard struct {
	id      int
	inbound chan contract.InboundMessage
	instrs  map[contract.InstrumentID]*InstrumentState
	clock   *clock.SessionClock
	public  chan<- emit.TapeEvent
	private chan<- contract.EngineResult
}

func newShard(
	id int,
	inbound chan contract.InboundMessage,
	sessionClock *clock.SessionClock,
	publicTape chan<- emit.TapeEvent,
	privateStream chan<- contract.EngineResult,
) *Shard {
	return &Shard{
		id:      id,
		inbound: inbound,
		instrs:  make(map[contract.InstrumentID]*InstrumentState),
		clock:   sessionClock,
		public:  publicTape,
		private: privateStream,
	}
}
