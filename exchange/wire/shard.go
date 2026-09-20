package wire

import (
	"time"

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
// never writes it; the writer is the monitor goroutine, which keeps the
// concrete clock on Exchange. Widening here also lets tests inject a stub
// phase without touching the real clock.
//
// control is the shard's own control channel (Stage 5, EXCHANGE_10 §7).
// Monitor holds the send-only end and fans out session transitions here.
// Buffered so a transient burst around shutdown doesn't block the monitor.
//
// ticker delivers per-second ticks for time-based work (currently OHLCV
// rolls). Left nil until wired up by test setup or Exchange.Start (Stage
// 6). A nil channel arm in select never fires, so nil is a safe default.
//
// instrIDs is the sorted list of this shard's instrument ids. Iteration
// order across instruments — for OHLCV rolls and session-event fan-out —
// runs over this slice, not the map, so cross-instrument event order on
// the public tape is deterministic across runs.
type Shard struct {
	id       int
	inbound  chan contract.InboundMessage
	instrs   map[contract.InstrumentID]*InstrumentState
	instrIDs []contract.InstrumentID
	phase    clock.SessionPhase
	control  chan controlMessage
	ticker   <-chan time.Time
	public   chan<- emit.TapeEvent
	private  chan<- contract.EngineResult
}

// controlBuffer is the size of each shard's control channel. Sized to
// absorb the worst-case burst around a session shutdown (open + auction
// window + close, plus one in-flight) without blocking the monitor.
const controlBuffer = 4

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
		control: make(chan controlMessage, controlBuffer),
		public:  publicTape,
		private: privateStream,
	}
}
