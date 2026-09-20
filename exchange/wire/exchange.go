package wire

import (
	"fmt"
	"sort"

	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// Exchange is the top-level assembly (EXCHANGE_10 §8). NewExchange composes
// the clock, P shards, ingress, and every per-instrument state. No
// goroutines are started here; a later stage adds Start() / Stop() as the
// ingress and shard run loops land.
//
// The three exposed channel fields are direction-narrowed on the seam: the
// caller writes to Inbound and reads from Public / Private. The internal
// bidirectional handles are retained so future stages can hand them to
// components that still need a bidirectional view.
type Exchange struct {
	Inbound chan<- contract.InboundMessage
	Public  <-chan emit.TapeEvent
	Private <-chan contract.EngineResult

	inbound   chan contract.InboundMessage
	public    chan emit.TapeEvent
	private   chan contract.EngineResult
	ingress   *Ingress
	shards    []*Shard
	monitor   *Monitor
	clock     *clock.SessionClock
	numShards int
}

// NewExchange composes an Exchange from a session schedule and a set of
// per-instrument specs. Panics on structural input errors (non-positive
// numShards, empty instruments, duplicate instrument id). Downstream
// constructors (clock, book, circuit, emit, match) panic on their own
// preconditions.
func NewExchange(numShards int, schedule clock.SessionSchedule, instruments []InstrumentSpec) *Exchange {
	if numShards <= 0 {
		panic(fmt.Sprintf("wire.NewExchange: numShards must be > 0, got %d", numShards))
	}
	if len(instruments) == 0 {
		panic("wire.NewExchange: instruments is empty")
	}

	sessionClock := clock.NewSessionClock(schedule)

	inbound := make(chan contract.InboundMessage)
	publicTape := make(chan emit.TapeEvent)
	privateStream := make(chan contract.EngineResult)

	shardInbound := make([]chan contract.InboundMessage, numShards)
	shardInboundSend := make([]chan<- contract.InboundMessage, numShards)
	shards := make([]*Shard, numShards)
	for shardID := 0; shardID < numShards; shardID++ {
		shardInbound[shardID] = make(chan contract.InboundMessage)
		shardInboundSend[shardID] = shardInbound[shardID]
		// sessionClock (*clock.SessionClock) implements clock.SessionPhase;
		// the shard receives it via the interface (see Shard.phase docs).
		shards[shardID] = newShard(shardID, shardInbound[shardID], sessionClock, publicTape, privateStream)
	}

	for _, spec := range instruments {
		shardID := int(hashInstrument(spec.Instrument) % uint32(numShards))
		if _, exists := shards[shardID].instrs[spec.Instrument]; exists {
			panic(fmt.Sprintf("wire.NewExchange: duplicate instrument %q", spec.Instrument))
		}
		shards[shardID].instrs[spec.Instrument] = newInstrumentState(spec, publicTape, privateStream)
	}
	// Finalise each shard's sorted instrument-id slice for deterministic
	// iteration in handleTick and handleControl.
	for _, shard := range shards {
		shard.instrIDs = sortedInstrumentIDs(shard.instrs)
	}

	ingress := newIngress(inbound, shardInboundSend)

	// Monitor takes the send-only end of every shard's control channel.
	// It is the sole caller of clock.CheckTransition (EXCHANGE_18 inv 2).
	controlSend := make([]chan<- controlMessage, numShards)
	for shardID := 0; shardID < numShards; shardID++ {
		controlSend[shardID] = shards[shardID].control
	}
	monitor := newMonitor(sessionClock, controlSend)

	return &Exchange{
		Inbound:   inbound,
		Public:    publicTape,
		Private:   privateStream,
		inbound:   inbound,
		public:    publicTape,
		private:   privateStream,
		ingress:   ingress,
		shards:    shards,
		monitor:   monitor,
		clock:     sessionClock,
		numShards: numShards,
	}
}

// sortedInstrumentIDs returns the map's keys as a slice sorted
// lexicographically. Called once per shard at construction; the resulting
// slice is stable for the shard's life (mid-life instrument add is out of
// scope per EXCHANGE_10 §8).
func sortedInstrumentIDs(instrs map[contract.InstrumentID]*InstrumentState) []contract.InstrumentID {
	ids := make([]contract.InstrumentID, 0, len(instrs))
	for id := range instrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
