package wire

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/clock"
)

// makeSchedule returns a two-boundary schedule (Continuous → Closed) starting
// at the Unix epoch. Tests that need a specific phase drive the clock
// separately; NewExchange only asserts the schedule is well-formed.
func makeSchedule() clock.SessionSchedule {
	start := time.Unix(0, 0)
	return clock.SessionSchedule{
		Boundaries: []clock.PhaseBoundary{
			{At: start, Phase: clock.PhaseContinuous},
			{At: start.Add(time.Hour), Phase: clock.PhaseClosed},
		},
	}
}

// TestNewExchange_InstrumentLandsOnHashedShard verifies the routing
// invariant (EXCHANGE_10 §5): every instrument lives in exactly one shard,
// namely shards[hashInstrument(id) % numShards].
func TestNewExchange_InstrumentLandsOnHashedShard(t *testing.T) {
	ids := []contract.InstrumentID{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
		"K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
		"U", "V", "W", "X", "Y", "Z",
	}
	specs := make([]InstrumentSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, makeSpec(id))
	}
	numShards := 4
	exchange := NewExchange(numShards, makeSchedule(), specs)

	for _, id := range ids {
		expectedShard := int(hashInstrument(id) % uint32(numShards))
		if _, present := exchange.shards[expectedShard].instrs[id]; !present {
			t.Fatalf("instrument %q missing from shard %d (its hashed home)", id, expectedShard)
		}
		for other := 0; other < numShards; other++ {
			if other == expectedShard {
				continue
			}
			if _, present := exchange.shards[other].instrs[id]; present {
				t.Fatalf("instrument %q also found in shard %d (expected only in %d)", id, other, expectedShard)
			}
		}
	}
}

// TestNewExchange_ClockInitialPhaseIsClosed pins the clock's start-of-life
// state (EXCHANGE_18: NewSessionClock stores PhaseClosed until the first
// CheckTransition). Wire relies on this so the gate rejects orders before
// the monitor first fires.
func TestNewExchange_ClockInitialPhaseIsClosed(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("A")})
	if got := exchange.clock.Current(); got != clock.PhaseClosed {
		t.Fatalf("initial phase = %v, want PhaseClosed", got)
	}
}

// TestNewExchange_PanicsOnZeroShards / _Negative / _EmptyInstruments /
// _Duplicate cover NewExchange's structural preconditions.
func TestNewExchange_PanicsOnZeroShards(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("expected panic on numShards = 0")
		}
	}()
	NewExchange(0, makeSchedule(), []InstrumentSpec{makeSpec("A")})
}

func TestNewExchange_PanicsOnNegativeShards(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("expected panic on numShards < 0")
		}
	}()
	NewExchange(-1, makeSchedule(), []InstrumentSpec{makeSpec("A")})
}

func TestNewExchange_PanicsOnEmptyInstruments(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("expected panic on empty instruments")
		}
	}()
	NewExchange(1, makeSchedule(), nil)
}

func TestNewExchange_PanicsOnDuplicateInstrument(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("expected panic on duplicate instrument id")
		}
	}()
	NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("A"), makeSpec("A")})
}

// TestNewExchange_NoGoroutinesStarted confirms Stage 1 leaves the exchange
// dormant. With no ingress / shard loops running, Public and Private have
// no possible writer, so a non-blocking receive must hit the default arm.
func TestNewExchange_NoGoroutinesStarted(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("A")})
	select {
	case event := <-exchange.Public:
		t.Fatalf("received event on Public before Start: %+v", event)
	case result := <-exchange.Private:
		t.Fatalf("received result on Private before Start: %+v", result)
	default:
	}
}

// TestNewExchange_UnusedShardHasEmptyInstrs verifies shards without any
// hashed instrument are still constructed (empty map, non-nil) — later
// stages iterate shards unconditionally.
func TestNewExchange_UnusedShardHasEmptyInstrs(t *testing.T) {
	// With numShards much larger than one instrument, at least one shard
	// will end up empty regardless of hash outcome.
	exchange := NewExchange(8, makeSchedule(), []InstrumentSpec{makeSpec("A")})
	nonEmpty := 0
	for _, shard := range exchange.shards {
		if shard.instrs == nil {
			t.Fatalf("shard %d has nil instrs map — should be empty but initialized", shard.id)
		}
		if len(shard.instrs) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty != 1 {
		t.Fatalf("expected exactly 1 shard with instruments, got %d", nonEmpty)
	}
}
