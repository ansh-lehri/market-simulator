package wire

import "simulator/exchange/clock"

// stubPhase implements clock.SessionPhase with a settable phase, mirroring
// gate/stubs_test.go's version. Wire tests use it (rather than a real
// *clock.SessionClock) because injecting a fixed phase is simpler than
// building a schedule and driving CheckTransition — the real clock
// reports PhaseClosed until a transition is explicitly triggered.
type stubPhase struct {
	phase clock.Phase
}

func (stub *stubPhase) Current() clock.Phase { return stub.phase }

// setAllShardsPhase installs a fresh stubPhase on every shard so gate.Admit
// sees the requested phase throughout the test. Shared object is fine — the
// shard only reads via the interface.
func setAllShardsPhase(exchange *Exchange, phase clock.Phase) {
	stub := &stubPhase{phase: phase}
	for _, shard := range exchange.shards {
		shard.phase = stub
	}
}
