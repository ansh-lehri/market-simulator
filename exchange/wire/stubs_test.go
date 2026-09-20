package wire

import "simulator/exchange/clock"

// stubPhase implements clock.SessionPhase with a settable phase, mirroring
// gate/stubs_test.go's version. Wire tests need this because
// *clock.SessionClock's checkTransition is unexported — the real clock can
// only report PhaseClosed from outside its package until Stage 5's monitor
// goroutine lands.
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
