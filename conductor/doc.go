// Package conductor is the session + market orchestration layer that
// sits above the broker. It brings canonical markets up on a timer,
// admits users into sessions, stands up on-demand instances, wires each
// exchange instance's edges to the broker (settlement) and market-data
// (tape), tears markets down, and serves the pre-session read doors.
//
// This file ships the scaffold: the Conductor, Registry, port
// interfaces, config loading, and the mux handoff. Flow logic (canonical
// timers, spin-up, join, on-demand, teardown, pre-session reads) lands
// in later phases.
//
// Load-bearing rules (from conductor/CLAUDE.md):
//   - Money-blind: the conductor reads no money tables.
//   - Identity-not-market-data: the conductor returns session/market
//     identity (catalog, instrument list, session windows); prices
//     (LTP/quote/depth) stream over the shipped market-data WebSocket,
//     never through here.
//   - One-mutex rule: Registry has one mutex, taken only for microsecond
//     reserve/finalize/state-flip steps — never across a call-out
//     (EnsureAccount, StartSession, WireInstance, exch.Start/Stop) or
//     a WebSocket ServeHTTP delegate.
//   - Unbuilt-seam rule: BrokerPort lists only shipped funnel methods
//     (EnsureAccount, StartSession, QualifyOnDemand); each later phase
//     grows the port as it builds the broker seam it needs — the
//     canonical bring-up phase adds WireInstance, the teardown phases
//     add EndSession and FinalizeMarket.
package conductor
