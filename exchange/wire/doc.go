// Package wire is the exchange's sharding & wiring spine (EXCHANGE_10). It
// assembles the finished component packages (book, match, gate, circuit,
// clock, emit) into a running Exchange: one Ingress fans the single edge
// inbound channel out to a fixed pool of Shards; each Shard owns many
// InstrumentStates and drives them from a serial loop. Every stub the
// component packages used in isolation is replaced here by the real
// implementation.
package wire
