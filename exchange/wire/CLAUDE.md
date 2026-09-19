# wire — sharding & wiring spine (1.10)

## Spec
`docs/exchange/EXCHANGE_10` (this spine) + `EXCHANGE_00` (split, seam, §6a sharding).
This assembles finished pieces, so read every component doc it wires and use each
component's REAL type — this is where every earlier stub is replaced:
- `EXCHANGE_11` book, `EXCHANGE_12` matcher, `EXCHANGE_13` auction, `EXCHANGE_14` gate
  pipeline + `gateCtx`, `EXCHANGE_15` real `CircuitBand`, `EXCHANGE_18` real
  `SessionClock` + the monitor that drives it, `EXCHANGE_19` real `Emitter` (the real
  `SeqSource`). No stub survives this layer.

## What this package owns
The spine: `InstrumentState` (all of one instrument's components bundled), `Shard`
(one worker owning many instruments, serial loop), `Ingress` (routes the one inbound
channel by `hash(Instrument)%P`), the top-level `Exchange`, the shard loop
(`handleSubmit`/`handleCancel`/`handleTick`), `buildGateCtx`, the global close
fan-out, and session startup/shutdown. It owns the per-second monitor and
`handleTransition` (the acting half the clock deliberately left out).

## Invariants (EXCHANGE_10 §0 standing rules + §10 concurrency)
1. One-in / one-out per connection — one inbound channel, one outbound path (public +
   private), multiplexed and self-described by an `Instrument` tag; never a
   per-instrument channel exposed to a connected party.
2. Every inbound message carries a `BrokerID`, echoed on the corresponding private
   `EngineResult`; a broker's private events never leak to another broker.
3. Single writer per instrument — each instrument's state is owned by one shard and
   mutated only by that shard's serial loop; no locks in the components.
4. Parallelism across shards, sharing no mutable state; the one cross-shard shared
   read is the global session phase (`atomic.Int32`, one writer, many readers).
5. Two per-instrument sequences — `admitSeq` (order time priority) and the emitter's
   event `Seq` — are distinct counters, neither shared across shards.

## Deps
- Everything: `contract` (`InboundMessage`/`CancelRequest` come from here), `book`,
  `match`, `gate`, `circuit`, `clock`, `auction`, `emit`.

## Do NOT
- Expose a per-instrument channel to a connected party — one inbound, one outbound.
- Add a lock inside a component — single-writer-per-shard is the concurrency model.
- Let more than one owner decide the market closed — the monitor owner fans the close
  out; shards don't each independently trigger global work.
- Build persistence of exchange internals — deferred (§11); in-memory for the demo.

## Testing (integration / e2e — may split)
End-to-end: an order in produces the right private `EngineResult` and public
`TapeEvent`s; ingress routes an instrument to the correct shard; the cancel path;
a phase transition fires the auction-clear fan-out; and a determinism check (same
inbound sequence → identical output). This is the vertical slice — the first full
order-in-to-events-out path. Expect to stage across more than one session.