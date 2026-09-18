# Exchange — Build Order
<!-- The sequence for implementing the exchange components. Referenced during
     orientation; not loaded every session. Broker has its own build order later. -->

## Ordering principle
Pure roots first (no dependencies, buildable and testable alone). Dependents that
need an unbuilt component depend on an **interface** and compile/test against a stub —
the concrete arrives later. The sharding/wiring spine is last; it wires finished
pieces, not moving targets.

## Sequence
| # | Package | Doc(s) | Role | Depends on / stubs |
|---|---------|--------|------|--------------------|
| 1 | `contract` | `Exchange_Broker_Contract` | The seam value types both sides import | none |
| 2 | `book` | EXCHANGE_00, EXCHANGE_11 | Order book (1.1) + derived quote (1.7); the four mutators | `contract` |
| 3 | `match` | EXCHANGE_12 | Continuous matching walk (1.2); emits `MatchResult`; writes last price | `book`; **stubs** `CircuitClamp`, `SeqSource` |
| 4 | `gate` | EXCHANGE_14 | Admission pipeline (1.4); produces `EngineResult` | `book`, `match`; **stubs** `SessionPhase`, `AuctionBatch`; reads `Rulebook` |
| 5 | `circuit` | EXCHANGE_15 | Per-instrument circuit band (1.5) | `contract`; **implements** `CircuitClamp` |
| 6 | `clock` | EXCHANGE_18 | Session-phase calculator (1.8) | `contract`; **implements** `SessionPhase` |
| 7 | `auction` | EXCHANGE_13 | Closing auction (1.3); uniform-price clear | `book`/`contract`; **implements** `AuctionBatch` |
| 8 | `emit` | EXCHANGE_19 | Event emitter + OHLCV + per-instrument event seq | all above; **implements** `SeqSource` |
| 9 | `wire` | EXCHANGE_10 | Sharding spine: ingress → shard pool → `InstrumentState` → egress | everything |

## Session notes
- **One session per component.** `match` and `wire` are the big ones — split at a
  clean stage boundary (commit, fresh session, re-orient) if a session runs long or
  starts to drift.
- `circuit` (5) and `clock` (6) are small — reasonable to pair in one sitting.
- **Git is the handoff.** A later component's session grounds on the *committed* code
  of the ones before it, not on their session context — so fresh sessions cost nothing.
- Steps 3–4 stub 5–8's interfaces; those stubs get replaced by the concrete types as
  5–8 land. Nothing is blocked on an unbuilt component.