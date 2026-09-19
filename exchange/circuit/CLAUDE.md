# circuit — circuit band (1.5)

## Spec
`docs/exchange/EXCHANGE_00` + `EXCHANGE_15` (this component).
This component is the single source of truth for the circuit rule, consumed by two
others through two read surfaces — read their docs so both signatures match exactly:
- `docs/exchange/EXCHANGE_12_MATCHING_CORE.md` — owns the `CircuitClamp` interface;
  this component's `PriceAllowed(p) bool` MUST satisfy it (it replaces the matcher's
  clamp stub). Match its signature verbatim.
- `docs/exchange/EXCHANGE_14_ADMISSION_GATE.md` — the gate's `checkCircuitBand` reads
  `Bounds() (lower, upper Ticks)`. Returning the same edges `PriceAllowed` enforces is
  what makes admit-side and clamp-side agree.

## What this package owns
The per-instrument **circuit band** — the price range trading is allowed in for the
session — and the authority on "may a trade print at this price?". One `CircuitBand`
per instrument, computed once at session start from a frozen reference and the
rulebook band width, then effectively immutable. It exposes exactly two reads:
`PriceAllowed(p)` (the matcher's clamp-half) and `Bounds()` (the gate's reject-half).

## Invariants (EXCHANGE_15 §6)
1. The bounds the gate reads (`Bounds`) are exactly the bounds the clamp enforces
   (`PriceAllowed`). One object, one source of truth — admit-side and clamp-side
   cannot diverge.
2. The band is frozen for the session — computed once at `NewCircuitBand`, never
   recomputed (floating bands deferred).
3. Bounds are exact integer ticks — no float ever enters the band.
4. Hitting the band clamps the order, never the market — no session-wide halt
   (deferred).
5. The band is per-instrument — one per stock, from that stock's own reference; no
   shared or index-level band exists in the prototype.

## Deps
- `contract` (`Ticks`).
- `match` — the `CircuitClamp` interface this implements (no code dep, a contract to
  satisfy).

## Do NOT
- Recompute the band after construction — it is frozen for the session.
- Introduce any market-wide or index-level halt — clamp only, per-instrument.
- Let a float enter the bounds — integer-tick computation, floored, once.

## Testing (table)
Band computation with integer-floor rounding (`reference * bandBps / 10000`);
`PriceAllowed` at `lower` and `upper` (inclusive), inside, and just outside;
`Bounds()` returns exactly the edges `PriceAllowed` enforces (invariant 1). Tiny,
one file. Not golden-first.