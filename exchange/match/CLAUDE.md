# match — continuous matching core (1.2)

## Spec
`docs/exchange/EXCHANGE_00` + `EXCHANGE_12` (this component); `EXCHANGE_11` for the
book ops it calls.
`docs/exchange/EXCHANGE_15_CIRCUIT_SYSTEMS.md` + `docs/exchange/EXCHANGE_19_EVENT_EMITTER.md` - important to understand these pieces because this component the match engine, depends on 2 interfaces each owned by one of them namely CircuitClamp and SeqSource respectively. It will help to properly write the match engine if you could think how the interface possibly would be like given context of those pieces.

## What this package owns
The continuous matching walk: take one admitted order, decide every fill / rest /
cancel by calling the book's mutators, and return one `MatchResult`. It is the sole
writer of **last price** and the one place **price and slippage emerge** (a by-product
of which resting orders were hit — nothing computes them). The matcher decides; the
book executes.

## Invariants (EXCHANGE_12 §12)
1. Every trade prints at the MAKER's price — never the taker's.
2. Time priority within a level is respected (always consume `Orders.Front()` first).
3. No negative fills, no over-fill (`min(remaining, front.Remaining)`, market-buy
   further capped by affordability).
4. A self-match never prints a trade, under any STP mode.
5. Market and IOC never rest; only GTC-limit remainders rest.
6. Last price moves only on an actual print, and equals the final trade's maker price.
7. Zero calls across the money seam — the matcher holds no account reference; its only
   external consult is the in-engine `CircuitClamp`.

## Deps
- `book` (concrete) — read via `Best()`/level inspection; mutate ONLY via
  `Insert`/`Cancel`/`ReduceFront`.
- `CircuitClamp` and `SeqSource` are **interfaces — stub them now** (clamp = always
  allow; seq = monotonic counter). Replaced by `circuit` (1.5) and `emit` (1.9) later.
- `contract` (`Trade, MatchResult, CancelReason, FundedOrder, STPMode`).

## Do NOT
- Splice a queue node, edit `TotalQty`, or touch the index directly — only via book
  mutators.
- Hold a money/account reference or make any money-seam call.
- Emit wire events — return a neutral `MatchResult`.
- Build FOK — reserve the `OrdType` slot and a `matchFOK` entry point; leave deferred.

## Testing (subtle component — golden-first)
Write golden scenarios FIRST, expected values pinned from EXCHANGE_12, test red, then
implement to green: price-improvement (buy limit 10025 vs resting ask 10023 → prints
10023), slippage across levels, market funded-cap floor (integer division), IOC
leftover cancelled, each STP mode, circuit-clamp halt. One property test per invariant
above.