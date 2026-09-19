# gate — admission pipeline (1.4)

## Spec
`docs/exchange/EXCHANGE_00` + `EXCHANGE_14` (this component); `EXCHANGE_11` for
`book.Quote()` and `EXCHANGE_12` for the `Matcher` it routes to.
This component is the receiving edge, so it OWNS or CONSULTS four collaborator
interfaces. Read the docs that will implement them, so the interface shapes you
define here are right from the start:
- `docs/exchange/EXCHANGE_18_SESSION_PHASE_CLOCK.md` — implements `SessionPhase`
  (`Current() Phase`), which this gate defines.
- `docs/exchange/EXCHANGE_13_AUCTION_MATCHER.md` — implements `AuctionBatch`
  (`Add(*FundedOrder)`), which this gate defines.
- `docs/exchange/EXCHANGE_15_CIRCUIT_SYSTEMS.md` — owns the band; `checkCircuitBand`
  reads its `Bounds()`. The doc shows a concrete `*CircuitBand`, but it is not built
  yet — define a minimal `Bounds() (Ticks, Ticks)` interface here (as with phase and
  auction) and let the real band satisfy it. Think about the bounds shape from EX_15.
- `SeqSource` (`Next() Seq`) is already defined in `match`; reuse it for
  `stampSequence`. Its real implementer is `emit` (`EXCHANGE_19`).

## What this package owns
The seven-stage admission pipeline — the receiving edge of the exchange. A
`FundedOrder` hits this first; the pipeline validates it (structural → session phase
→ liquidity → tick grid → circuit band), stamps its time-priority `Seq`, and routes
it to the continuous matcher or the auction batch — or rejects it. It produces the
engine's single output, `contract.EngineResult`. Gate 4 is gone: this pipeline never
consults money and holds no account reference.

## Invariants (EXCHANGE_14 §8)
1. A rejected order never mutates the book or consumes a `Seq` — rejection stops the
   pipeline before `stampSequence` and before `routeOrder`.
2. Every order that reaches matching or auction has a stamped `Seq`, assigned at
   exactly one site (`stampSequence`), feeding both forks.
3. `OrderID` is echoed on every `EngineResult`, all three outcomes.
4. The pipeline consults no money state and holds no account reference — Gate 4 is
   absent; funding is the broker's.
5. Auction-routed orders run every gate except liquidity — structural, phase, tick,
   and band all apply; only the immediate-matchability liquidity check is skipped.
6. Stage identifiers are descriptive, never positional — in code and in the
   `Rejection.Stage` diagnostic string.
7. The pipeline always terminates in `routeOrder` — the only stage besides a
   rejection that stops it, and it is always last.

## Deps
- `contract` — use `EngineResult`, `Outcome` (the SIX-value version), `Rejection`,
  `RejectReason`, `FundedOrder` FROM contract. Do NOT redefine EXCHANGE_14's local
  3-value `Outcome`; flag the discrepancy.
- `book` (concrete) — `Quote()` for `checkLiquidity`.
- `match` (concrete) — `Matcher.Match` for `routeOrder`; `SeqSource` interface reused.
- Interfaces defined + stubbed HERE: `SessionPhase`, `AuctionBatch`, and the band
  `Bounds()` interface. `Rulebook` is injected config (reads `TickSize` only).

## Do NOT
- Consult money or hold any account/reservation reference — Gate 4 does not exist here.
- Spend a `Seq` on, or mutate the book for, a rejected order.
- Use positional stage names (`gate2`) — descriptive only, matching `Rejection.Stage`.
- Build the auction clear, the real clock, or the real band — only the interfaces.

## Testing (example + table)
Table cases per reject reason (malformed, market-closed, no-liquidity, bad-tick,
beyond-circuit); routing cases (continuous → `Match`, closing-auction → `Add`); and
the structural invariants (a rejected order consumes no `Seq` and leaves the book
untouched; an auction-routed order skips only liquidity). Inject stub collaborators
(settable phase, a band with fixed bounds, a recording `AuctionBatch`, a monotonic
`SeqSource`). Not golden-first — deterministic branching, not emergent arithmetic.