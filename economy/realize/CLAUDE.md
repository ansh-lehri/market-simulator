# economy/realize (Layer 5.4) — Realized-P&L / Settlement
<!-- Per-package CLAUDE.md. READ ORDER for any session working in this package, all four
     apply together:
       1) /CLAUDE.md            — root project constitution
       2) /economy/CLAUDE.md    — money-truth (L5) constitution
       3) THIS file             — what is specific to the realize package
       4) the spec doc below    — the authority for behavior
     This file does not repeat the two constitutions above. -->

## Spec (the authority — read in orientation, implement exactly, invent nothing)
- `docs/economy/LAYER_5_4_REALIZATION.md` — the whole doc (self-contained).

## What 5.4 IS (one line)
The **narrative** layer: it computes "how much did a closed trade make/lose after costs, and what
shows while a position is open." It **explains** money movement; it does **not** move money — with
exactly one exception (session-end auto-square, §5).

## Owns / public surface
- **`Realizer`** holding: `book` (the close-accumulators), a read handle to accounts (`ledger`, for
  `PersistOnClose`), and `persist Persistence`.
- **Methods:** `ComputeRealizedPnl` (item 37, per-fill FIFO, **pure**); `OnSellFill` (the handler
  `Convert`'s sell branch calls — folds per-fill realization into the accumulator, flushes one row on
  flat); `SettleSessionEnd` / auto-square (item 41, §5); `ComputeUnrealizedMark` (item 39, **pure**);
  `ComputeWeightedAvgPrice` (item 40, **pure**).
- **Types:** `RealizationInput`, `RealizationAccumulator`, `MatchedLot`, the three-line view, the
  closed-trade record (`row` + `costRow`). Consumes `CostBreakdown` (from 5.3) and `MatchedLot`s
  (from 5.1's `RemovePosition`).
- **The ONE piece of state it owns: the close-accumulator** — one entry per open
  `(account, instrument)` round-trip. Nothing else.

## Boundaries & invariants (verbatim from the doc — load-bearing, do not cross)
- **Boundary 1 — owns NO lot queue; pure consumer.** The FIFO queue is Tier-1's `Position.OpenLots`;
  `RemovePosition` does the match and **returns** the matched lots. 5.4 computes arithmetic on them and
  **never holds, mutates, or re-derives lot state.**
- **Boundary 2 — 5.4 computes; 5.2 moves money.** On a voluntary close the cash already moved
  (`Convert` did `CreditCash`). The **only** exception is auto-square (§5): no order, no `Convert`, so
  5.4 books settlement directly via Tier-1 hands.
- **One `participant_trades` row = one full round-trip** (not per fill) — partial sells **accumulate**
  until the position is flat, then flush a single row + one `trade_costs` child.
- **PersistOnClose gate:** canonical → enqueue; on-demand → compute + display but enqueue **nothing**.
  The realized-P&L **math is identical** for both; only the write-back differs.
- **Signed throughout** — gross/net may be negative; **never clamp**.
- **Brokerage is NOT here** — order-level, excluded from every `trade_costs` row and from `NetPnl`
  (it's 5.2/5.3's order path). 5.4 must not add it.
- **Determinism:** one round-trip → exactly one row + one cost child, emitted on flat.

## Persistence seam (this package CARRIES it)
- `Realizer` holds `persist Persistence` and calls **`EnqueueClosedTrade(row, costRow)`** on flat,
  **only when `PersistOnClose`**. Never writes Postgres directly — the drainer (elsewhere, not 5.4)
  does. **Phase 1: inject a no-op / spy `Persistence`**; assert it's called on canonical, NOT called on
  on-demand.
- `PersistOnClose` is read from the account at accumulator birth (`ledger.MustGet(...).PersistOnClose`).

## Deps & seam
- **Depends on `account`** (the `MatchedLot`/`Lot` shapes, `ReadAvailablePosition`, `MustGet` for
  `PersistOnClose`, and — for auto-square only — the Tier-1 hands `RemovePosition` / `CreditCash`).
- **Uses `CostBreakdown` from `cost`** as an input **value** — 5.4 **receives** the per-fill breakdown
  (5.2 computed it); it does **not** call `CostModule` itself.
- **Does NOT depend on `reserve`** — `reserve` depends on 5.4, not the reverse.

## Do NOT (in this package)
- Never own / mutate / re-derive the lot queue (Boundary 1).
- Never move money on a voluntary close — `Convert` already did (`CreditCash`). Only **auto-square**
  books via Tier-1 hands (Boundary 2). No other `CreditCash` / `DebitCash`.
- Never put brokerage in a `trade_costs` row or in `NetPnl`.
- Never write Postgres directly; never enqueue when `PersistOnClose == false`.
- Never clamp a signed P&L to zero.
- No goroutine, no mutex — single-threaded (single-writer assumed).

## Flag in orientation (do not silently resolve)
- **`EnqueueClosedTrade` signature:** this doc calls it `(row, costRow)` (two args); the 5.2 doc §1.3
  shows `(t ClosedTradeRow)` (one arg). Reconcile against the actual seam before wiring — **5.4 is the
  caller** and assembles both row + cost child.
- **Close-accumulator crash recovery is DEFERRED** — the accumulator is live session state (a partial
  round-trip) and belongs in the snapshot as a fourth mirrored thing, but that's the joint
  snapshot-capture pass, **not built here**. Phase 1's no-op persistence is correct.
- **`PositionNowFlat`** is passed in by `Convert` (a small Tier-1/5.2 wiring seam).

## Tests (this package — golden-FIRST; no DB, spy Persistence)
- **Golden, hand-computed FIRST:** `ComputeRealizedPnl` FIFO multi-lot (proceeds − FIFO basis −
  per-fill costs = realized), and weighted-average display vs FIFO realization on a partial close.
- **Accumulator:** partial sells accumulate → exactly **one** row on flat; one row + one cost child.
- **Gate:** canonical → spy `EnqueueClosedTrade` **called** with the right row; on-demand → spy
  **not called**, but P&L still computed.
- **Auto-square (§5):** applies the sell stack + CST-5, books via Tier-1 hands, `exit_reason =
  auto_square`.
- **`rapid` property:** signed P&L never clamped.
- Real `account` (built earlier) may be used; `CostBreakdown` fed as test input. `-race` on,
  **single-threaded**, determinism replay (same fills → identical rows).