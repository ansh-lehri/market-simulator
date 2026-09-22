# economy/reserve (Layer 5.2) — Reservation Service (the brain)
<!-- Per-package CLAUDE.md. READ ORDER for any session working in this package, all four
     apply together:
       1) /CLAUDE.md            — root project constitution
       2) /economy/CLAUDE.md    — money-truth (L5) constitution
       3) THIS file             — what is specific to the reserve package
       4) the spec doc below    — the authority for behavior
     This file does not repeat the two constitutions above. -->

## Spec (the authority — read in orientation, implement exactly, invent nothing)
- `docs/economy/LAYER_5_2_RESERVATION_FLOWS.md` — the whole doc (self-contained).

## What 5.2 IS (one line)
The **reservation brain**: it decides *what* moves and *why*, then directs the Tier-1 hands (5.1) to
perform the mutation. It **holds no balances** and **never mutates an account field** — 5.1 is the
sole writer. It is the top of the compute stack: it calls **down** into account, cost, and realize.

## Owns / public surface
- **`ReservationService`** holding: `book` (the reservation book), `ledger` (`AccountID → *Account`),
  `costs CostModule` (5.3), `realiz Realizer` (5.4), `events EventSink` (deferred), `persist
  Persistence`.
- **`ReservationBook`** (5.2-held, **separate from `Account`**, whole-economy scope, keyed by
  `OrderID`) of **`ReservationRecord`** — carries its own `AccountID`/`SessionID`/`InstrumentID`,
  `Side`, `IsMarket`, `ReservedCash`/`ReservedUnits`/`RemainingQty`, and the cost-lifecycle flags
  **`OrigQty` / `EverFilled` / `BrokerageCharged`**. Link-back ids only — **never a pointer into an
  `Account`**.
- **Four methods:**
  - `ReserveIfAvailable(participant, order) → reserved | reason` — the engine's **one synchronous,
    blocking** consult (Gate 4).
  - `Convert(order_id, fill_qty, maker_price)` — per-fill lifecycle bookkeeping.
  - `Release(order_id, release_qty = all)` — non-fill terminal.
  - `ReleaseExcess(order_id)` — market-buy buffer true-up.

## The core invariant (verbatim §0.1 — the through-line of all four methods)
> The reservation covers **exactly** the still-unfilled quantity; debits/credits cover **exactly** the
> filled quantity; the two never overlap.
This is what makes partial-fill-then-cancel correct: the filled part converted, the unfilled remainder
released, no overlap. Hold it at every instant.

## Locked rules (verbatim — a wrong guess corrupts money)
- **Cost ordering (§3.3):** principal move + position delivery **first**, then the 5.3 cost debit
  **after** — same for buy and sell. Friction rides on top of settled principal.
- **Brokerage (§3.4 / §5):** charged **once per executed order**, at `RemainingQty == 0` inside
  `Convert`, guarded by `BrokerageCharged`. `EverFilled` is set on **any** `Convert`. The **partial-
  fill-then-cancel** case terminates in `Release`, so `Release` **also** checks and charges brokerage —
  `BrokerageCharged` makes it fire exactly once regardless of which method sees the terminal state.
  `ReleaseExcess` needs **no** brokerage block (a full-fill market buy already charged in `Convert`) —
  a `BrokerageCharged == true` assertion is the right check there. **Never charge brokerage per fill.**
- **No debt by construction (§2.7):** you can only `Convert` what you reserved — the
  `MoveCash(Reserved, Settled, …)` is sufficiency-guarded against `Reserved`, so no negative balance is
  reachable from reservation.
- **Atomicity is free (§2.8):** single goroutine per session + single-active-session rule → no order
  from one participant races another. **No lock.**
- **Failure never partially mutates:** return the typed reason **before any move** on the failure path.
  A hand-guard failing **after** a positive read-check is a **programming error → panic**, not a
  returned error.

## Persistence seam (this package CARRIES it)
- 5.2's own persistence call is **`MarkSnapshotDirty(sessionID, accountID)`** — async, after mutating
  the `Account` (via hands) and the `ReservationBook` (directly). **No synchronous Redis/Postgres write
  anywhere in 5.2.** `EnqueueClosedTrade` is called by **5.4** on the realization path, not by reserve.
- Reservation records are live session state mirrored by the async snapshotter
  (`l5:snap:…:resv:{order_id}`) — reserve **mutates the book + marks dirty**; the mirror itself is the
  snapshotter's job. **Phase 1: inject a no-op / spy `Persistence`** and assert `MarkSnapshotDirty` was
  called.

## Deps & seam (top of the compute stack — built LAST)
- **`account`** — `ledger` resolves `*Account`; hands used: `ReadAvailableCash`/`ReadAvailablePosition`,
  `MoveCash`, `MovePosition`, `CreditCash`, `DebitCash`, `CreditPosition`, `RemovePosition`.
- **`cost`** — calls `CostModule.ComputeFillCost` (per fill) and `ComputeOrderCost` (brokerage at
  terminal); debits the returned `Total` via `DebitCash`.
- **`realize`** — calls `Realizer.OnSellFill` on sell fills (hands over matched lots + cost breakdown +
  `PositionNowFlat`).
- **`events`** — `EventSink`: wire the reserve/convert/release call sites; the event **system** is
  deferred (§9).

## Do NOT (in this package)
- Never mutate a balance directly — **5.1 writes**; 5.2 only directs the hands.
- Never do cost math (call `CostModule`) or FIFO realization (call `Realizer.OnSellFill`) inline.
- Never write a store synchronously on the hot path — only `MarkSnapshotDirty` (async).
- Never let `Convert` move more than was reserved (sufficiency-guarded); never allow a negative balance
  from reservation.
- Never charge brokerage more than once, and never per fill.
- Never partially mutate on a failure path. No goroutine, no mutex.

## Flag in orientation (do not silently resolve)
- **`EnqueueClosedTrade` signature:** this doc §1.3 shows `(t ClosedTradeRow)` (one arg); the 5.4 doc
  shows `(row, costRow)` (two args). **5.4 is the real caller** — reconcile the interface to match it.
- **Item 32 (per-archetype Gate-4 rule)** is **deferred** — `ReserveIfAvailable` treats every
  participant identically for now; do not add archetype branching.
- **Money-event system deferred** — wire the call sites, do not build the system.
- **`PositionNowFlat`** — `Convert` reads post-`RemovePosition` state to tell 5.4 if the fill closed the
  position (small Tier-1 seam).

## Tests (this package — golden-FIRST; spy Persistence + EventSink, real account/cost/realize)
- **`rapid` property for the core invariant:** across a partial-fill-then-cancel sequence,
  `reserved == exactly the unfilled qty` and debits/credits == exactly the filled qty, never
  overlapping.
- **`ReserveIfAvailable`:** buy reserves `best_ask × qty + buffer` (market) / limit cash; sell reserves
  units; insufficient → typed reason, **no** mutation.
- **`Convert` golden (hand-computed):** principal-then-cost ordering; brokerage charged **once** exactly
  at `RemainingQty == 0`.
- **`Release`:** partial-fill-then-cancel charges brokerage in `Release`, releases the remainder, no
  overlap; fully-unfilled cancel charges **zero** brokerage.
- **`ReleaseExcess`:** market-buy `MaxSpend` → excess released, `BrokerageCharged == true` assertion holds.
- Use **real** `account` / `cost` / `realize` (built earlier); **spy** `Persistence` (assert
  `MarkSnapshotDirty`) and `EventSink`. `-race` on, **single-threaded**, determinism replay.