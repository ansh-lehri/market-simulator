# economy/account (Layer 5.1) — Account State + the Hands
<!-- Per-package CLAUDE.md. READ ORDER for any session working in this package, all four
     apply together:
       1) /CLAUDE.md            — root project constitution
       2) /economy/CLAUDE.md    — money-truth (L5) constitution
       3) THIS file             — what is specific to the account package
       4) the spec docs below   — the authority for behavior
     This file does not repeat the two constitutions above; it only pins them to this
     package and states what is account-specific. -->

## Spec (the authority — read in orientation, implement exactly, invent nothing)
- `docs/economy/LAYER_5_1_OPERATIONS.md` — **§0 and §1 ONLY**: the account model (§0.4–§0.6)
  and the 6 write hands (§1.1–§1.6).
- `docs/economy/LAYER_5_1_READ_OPERATIONS.md` — the **2 hot-path reads (items 14 / 15) ONLY**.
- **DO NOT read or implement §2 of OPERATIONS.md** — it is the dead Postgres-seed read model
  (see /economy/CLAUDE.md). Item 16 `read_balance_snapshot`, `load_account`, and
  crash-recovery load are **NOT this package** — they belong to `store` / `lifecycle`.

## Owns / public surface
- **Types:** `Account`, `Cash{Available,Reserved,Settled,Bank}`,
  `Position{Available,Reserved,OpenLots}`, `Lot{EntryPrice,EntryAt,RemainingQty}`,
  and the `Bucket` / `PosBucket` enums. This package is the **home of the shared account
  model** — every other economy package imports these.
- **6 write hands:** `MoveCash`, `MovePosition` (conserving transfers); `CreditCash`,
  `DebitCash` (mint / burn cash); `CreditPosition`, `RemovePosition` (mint / burn units).
- **2 hot-path reads:** `ReadAvailableCash` (14), `ReadAvailablePosition` (15).
- `RemovePosition` **returns the FIFO-matched lots** (the cost basis 5.4 consumes) — it
  matches lots, it **never does money math**.

## Invariants (verbatim from OPERATIONS.md §0.6 — assert; panic on violation, never swallow)
1. **Cash conservation (transfers only):** a pure bucket-to-bucket `MoveCash` leaves the sum
   of all cash buckets unchanged. **Asserted in `MoveCash`.** (`CreditCash` / `DebitCash`
   break this on purpose — do **not** assert it there.)
2. **Position–lot coupling:** for every instrument,
   `Position.Available + Position.Reserved == Σ OpenLots[i].RemainingQty`. Preserved by
   `CreditPosition` / `RemovePosition` (both sides equally), untouched by `MovePosition`.
   **Asserted in `RemovePosition`.**
- Plus **each hand's own legal-move-set + sufficiency guard** (per §1.1–§1.6): enforce
  exactly as the doc states. A guard firing *after* the caller already proved the
  precondition is a **panic** (programming error), not a returned error.

## Account-specific value rules (general rules are in the two constitutions)
- `Available` is **signed (Option B)** — negative *is* debt. `DebitCash` is the **only** door
  debt enters: no floor, no clamp, no gate. Debt / 5.5 lives here as **tests + a documented
  property, not a package**.
- `Reserved`, `Settled`, `Bank`, and both `Position` buckets are **≥ 0**; only `Available`
  may go negative.

## Deps & seam
- **Leaf — no economy deps.** Must NOT import `cost` / `realize` / `reserve` / `store` /
  `lifecycle`. Borrows only the integer aliases from `contract/` if needed.
- **Holds NO `Persistence`, does ZERO I/O.** A separate snapshotter *reads* this state to
  mirror it; this package never writes a store, never marks a snapshot, never calls the seam.

## Do NOT (in this package)
- No I/O of any kind — no Redis, no Postgres, no file, no network; **no log inside a hand**
  (they must stay pure/replayable).
- No cost math (5.3), no realization / P&L (5.4), no reservation logic (5.2), no order or
  wire types (`FundedOrder` / `EngineResult` are the broker tier's).
- No `Persistence` call, no `MarkSnapshotDirty`, no `EnqueueClosedTrade`.
- No goroutine, no mutex — single-writer is **assumed** (the funnel enforces it later).
- Do not fold mint/burn into the transfer hands — keep the 6 primitives distinct (§1 rationale).

## Tests (this package — pure, no DB, no Persistence spy needed)
- **Table-driven units** per hand and per read: every legal move, and every guard rejection.
- **`rapid` property tests for BOTH §0.6 invariants** — fuzz sequences of hands, re-check the
  invariant after each op.
- Golden units for `RemovePosition`'s FIFO lot-matching (hand-computed multi-lot cases).
- `-race` on; **single-threaded** (no goroutines); a determinism replay check
  (same ordered hand calls → identical `Account`).