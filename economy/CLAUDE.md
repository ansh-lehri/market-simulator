# Economy (Layer 5) — Money-Truth Constitution
<!-- economy/CLAUDE.md: loaded for every session under economy/. Adds the L5-specific
     rules ON TOP of the root constitution; it does not repeat the root, which still
     applies in full. Each package's own CLAUDE.md adds that component's public surface
     and its verbatim invariants. Target < 180 lines. -->

Economy is **Layer 5 — the broker's money engine**: the sole writer of *money truth*,
in memory, running **on the broker's owner goroutine**. It is pure hands + a thin brain
over one account's state. It has **no exchange knowledge, no wire types, and no
goroutine of its own** — it *assumes* the single-writer, it does not create it (the
broker funnel does, later). Money persistence is done at boundaries, asynchronously,
through one seam — never inline on the hot path.

## What economy owns — and what it must NOT do
- **Owns:** the `Account` model, the reservation book, cost math, realization/P&L, and
  the L5 lifecycle flows (load / seed / save / discard).
- **Does NOT:** translate `FundedOrder` / `EngineResult` — that is the **broker tier's**
  job; economy speaks `OrderID` + `Side` + `Qty` + integer price only. Does not read the
  book or any market truth. Runs no HTTP and spawns no goroutine. Does not *enforce*
  single-writer — it **relies** on it.

## The account model (the shape every package shares)
- `Account{ Cash, Positions map[InstrumentID]*Position, PersistOnClose bool }`.
- `Cash{ Available, Reserved, Settled, Bank int64 }` — integer coins.
  **`Available` is signed (Option B)** — it may go negative, and that negative *is* debt.
  Never clamp it, never gate it to zero.
- `Position{ Available, Reserved int64, OpenLots []Lot }` — **session-scoped, born
  empty, NEVER persisted to Postgres** (Redis snapshot only), auto-squared flat at close.
- **The split (`Available/Reserved/Settled`) lives only in memory + its Redis mirror.
  Postgres holds ONE signed `trading_cash`** the split reconciles to. Never write a split
  to Postgres.

## The two coupling invariants (asserted everywhere — panic on violation)
- **Cash conservation:** a bucket→bucket transfer never changes the total. Sum before ==
  sum after every `MoveCash` / `MovePosition`.
- **Position↔lot coupling:** `Position.Available + Position.Reserved == Σ Lot.RemainingQty`.
- Both are `panic`-on-violation (dev-fatal, per root) AND a `rapid` property test in every
  package that can touch them.

## Direction is the primitive, never the sign (root rule, pinned to the 6 hands)
- Six write hands; direction fixed by *which hand* + *which buckets*; amount args strictly
  positive: `MoveCash` / `MovePosition` (conserving transfers), `CreditCash` / `DebitCash`
  (mint / burn cash), `CreditPosition` / `RemovePosition` (mint / burn units).
- `RemovePosition` **returns the FIFO-matched lots** for 5.4 — it *matches*, it never does
  money math.
- `DebitCash` is the **only** door debt enters (drives signed `Available` negative, no gate).

## The reservation invariant (5.2)
- **Reserved covers exactly the unfilled qty; debits/credits cover exactly the filled qty;
  the two never overlap.** Hold this across partial fill, cancel, and rejection.
- **Brokerage is order-level, charged ONCE** at `RemainingQty == 0` (terminal), via the
  `EverFilled` / `BrokerageCharged` / `OrigQty` flags on the reservation record — excluded
  from every per-trade cost row.
- The reservation book is **separate from `Account`**, whole-economy scope, keyed by
  `OrderID`, each record carrying its own `AccountID` / `SessionID` (link-back, never a
  pointer into an `Account`).

## Persistence — the seam, and the two phases  ← READ BEFORE WIRING ANY PERSISTENCE
Money persistence is **async, through ONE injected seam** — never inline SQL, never a
synchronous store write on the hot path:

```go
type Persistence interface {
    // Async, non-blocking. Flag the account's live state dirty so the background
    // snapshotter mirrors {cash, positions, reservations} to Redis on its cadence.
    MarkSnapshotDirty(sessionID SessionID, accountID AccountID)

    // Async. On a round-trip close, push the closed-trade row(s) to the Redis buffer;
    // the drainer moves them to Postgres later. Gated by PersistOnClose (5.4).
    // EXACT arg list: take it from the doc — 5.2 §1.3 and 5.4 show slightly different
    // shapes (single row vs row + cost row). Reconcile in orientation; do not guess.
    EnqueueClosedTrade(/* per doc */)
}
```

- **Pure — hold NO `Persistence`:** the 5.1 hands and 5.3 cost. Zero I/O, zero snapshot
  marks; a separate snapshotter *reads* their state. A persistence call in these is a bug.
- **Carry a `Persistence` field and call the seam:** 5.2 `reserve` (`MarkSnapshotDirty` at
  the end of Reserve / Convert / Release) and 5.4 `realize` (`EnqueueClosedTrade` on a
  position-flat flush, gated by `PersistOnClose`).
- **Phase 1 (build now): inject a no-op / in-memory spy `Persistence`.** Methods run and
  test fully in memory; assert the seam was called with the right account / row. No DB.
- **Phase 2: build the real impl BEHIND THE SAME SEAM** — the snapshotter (dirty state →
  Redis) and the write-behind drainer (buffer → Postgres). **The compute methods change
  zero lines** between phases.
- The real snapshotter / drainer use **pgx/pgxpool (Postgres) and the Redis client
  DIRECTLY — no `Store`-interface wrapper.** `Persistence` is the *only* economy
  abstraction; do not add another. Schema names / Redis keys come **verbatim** from
  `LAYER_5_PERSISTENCE.md`.

## Reads — Redis-first for the snapshot, in-memory for the hot path
- **Hot path (14 `ReadAvailableCash` / 15 `ReadAvailablePosition`): in-memory only.** No
  store hop per order, ever. 14 returns the raw signed value; 15 returns ≥0 (missing
  instrument → 0).
- **Session snapshot (16 `read_balance_snapshot`): Redis**, seeded ONCE from Postgres at
  `session_load`. **Postgres is read only at the boundary** (`load_account`, session
  start), never on the hot path.
- **DEAD MODEL — do not implement:** the old `LAYER_5_1_OPERATIONS.md §2` read section
  (Postgres-seeded reads) is removed from the doc. The read model is
  `LAYER_5_1_READ_OPERATIONS.md`. If any leftover index row still tags item 16 as
  "Postgres", ignore it — 16 reads Redis.

## PersistOnClose — the one canonical-vs-on-demand switch
- A single bool on `Account`, set at load (`true` canonical, `false` on-demand). It is the
  **only** place economy differs by mode. `realize` gates its enqueue on it; nothing else
  branches on "mode". On-demand computes and displays P&L but **never writes back**.

## Testing (economy-specific — on top of the root policy)
- **Pure packages (`account`, `cost`): table-driven units + `rapid` property tests** for
  the two invariants; hand-verified golden cases for cost (India: ₹20-once, GST split,
  STT sell-only / stamp buy-only).
- **`reserve` (5.2) + `realize` (5.4): golden-FIRST** — hand-compute the expected ledger
  from the doc *before* coding (partial-fill-then-cancel, brokerage-once, market-buy
  `MaxSpend` → `ReleaseExcess`; FIFO multi-lot realize, weighted-avg vs FIFO on a partial
  close, one `participant_trades` row per round-trip, auto-square CST-5). Inject the
  **no-op / spy `Persistence`** and assert the seam calls.
- **`store` + `lifecycle`: integration tests against the live TEST DB** — a dedicated
  `l5_test` database/schema, **never** dev data — with **transaction-rollback per test** so
  each self-cleans (no manual truncation). Assert the Redis-first read model explicitly
  (Postgres→Redis seeded once; snapshot from Redis; hot-path from memory).
- **All economy tests run SINGLE-THREADED — no goroutines.** Economy assumes single-writer;
  the funnel enforces it later, so there is nothing concurrent to test here. `-race` stays
  on (it proves no accidental sharing). Add a **determinism check** (replay the same call
  sequence → identical ledger) for `reserve` / `realize`.

## Doc map (the spec for each package — read the right one in orientation)
- `account` (5.1) → `LAYER_5_1_OPERATIONS.md` **§0–§1 only** + `LAYER_5_1_READ_OPERATIONS.md` (reads 14/15)
- `cost` (5.3) → `LAYER_5_3_COST_ENGINE.md`
- `realize` (5.4) → `LAYER_5_4_REALIZATION.md`
- `reserve` (5.2) → `LAYER_5_2_RESERVATION_FLOWS.md`
- `store` → `LAYER_5_PERSISTENCE.md`
- `lifecycle` → `LAYER_5_2_LIFECYCLE_FLOWS.md` + `LAYER_5_1_READ_OPERATIONS.md` (read 16)
- overview → `LAYER_5_ECONOMY.md`
<!-- CANONICITY: if LAYER_5_2_METHOD_FLOWS.md is present, RESERVATION_FLOWS + LIFECYCLE_FLOWS
     are canonical for 5.2. Do NOT implement from METHOD_FLOWS without confirming first. -->

## Build dependency order (leaves first)
`account` → `cost` → `realize` (needs account + cost) → `reserve` (needs account + cost +
realize) → **[schema]** → `store` → `lifecycle` (needs account + store). Debt (5.5) is
folded into `account` + `reserve` as properties/tests — **there is no `debt` package.**