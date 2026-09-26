# economy/lifecycle (Layer 5.2 Tier-2) — Session Lifecycle Flows
<!-- Per-package CLAUDE.md. READ ORDER for any session working in this package, all apply
     together:
       1) /CLAUDE.md            — root project constitution
       2) /economy/CLAUDE.md    — money-truth (L5) constitution
       3) THIS file             — what is specific to lifecycle
       4) the spec docs below   — the authority for behavior
     Does not repeat the two constitutions; pins them to this package. -->

## Spec & authority
- `docs/economy/LAYER_5_2_LIFECYCLE_FLOWS.md` — the six flows (18-23): signatures, step logic,
  ordering, reconciliation, guards.
- `docs/economy/LAYER_5_1_READ_OPERATIONS.md` — **item 16 `read_balance_snapshot` ONLY** (its section 5).
- **`economy/persistence` (the built `Component`) is the AUTHORITY for the store surface and the
  snapshot shape.** Where the lifecycle doc's illustrative SQL / Redis-key sketch (its section 1)
  differs from the persistence code, **the code wins** — persistence owns those shapes; lifecycle
  consumes them through the Component's methods.

## What lifecycle IS (one line)
Tier-2 **orchestration**: it *establishes, persists, and restores* the in-memory `account.Account`
across session boundaries by **calling `economy/persistence.Component`**. It decides sequence and
policy; it holds **no SQL** and touches **no driver**.

## THE BRIDGE (the doc predates the persistence split - read before any flow)
`LAYER_5_2_LIFECYCLE_FLOWS.md` shows inline SQL and `db DB, rds Redis` params. That is
**illustrative of what `persistence.Component` does - NOT what lifecycle codes.**
- Lifecycle holds a **`*persistence.Component`** and calls its exported methods -
  `LoadAccount`, `SaveAccount`, `WriteSnapshot`, `ReadSnapshot`, `DeleteSnapshot`. It runs **no raw
  SQL**, imports **no pgx/redis**, defines **no new `DB`/`Redis` interface**.
- The doc's `db DB, rds Redis` = that one `*Component`. Use the concrete economy-owned type; do not
  abstract it.
- Lifecycle owns the **policy** the doc describes (ordering, guards, reconcile math, "don't discard
  snapshot on failed save"); persistence owns the **store ops** underneath.

## Owns / public surface (six flows + read 16)
- `open_account` (18) - durable birth: `Component`-mediated `accounts` INSERT, config-sourced starting
  capital, `ErrNoAccount` never created here. Postgres only; no Redis, no session.
- `session_load` (19, canonical) - `LoadAccount` once -> **hydrate the fresh split** -> build `Account`
  -> assemble a `SnapshotPayload` -> **seed Redis once** (`WriteSnapshot`).
- `ondemand_session_load` (20) - isolated, `PersistOnClose=false`; teardown is discard, never save.
- `session_save` (21, canonical) - reconcile -> `SaveAccount` (one tx) -> `DeleteSnapshot`.
- `session_discard` (22, on-demand) - `DeleteSnapshot`; **never a Postgres write**.
- `crash_recovery_reload` (23) - restore the split + positions + reservation book from `ReadSnapshot`
  (**L5 slice only** - see flags).
- `read_balance_snapshot` (16) - a **thin wrapper over `persistence.ReadSnapshot`**: read the session
  snapshot from Redis, return `*account.Cash{Available, Reserved, Settled, Bank}` from
  `SnapshotPayload.Cash`. Missing key -> `ErrSnapshotMissing` (hard stop, never fabricate a balance);
  Redis failure -> `ErrLoadFailed`. Not hot-path. (Hot-path reads 14/15 are in `account`, in-memory.)

## Load-bearing rules (verbatim from the doc)
- **Hydration (load, section 3):** Postgres holds one signed `trading_cash`; the fresh split is
  synthesized **once, here** - `Available = trading_cash`, `Reserved = 0`, `Settled = 0`,
  `Bank = bank_cash` - then written to Redis via `WriteSnapshot`. **Never re-synthesized** (the split
  then evolves in memory; `crash_recovery_reload` *restores* an evolved split, it does not
  re-synthesize).
- **Reconciliation (save, section 5, Decision-7):** at clean close the session is flat, so
  **`Reserved == 0` (assert; panic if not)** and **`trading_cash(durable) = Available + Settled`**.
  `bank`/drip carry over. This is the ONLY money math lifecycle does.
- **Ordering (load-bearing):** auto-square (5.4) -> trades flushed -> **THEN `session_save`**. Save is
  the last teardown step; asserts flat (`Reserved==0`, no live position -> **panic**).
- **On-demand guard:** `session_save` on `PersistOnClose==false` -> `ErrNotCanonical` (refuse; never
  corrupt the durable account). On-demand routes to `session_discard`.
- **Don't discard on failed save (section 5 step 5):** if `SaveAccount` errors, **do NOT**
  `DeleteSnapshot` - the snapshot is the only surviving copy. Postgres-commit **before** Redis-discard,
  always. Discard failure is non-fatal (TTL cleans stale keys).
- **`trading_cash` is signed** - write it as-is on save; **no floor, no clamp** (Option 4).

## Deps & seam
- **`account`** - builds/reads the `Account` (hydration target); the split fields; `read_16` returns
  `*account.Cash`.
- **`economy/persistence`** - the concrete `*Component`; **all** load/save/snapshot/read go through it.
- **`contract`** - id types. Imports **no** pgx/redis, **no** `reserve`/`cost`, **no** driver.
- Lifecycle is **above** persistence (imports it); persistence never imports lifecycle.

## Do NOT (in this package)
- No raw SQL, no pgx/redis import, no new `DB`/`Redis` interface - route through the `Component`.
- No money/compute math beyond the hydration synthesis and the save reconciliation above.
- No per-trade persistence - closed trades go `realize -> persist.EnqueueClosedTrade` during the
  session; lifecycle is session **boundaries** only.
- No position persistence to Postgres (positions are gone by save-time; never durable).
- No goroutine/mutex - single-threaded (single-writer assumed); flows run off the hot path.

## Flags (surface in orientation; do not silently resolve)
- **`crash_recovery_reload` (section 7) is PARTIAL - coordination OPEN.** Build the **L5 slice** only:
  rebuild cash split + positions + reservation book from `ReadSnapshot`. The
  `SnapshotPayload.Reservations` carry the **full record** (`OrigQty`/`EverFilled`/`BrokerageCharged`),
  so the reservation book - including the brokerage-once guarantee - rehydrates from it. The cross-layer
  coordination (engine book rebuild, L2 re-attach) and the **close-accumulator** restore are
  **deferred to the funnel's joint pass** - implement to the L5 boundary, flag the rest, do not invent
  it.
- **read-16 error mapping** - confirm `persistence.ReadSnapshot` distinguishes a **missing key** from a
  **transport failure**, so `read_balance_snapshot` can map them to `ErrSnapshotMissing` vs
  `ErrLoadFailed` per item-16's contract. If it collapses them, that's a small persistence-side check
  to raise (do not paper over it in lifecycle).

## Tests
- **Unit (no DB) for the pure logic:** the hydration split synthesis; the save reconciliation
  (`Available + Settled`, `Reserved==0`); the flat-precondition **panics** (reserved cash / unsquared
  position at save); the on-demand `ErrNotCanonical` guard; the `read_16` payload->`*Cash` mapping and
  its two error branches (with a fake Component).
- **Integration through the REAL `persistence.Component` against `l5_test`** (tx-rollback per test;
  namespaced Redis flushed per test): `open_account` -> `session_load` **seeds Redis and
  `read_balance_snapshot` reads it back**; a mutate -> `session_save` **round-trips the signed balance**
  (incl. negative) and discards the snapshot; **on-demand discard writes NOTHING to Postgres**; **a
  forced `SaveAccount` failure leaves the snapshot intact**.
- `-race` on; **single-threaded**; determinism where stateful.