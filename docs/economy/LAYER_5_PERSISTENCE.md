# Layer 5 — `economy/persistence` **Component** (Phase 2, Stage 3)

<!-- SOURCES: LAYER_5_PERSISTENCE.md §3 (tables — canonical DDL, already realized in the
     schema migrations), §4 (Redis snapshot keys + write-behind buffer, transcribed below),
     §5 (load/run/save). The realize seam types (ClosedTradeRow / ParticipantTradeRow /
     TradeCostRow) are as defined in economy/realize. Where this doc and LAYER_5_PERSISTENCE.md
     disagree on contents, LAYER_5_PERSISTENCE.md wins. What this doc ADDS is the package's Go
     surface, the worker model, and the seven locked decisions — none of which change the
     schema or the snapshot contents. This merges the previously-separate `store` (Stage 3)
     and `persist` (Stage 4) plans into ONE economy-owned package. -->

## 0. Scope & standing

`economy/persistence` is **economy's own persistence component** — it holds economy's SQL for
economy's three tables, drives the async snapshot/write-behind workers, and satisfies the
`Persistence` interface that `reserve`/`realize` already call. It is the imperative shell for the
economy money engine.

- **Not shared, not generalized.** It knows economy's tables and only economy's. If exchange or
  broker later need durable state, they get their **own** `exchange/persistence` /
  `broker/persistence`. The **only** thing shared across components is `connectivity` (pools + config).
- **Uses `connectivity` directly.** It takes the injected `*pgxpool.Pool` and `*redis.Client`; it does
  not open connections, read env, or manage pools (that's `connectivity`).
- **Imports:** `connectivity`, pgx, go-redis, `economy/realize` (for `ClosedTradeRow` / `SessionID`),
  `economy/account` (for `AccountID` and the durable-account shape), `contract` (ids), stdlib. Nothing
  imports it except the wiring root (and, indirectly, whoever holds the `Persistence` value).

## 1. Package shape — three concerns, one package

Layered internally so the SQL never touches a domain type:

```
reserve / realize  ──calls──▶  Persistence impl  ──translates──▶  SQL layer  ──uses──▶  connectivity
                                (§4, domain-aware)                 (§3, primitives only)   (pool/client)
        async workers (§5) ─────────────────────────────────────▶ SQL layer
```

- **SQL layer (§3)** — the "hands": synchronous, dumb query methods. Table names and column lists live
  **here and nowhere else**. Sees only primitives / flat DTOs — never `account.Account`,
  `realize.ClosedTradeRow`, etc.
- **Async workers (§5)** — the "brain": the drainer and snapshotter goroutines. Timing, batching,
  retry.
- **`Persistence` impl (§4)** — the seam the compute packages call. The **one** place a
  `realize.ClosedTradeRow` is named and unpacked into primitives for the SQL layer.

## 2. Tables it owns (reference — DDL is in the schema doc, not restated)

`accounts`, `participant_trades`, `trade_costs` — per `LAYER_5_2_SCHEMA.md`. This component binds its
SQL to exactly those columns/constraints; it never issues DDL (migrations own that).

## 3. The SQL layer (synchronous, primitives only)

Struct `store` holding the injected `*pgxpool.Pool` + `*redis.Client`. Methods (illustrative Go
shapes; ctx on every call):

**Postgres**
- `loadAccount(ctx, accountID string) (DurableAccount, error)` — SELECT the `accounts` row; `ErrNoAccount`
  when absent (never create here). One row, session-start only.
- `saveAccount(ctx, DurableAccount) error` — UPDATE the `accounts` row (signed `trading_cash`, `bank_cash`,
  drip fields, `updated_at`), one tx. Canonical close only. The **caller has already reconciled** the
  live split to one signed figure (see D7).
- `insertClosedTrade(ctx, TradeInsert) error` — INSERT `participant_trades` **and** `trade_costs` in **one
  tx**, **idempotent** via `ON CONFLICT (trade_id) DO NOTHING` on both. `TradeInsert` is flat primitives.

**Redis** (key shapes verbatim from §7 below)
- `writeSnapshot(ctx, SnapshotPayload) error` — write the three key groups for one account.
- `readSnapshot(ctx, sessionID, accountID string) (SnapshotPayload, error)` — read them back (crash
  recovery + read 16).
- `deleteSnapshot(ctx, sessionID, accountID string) error` — on clean save/discard.
- `pushClosedTrade(ctx, TradeInsert) error` — `LPUSH` onto `l5:buf:trades`.
- `drainClosedTrades(ctx, max int) ([]TradeInsert, error)` — `RPOP` up to `max` (see D4/D5).

## 4. The `Persistence` impl (the seam)

Concrete type satisfying **both** `reserve`'s interface (`MarkSnapshotDirty` only) and `realize`'s
(`MarkSnapshotDirty` + `EnqueueClosedTrade`) — structural satisfaction; the impl imports `realize`, not
`reserve`.

- `MarkSnapshotDirty(sessionID, accountID)` — **flags the account dirty; no I/O.** Records the pair in
  the dirty set (D6). The snapshotter acts on it later. This is what keeps snapshotting off the hot path.
- `EnqueueClosedTrade(closed realize.ClosedTradeRow)` — the **one translation hop**: unpack
  `closed.Summary` (→ the `participant_trades` columns) and `closed.Cost` (→ the flattened `trade_costs`
  columns, D1) into a flat `TradeInsert`, then `pushClosedTrade` (buffer, return fast). Never inserts to
  Postgres inline.

## 5. The async workers

- **Drainer** — a goroutine: on a tick (D5), `drainClosedTrades(max)` → `insertClosedTrade` for each,
  idempotent. On Postgres error: backoff and retry; the row **stays in the buffer** until it lands
  (durable in Redis). On shutdown: stop the tick, drain what's cheaply drainable, then exit — anything
  left rides the buffer to the next boot (D5).
- **Snapshotter** — mirrors dirty accounts to Redis via `writeSnapshot`. **Phase 2: synchronous,
  owner-driven** — the owner (or, in tests, the harness) assembles a `SnapshotPayload` from the live
  `Account` + `ReservationBook` and calls `writeSnapshot`. `MarkSnapshotDirty` flags the map; the owner
  walks `DrainDirty()` on its own cadence. No async cadence loop is built in this package — the
  concurrent-safe capture off the owner goroutine is **deferred to the broker funnel's joint pass**.
- Only the drainer exposes `Start(ctx)` / `Stop()`; the snapshotter has no lifecycle (it runs
  synchronously on the owner).

## 6. Data types (flat DTOs — defined in this package)

Primitives only, so the SQL layer names no domain type:
- `DurableAccount{ AccountID, OwnerType, OwnerID string; TradingCash, BankCash int64; DripActive bool;
  DripProgress *int64 }` — mirrors the `accounts` row (`DripProgress` nullable).
- `TradeInsert` — the flattened `participant_trades` columns (universal) + `CostLines
  map[string]int64` + `CostTotal int64` for the `trade_costs` payload. The map stays opaque in the
  DTO; the injected `CountryCostAdapter` (D1) flattens it to the country's columns at INSERT time.
- `SnapshotPayload{ SessionID, AccountID string; Cash CashSnap; Positions []PositionSnap;
  Reservations []ReservationSnap }` — plain values the owner assembles; **defined here** (D3). `CashSnap`
  = the live split + bank + drip; `PositionSnap` = units + `open_lots`; `ReservationSnap` = the 5.2 record
  fields.

## 7. Redis keys & buffer (verbatim from LAYER_5_PERSISTENCE.md §4)

```
l5:snap:{session_id}:acct:{account_id}:cash  = { available, reserved, settled, bank_cash, drip_active, drip_progress }
l5:snap:{session_id}:acct:{account_id}:pos:{instrument_id} = { available_units, reserved_units, open_lots:[{entry_price, entry_at, remaining_qty}, ...] }
l5:snap:{session_id}:acct:{account_id}:resv:{order_id} = { order_id, account_id, session_id, instrument_id, side, is_market, reserved_cash, reserved_units, remaining_qty, orig_qty, ever_filled, brokerage_charged }
l5:buf:trades  = LIST of pending participant_trades (+ trade_costs) rows
```
- `open_lots` is FIFO arrival order — one structure for the position, `position_age`, and lot-matching.
- The resv shape carries the **full record** (not the sketch); `orig_qty` / `ever_filled` /
  `brokerage_charged` are needed to rehydrate reserve's once-per-order brokerage guarantee.
- **Snapshot is discarded on clean close** (a within-session safety net, not a record). On-demand
  sessions are snapshotted but never write back to Postgres.
- **Absent by design:** unrealized marks (read-time), closed trades (durable via the buffer), the durable
  resting balance (Postgres `accounts`).

## 8. Locked decisions (the seven — resolved as the defaults)

1. **Cost map → columns.** `realize.TradeCostRow.Lines map[string]int64` flattens to the country's
   `trade_costs` columns via an **injected `CountryCostAdapter`** (India's adapter allowlists
   `{stt, stamp, exchange, sebi, gst, autosquare_fee}`; a US adapter would plug into the same
   interface with SEC/TAF/FINRA columns — zero change to the generic core). An **unrecognized key is
   an error, not a silent drop** — a cost line with no column is a bug and must surface. `Total` maps
   to `trade_costs.total`.
2. **`brokerage` column.** `TradeCostRow` deliberately carries no brokerage (order-level, separate path).
   On insert, `trade_costs.brokerage = 0` for now; the real order-level brokerage persistence is a later,
   separate path. `participant_trades.total_costs` and `trade_costs.total` come from the trade's own cost
   stack (brokerage excluded), consistent with 5.4.
3. **`SnapshotPayload` home.** Defined **in `economy/persistence`**. The owner imports this package to
   assemble it. (It's a persistence concern; keeping it here avoids bloating `contract`/`realize`.)
4. **Buffer mechanism.** Redis **LIST** (`LPUSH` / `RPOP`), single drainer. Not a STREAM — consumer
   groups buy nothing at one drainer; revisit only if multiple drainers ever exist.
5. **Drainer cadence & failure.** Tick interval + max batch per drain, both config-defaulted. Retry with
   backoff on Postgres error; a failed row **stays in the buffer** (idempotent re-insert via `ON CONFLICT`
   makes replay safe). At shutdown, un-drained rows are **left in the buffer** for the next boot — the
   buffer is durable in Redis, so nothing is lost.
6. **Dirty-tracking.** A `map[dirtyKey]struct{}` (key = sessionID+accountID). Phase 2 it's owner-driven /
   synchronous, so **no lock**; when the funnel arrives the owner serializes access. Do not over-build a
   concurrent structure now.
7. **`saveAccount` reconciliation.** The live split (`Available`/`Reserved`/`Settled`) reconciles to one
   signed `trading_cash` **in `lifecycle`** (Stage 5, which owns session end) **before** it calls
   `saveAccount`. Persistence just writes the final figure — it stays dumb, does no money reconciliation.

## 9. Layering & dependency rules

- **SQL layer sees no domain type** — primitives/DTOs only, even inside this one package. The single
  translation (`realize.ClosedTradeRow` → `TradeInsert`) happens at the `Persistence` impl, one hop.
- Imports `connectivity` (handles), `realize`/`account`/`contract` (seam types), drivers, stdlib. Imports
  **no** `reserve` (satisfied structurally), no `cost`, no `lifecycle`.
- Takes the pool/client **injected**; never constructs them.

## 10. What it does NOT do

- No compute/money logic — no reservation or realization math, no `Account` mutation, no cost
  computation, no split reconciliation (D7).
- No DDL / migrations (Stage 2 owns those); no config, pooling, or env reading (connectivity owns those);
  no reconnect/health-loop (connectivity's startup ping is the bar).
- No read-surface / query API (that's C12 / broker Tier 4). This component's reads are only what load /
  crash-recovery / snapshot need.

## 11. Testing

Integration against the `l5_test` database (provisioned by the Stage-2 migrations):
- **Postgres tests: transaction-rollback per test** — each test runs in a tx that rolls back; no manual
  truncation.
- **Redis tests: a namespaced test keyspace, flushed per test** (Redis has no tx-rollback).
- **Assert:** `loadAccount`→`saveAccount` round-trips the **signed** balance (incl. negative
  `trading_cash`); `ErrNoAccount` on a missing row; snapshot `write`→`read` is faithful across all three
  key groups (incl. `open_lots` FIFO order); the drainer is **idempotent** (drain the same row twice → one
  Postgres row via `ON CONFLICT`); a failed insert leaves the row in the buffer. (The "on-demand path
  writes nothing to Postgres" assertion belongs to `lifecycle` — the `PersistOnClose` gate lives
  there, not here.)
- **Unit (no DB):** the D1 map→columns flattening — correct columns for a known India stack, **error on an
  unknown key**; the `ClosedTradeRow`→`TradeInsert` translation; dirty-flagging; enqueue→drain wiring
  (with a fake/real Redis).
- `go test ./... -race` stays green; the workers' `Start`/`Stop` tested for clean shutdown.

## 12. Definition of done

- `economy/persistence` builds and its SQL binds to the exact Stage-2 columns/constraints.
- `Persistence` impl satisfies both `reserve`'s and `realize`'s interface; injected at the wiring root in
  place of the Phase-1 no-op (compute packages change **zero** lines).
- Drainer + snapshotter start/stop cleanly; the end-to-end path (enqueue → buffer → drain → Postgres, and
  dirty → snapshot → Redis) works against `l5_test`.
- The SQL layer names no domain type; connectivity is the only shared dependency; local→cloud Postgres/Redis
  is still an env change only (inherited from connectivity).

## 13. Open flags (surface in orientation; do not silently resolve)

- **`SnapshotPayload` field types** must match what the owner can actually hand over from the live
  `Account` + `ReservationBook` — confirm against those structs during orientation (esp. `open_lots` and
  the reservation-record fields).
- **Close-accumulator** is **not** in the snapshot (per §4.1 / 5.4's deferral) — a canonical crash mid
  round-trip loses the partial accumulation. Accepted for the prototype; flagged.
- **Timestamp/enum encoding** in Redis JSON (e.g. `entry_at`, `side`) — **Resolved:** `Config.TimeEncoding`
  (default `TimeRFC3339`, or `TimeUnixNano`) and `Config.SideEncoding` (default `SideString`, or
  `SideInt`); encode/decode symmetric.
