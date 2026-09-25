# economy/persistence (Layer 5) — Money Persistence Component
## Spec (the authority — read in orientation, implement exactly, invent nothing)
- `docs/economy/LAYER_5_PERSISTENCE.md` — the whole doc (self-contained).
- `docs/economy/LAYER_5_2_SCHEMA.md` — fixes the three tables' columns / constraints.

## What persistence IS (one line)
The **imperative shell** for the money engine: economy's own persistence component,
sole owner of the SQL for its three durable tables, sole writer to Redis for the
snapshot mirror and the closed-trade write-behind buffer. It satisfies the
`Persistence` interface `reserve` / `realize` already call.

## Owns / public surface (Stages 1–5 landed)
- **`Component`** — the seam impl injected in place of Phase-1 no-op/spies.
  Holds the SQL layer, the dirty-account set, the drainer worker, and the
  injected `CountryCostAdapter`. (Stage 3–4.)
- **Seam methods** satisfying both `reserve.Persistence` and `realize.Persistence`:
  - `MarkSnapshotDirty(sessionID, accountID)` — flag only, no I/O.
  - `EnqueueClosedTrade(closed realize.ClosedTradeRow)` — translate + LPUSH.
- **Owner-facing API** (Phase 2 is synchronous, owner-driven):
  - `WriteSnapshot(ctx, SnapshotPayload) error`
  - `ReadSnapshot(ctx, sessionID, accountID)` — for read 16 + crash recovery.
  - `DeleteSnapshot(ctx, sessionID, accountID)` — on clean close.
  - `DrainDirty()` — the owner walks these keys and calls WriteSnapshot.
- **Boundary loaders:** `LoadAccount(ctx, id)` / `SaveAccount(ctx, DurableAccount)`.
- **Async drainer:** `Start(ctx) / Stop()` for the closed-trade drainer goroutine.
- **Flat DTOs (Stage 1):** `DurableAccount`, `TradeInsert`, `SnapshotPayload`,
  `CashSnap`, `PositionSnap`, `LotSnap`, `ReservationSnap`, `Config`.
- **Country adapter (Stage 1):** `CountryCostAdapter` interface + `IndiaCostAdapter`
  implementation. This is the ONE place India column names live; the generic
  persistence core carries an opaque `CostLines map[string]int64` on `TradeInsert`
  and never inspects the keys.
- **Errors:** `ErrNoAccount`, `ErrUnknownCostLine`.

## The seven locked decisions (LAYER_5_PERSISTENCE §8)
1. **Cost map → columns.** `realize.TradeCostRow.Lines` flattens to the country's
   trade_costs column layout via an injected `CountryCostAdapter`. India's adapter
   allowlists `{stt, exchange, sebi, stamp, gst, autosquare_fee}` and rejects
   anything else with `ErrUnknownCostLine` (never a silent drop). The generic
   `TradeInsert` DTO stays country-agnostic — no India column ever appears as a
   fixed struct field. When US arrives (schema §4 forward seam), a `USCostAdapter`
   plugs into the same interface with SEC/TAF/FINRA columns — zero change to the
   generic core.
2. **`brokerage` column.** `TradeCostRow` carries no brokerage (order-level, separate
   path). India's adapter emits `brokerage = 0` as a fixed column value.
3. **`SnapshotPayload` home.** Defined in **this** package. Owner imports it to assemble.
4. **Buffer mechanism.** Redis LIST (`LPUSH` / `RPOP`), single drainer.
5. **Drainer cadence + failure.** Tick + max batch, both `Config`-defaulted (100ms / 64).
   Retry with backoff; failed row **stays in the buffer**; idempotent replay via
   `ON CONFLICT (trade_id) DO NOTHING`.
6. **Dirty-tracking.** Plain `map[dirtyKey]struct{}`, **no lock** (Phase 2 is owner-driven).
7. **`saveAccount` reconciliation.** `lifecycle` reconciles the split → signed
   `trading_cash` **before** calling saveAccount. Persistence just writes.

## Layering — SQL layer sees NO domain type
```
reserve / realize  ──calls──▶  Component  ──translates──▶  store (SQL)  ──uses──▶  connectivity
                                (seam impl)               (primitives only)         (pool/client)
```
- The **SQL layer** (`store`, Stage 2) takes/returns only primitives + flat DTOs. Table
  names, column lists, and Redis key strings live here and NOWHERE else.
- The **seam methods** (Stage 3) are the one place a `realize.ClosedTradeRow` is named
  and unpacked into a `TradeInsert`.
- The **drainer** (Stage 4) is the only async worker. The snapshotter is
  synchronous / owner-driven in Phase 2 — no goroutine.

## Concurrency (Phase 2)
- **Snapshotter: synchronous.** `MarkSnapshotDirty` flags the map; `WriteSnapshot`
  writes Redis. Both run on the owner goroutine. No worker goroutine.
- **Drainer: async.** `EnqueueClosedTrade` LPUSHes and returns fast; a background
  goroutine (started via `Start(ctx)`) drains to Postgres. Idempotent via `ON CONFLICT`.
- `go test ./... -race` stays green — the drainer owns its local state; the pool /
  redis client are thread-safe by contract.

## Encoding (configurable; Stage 1 delivers the codec)
- `TimeEncoding`: `TimeRFC3339` (default, redis-cli friendly) or `TimeUnixNano`.
- `SideEncoding`: `SideString` (default, "buy"/"sell") or `SideInt` (0/1).
- Symmetric: write and read use the same setting. One `Config` value, one codec, one
  round-trip test matrix.

## Deps & seam
- **Imports:** `connectivity` (Postgres pool + Redis client — injected values), pgx,
  go-redis, `economy/realize` (for `ClosedTradeRow` / `SessionID`), `economy/account`
  (`AccountID`, `Lot` shape), `contract` (ids, `Side`), stdlib.
- **Does NOT import** `reserve` (satisfied structurally; the compile-time guard lives
  in test-only code), `cost`, `lifecycle`.
- **Never reads env; never opens/closes handles.** Wiring root does that via
  `connectivity/db`.

## Do NOT (in this package)
- Never do compute/money logic (no realization math, no reservation logic, no cost
  computation, no `Account` mutation, no split reconciliation — D7).
- Never issue DDL / migrations (Stage 2 of the build owns those).
- Never write logs at anything but boundaries (loadAccount / saveAccount /
  insertClosedTrade / snapshot write / drainer tick + error / Start/Stop).
- Never log a DSN, URL, or password.
- Never let a `realize.ClosedTradeRow` leak into the SQL layer — the ONE translation
  hop lives in the seam method (`EnqueueClosedTrade`).
- Never let a country's column names or cost-line strings appear anywhere OUTSIDE
  that country's adapter file (`india.go`). Generic types (`TradeInsert`,
  `translate.go`, `Component`, SQL layer) MUST stay country-agnostic — the ONLY
  file with India column names is `india.go`.
- Never gate on `PersistOnClose` here — that gate lives in `realize.OnSellFill`.
  Persistence has no on/off knob.

## Flag in orientation (do not silently resolve)
- **`SnapshotPayload` reservation fidelity.** §7 sketches a summary shape; we persist
  the FULL `ReservationRecord` fields (incl. `OrigQty`, `EverFilled`,
  `BrokerageCharged`) because a partial payload cannot rehydrate the once-per-order
  brokerage guarantee. Confirmed with user (deviation from §7 verbatim shape).
- **Close-accumulator not snapshotted.** A canonical crash mid round-trip loses partial
  P&L accumulation — deferred to the broker funnel's joint snapshot pass.
- **`AccountLedger` duplication.** `reserve` and `realize` each declare an identical
  narrow read handle. Persistence does not need one — it takes `SnapshotPayload` from
  the owner (D3). No third redeclaration here.

## Tests (this package)
Pure unit tests (translate, codec, keys, config, adapter, backoff). Integration
tests against `l5_test`:
- **Postgres:** transaction-rollback per test (nested savepoints let `store.withTx`
  work the same way); assert signed `trading_cash` round-trip, every constraint
  trace, idempotent `insertClosedTrade`.
- **Redis:** namespaced test keyspace flushed per test; assert three-key-group
  snapshot faithfulness (incl. FIFO `open_lots` order), stale-key cleanup, buffer
  FIFO, drainer idempotency, "failed insert leaves the row in the buffer."
- **Drainer lifecycle:** `Start(ctx)`/`Stop()` clean shutdown, double-Start error,
  idempotent Stop, ctx cancellation exits the goroutine, end-to-end goroutine drain.
- **Smoke:** one end-to-end scenario through the public `Component` API exercises
  BOTH required paths (enqueue → buffer → drain → Postgres, and dirty → snapshot →
  Redis) plus the boundary loaders' round-trip — `smoke_test.go`.
- **Commit-mode inspection:** set `PERSISTENCE_COMMIT_TEST_DATA=1` to have the
  outer test tx COMMIT instead of rollback; rows land in the DB for manual psql
  inspection (constraint-violation tests fall back to rollback automatically).
- `go test ./... -race` stays green.
