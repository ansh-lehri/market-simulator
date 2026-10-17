# broker/store — The Durable Store Layer

<!-- READ ORDER (all apply together):
       1) /CLAUDE.md                              — root constitution
       2) broker/CLAUDE.md                        — the broker (funnel) constitution
       3) THIS file                               — what the Store owns
       4) docs/broker/BROKER_PHASE1_IMPLEMENTATION_DESIGN.md  — the spec (§2 handle, §3 reads, §4 transfer, §5 consolidated)
          docs/broker/BROKER_PHASE2_IMPLEMENTATION_DESIGN.md  — Phase-2 supersession P2-1·D3 (Store gains redis + lifecycle); the Phase-2 Store surface additions (TradingCash + Lifecycle()); EnsureAccount's consumption of OpenAccount via the Store
          docs/broker/BROKER_READ_SURFACES_tier4.md          — the read designs these accessors serve (Phase 6 consumes) -->

## Spec & authority
`BROKER_PHASE1_IMPLEMENTATION_DESIGN.md` §2–5 is the Phase-1 spec.
`BROKER_PHASE2_IMPLEMENTATION_DESIGN.md` P2-1·D3 **supersedes** this file's older
"pool only, no Redis" rule (see the Phase-2 section below). The read designs trace to
the Tier-4 read-surfaces doc (Phase 6 builds the endpoints; Phase 1 built only these
accessors). **Code wins over doc:** the real `migrations/0001–0003` DDL and the
`economy/persistence` source (the drainer's insert SQL, `india.go` cost columns) are
the truth for every column/type — where a doc sketch diverges, follow the built schema
and **flag it**.

## What it IS (one line)
The broker-owned, **shard-independent** durable layer: its **own** Postgres pool, its
**own** Redis client, and an **unstarted** `economy/lifecycle.Service` — hosting every
**off-shard** durable operation (the five Phase-1 reads + the Phase-1 off-session bank↔trading
transfer + Phase-2's EnsureAccount / StartSession loaders + the Phase-2 TradingCash
read). No funnel, no shard, no money math.

## THE BRIDGE (doc ↔ built schema)
- `TradeRow` is 1:1 with `participant_trades`' 15 columns — confirm against the drainer's
  `insertParticipantTradeSQL`, not the doc list.
- `trade_costs` India columns (`brokerage, stt, exchange, sebi, stamp, gst, autosquare_fee, total`)
  come from `india.go`; `trade_costs` has **no `account_id`** (1:1 child, PK `trade_id`) → cost
  reads **must** join `participant_trades` to filter by account.
- The `ix_trades_account_recent (account_id, exit_at DESC)` index is **assumed from schema, not
  verifiable in Go source** — **flag it; verify against migrations** (recent/page perf depends on it).
- `TradingCash` reads the signed `trading_cash` column (migration 0001: `BIGINT NOT NULL`;
  **no `CHECK(>=0)`** — debt is legal; a QualifyOnDemand gate relies on a negative value
  cleanly failing the `>=` comparison).

## Owns / public surface
```go
type Store struct {
    pool      *pgxpool.Pool    // own Postgres pool — all Phase-1 reads/writes + Phase-2 lifecycle writes
    redis     *redis.Client    // own Redis client  — idle at EnsureAccount birth; active at session loads
    lifecycle *lifecycle.Service // UNSTARTED persistence.Component underneath; no drainer
}

func NewStore(pool *pgxpool.Pool, redisClient *redis.Client, lifecycleSvc *lifecycle.Service) *Store
func (s *Store) Pool() *pgxpool.Pool               // accessor for callers that run SQL directly (bringup smoke, diagnostics); not a shutdown handle
func (s *Store) Lifecycle() *lifecycle.Service     // Phase-2 EnsureAccount / StartSession read this
func (s *Store) Close()                            // closes every handle (redis then pool); nil-safe + idempotent

// Phase-1 surface:
func (s *Store) AccountHandle(ctx, ownerType, ownerID string) (AccountID, error)                 // §2
func (s *Store) RecentTrades(ctx, accountID, n int) ([]TradeRow, error)                          // §3
func (s *Store) TradePage(ctx, accountID, limit int, before *time.Time) ([]TradeRow, error)
func (s *Store) PnLSummary(ctx, accountID) (PnLSummary, error)
func (s *Store) CostsPerTradeIN(ctx, accountID, limit int, before *time.Time) ([]TradeCostBreakdown, error)
func (s *Store) CostsAggregateIN(ctx, accountID) (CostAggregate, error)
func (s *Store) DurableTransfer(ctx, accountID, direction TransferDirection, amount int64) (available, bank int64, err error)  // §4

// Phase-2 additions (BROKER_PHASE2 §2):
func (s *Store) TradingCash(ctx, accountID AccountID) (int64, error)                             // signed; may be negative

// Pre-session cash view — the signup/login flow needs trading_cash + bank_cash before any
// market join, but the live balance read requires a session on an owning shard and so cannot
// answer. This method serves that pre-session case straight from Postgres:
func (s *Store) AccountCash(ctx, accountID AccountID) (tradingCash int64, bankCash int64, err error)  // signed trading, non-neg bank
```
Types: `TradeRow`, `PnLSummary`, `TradeCostBreakdown`, `CostAggregate`, `Line`, `TransferDirection`.
Errors: `ErrNoAccount` (a **signal**, not a failure), `ErrLoadFailed`, `ErrBadTransfer`, `ErrInsufficient`.

## Phase-2 supersession (P2-1·D3 — LOCKED)
`BROKER_PHASE2 §1.3` promotes the Store from "pool-only" to three handles:
- **Why redis:** Phase-2's StartSession runs `SessionLoad` / `OndemandSessionLoad` on the
  **Store's** lifecycle service (off-shard concurrency-safe: the load path is Postgres read
  + Redis WriteSnapshot, never touching the unlocked `dirty` map). The initial snapshot write
  needs a Redis client. Same cluster + same empty `KeyNamespace` as the per-shard bundles so
  the snapshot the Store writes lands at the exact key the owning shard mutates and the save
  path later deletes.
- **Why unstarted Component:** the Store NEVER calls `EnqueueClosedTrade` — the closed-trade
  drainer belongs on the live per-shard bundles (one drainer per shard, each shard its own
  Component). The Store's Component mediates only `OpenAccount` / `LoadAccount` /
  `WriteSnapshot` — all synchronous owner-driven methods — so no `Start()`, no goroutine,
  nothing for shutdown to tear down beyond closing the shared redis handle.
- **Why reuse L5's `OpenAccount`:** schema-coupling, not single-writer. L5 owns the
  `accounts` row shape (`drip_active` / `drip_progress`) and the id-mint scheme; the Store
  reuses `OpenAccount` to keep those L5-owned, rather than issuing a raw broker INSERT.

## Load-bearing rules
- **Own pool.** No shard references. The pool is handed in by the wiring root (bringup,
  Phase-1 Stage D). Torn down by `Store.Close()` as its last internal step (redis
  first, pool last) — the wiring root makes one `Store.Close()` call, after Broker.Stop
  and all per-shard bundles have been torn down.
- **Own redis.** New in Phase 2 per P2-1·D3. Also closed by `Store.Close()`, before the
  pool close. Must share cluster + empty `KeyNamespace` with the per-shard bundles
  (otherwise session snapshots the Store writes won't reach the recovering shard — a
  silent divergence).
- **Economy call surface is `economy/lifecycle` ONLY** (via the stored `*lifecycle.Service`).
  No `reserve` / `realize` / `cost` calls; the Store never does money math.
- **Reads are pure durable reads** — nothing persisted. Return **raw rows**; all shaping/formatting
  is the Phase-6 response layer, never here.
- **No recomputation — ever.** P&L and cost figures are SQL `sum`/`count` over **already-stored**
  columns (`gross_pnl`, `net_pnl`, the India cost columns). The P&L and cost *math* happened in L5
  at write time. The Store **projects**; it never re-derives a number. (This is why the Store needs
  no formula audit.)
- `AccountHandle`: both filters required, **one row max** (`uq_accounts_owner` UNIQUE on
  `(owner_type, owner_id)`). `pgx.ErrNoRows` → `ErrNoAccount` (signal); other → `ErrLoadFailed`.
  **`ownerType` is a string at this seam** (P1-D5) — L5's typed `OwnerType` stays up in Phase 2.
- `TradingCash`: single-column `SELECT`; ctx-bound; **zero writes**. `pgx.ErrNoRows` →
  `ErrNoAccount`; other → wrapped `ErrLoadFailed`. Returns the **raw signed value** — Phase-2's
  QualifyOnDemand owns the `>= onDemandMinimum` comparison; the Store does not shape.
- `AccountCash`: two-column `SELECT (trading_cash, bank_cash)`; ctx-bound; **zero writes**. Same
  error contract as `TradingCash` (`pgx.ErrNoRows` → `ErrNoAccount`; other → wrapped
  `ErrLoadFailed`). Returns the **raw signed trading_cash** + non-negative `bank_cash` — no
  shaping, no comparison. Added for the pre-session cash view the signup/login flow needs: the
  live `/v1/account/balance` requires a session on an owning shard, so an account with no live
  session cannot be answered through the live family; `AccountCash` is that off-shard answer.
  Off-session sole-writer + XOR invariants apply unchanged (`DurableTransfer` is still the only
  cash mutator off-session, and account-live-XOR-off-session rules out a race against
  shard-side `MoveCash`).
- Read behaviour: `n<=0` → `[]` (no query); cap `n` at `maxRecent`; **empty ≠ error**; `before=nil`
  → newest page; keyset pagination on `exit_at` (`next_before` = last row's `exit_at`, null → last
  page); all queries ctx-bound.
- **Cost reads are India-only by design** (P1-D7) — India column knowledge is **quarantined** in
  these two methods (one shared column→`[]Line` mapper); the country→table registry + dispatch is
  the **Phase-6 handler's** job, not here. `brokerage` is always 0 (order-level, separate path) →
  it drops from the per-trade breakdown's zero-value lines.
- **`DurableTransfer` = one guarded `UPDATE`** — atomic guard-and-move, conserving by construction,
  **no transaction wrapper, no read-then-write.** Asymmetric guards: `toTrading` needs
  `bank_cash >= amount`; `toBank` needs `trading_cash >= amount`. `trading_cash` is **signed** (debt
  legal, no CHECK); `bank_cash` carries DB CHECK `ck_bank_nonneg` that **backstops** the guard. On
  **0 rows**, disambiguate via the `AccountHandle` existence probe: missing → `ErrNoAccount`, guard
  failed → `ErrInsufficient`. Validate `amount>0` + legal direction first → else `ErrBadTransfer`.
- **Off-session sole-writer + XOR.** Off-session, `DurableTransfer` is the **only** mutator of the
  `accounts` cash columns (`open_account`/`save_account` are birth/live-close only), and an account
  is live-on-a-shard **XOR** off-session — so `DurableTransfer` never races `MoveCash`. Conservation
  holds without a lock. The Phase-2 `OpenAccount` birth and `SessionSave`/`SessionDiscard` runs via
  the Store's lifecycle service respect this XOR because EnsureAccount runs before any session
  exists and save/discard runs after the live session has ended.
- **Buffer-lag: accept + document** (P1-D6). A just-closed trade may sit in the Redis closed-trade
  buffer for one drain interval before a Postgres read sees it. Not merged, not flushed.

## Deps & seam
- `connectivity/db` (pool + redis are handed in by the wiring root's `NewPostgres` /
  `NewRedis`), `pgx/v5`, `go-redis/v9`. Reads `accounts`, `participant_trades`, `trade_costs`
  (migrations 0001–0003).
- `economy/lifecycle` — stored `*lifecycle.Service` for `OpenAccount` / `LoadAccount` /
  `SessionLoad` / `OndemandSessionLoad`. Phase 2 EnsureAccount + StartSession consume this.
- `economy/persistence` — bringup builds the UNSTARTED `*persistence.Component` the lifecycle
  service wraps. The Store itself names persistence only in the wiring step, never in a method
  body (lifecycle holds the Component behind an unexported `store` interface).
- Consumed by: Phase 2 `EnsureAccount` (`AccountHandle` + `Lifecycle().OpenAccount`),
  Phase 2 `QualifyOnDemand` (`TradingCash`), Phase 2 `StartSession`
  (`Lifecycle().SessionLoad` / `.OndemandSessionLoad`), Phase 3 transfer endpoint
  (`DurableTransfer`), Phase 6 read handlers (the five reads).

## Do NOT
- hold a shard reference, or call `economy/reserve` / `economy/realize` / `economy/cost`;
- call `component.Start` on the Store's persistence.Component — the drainer belongs on
  per-shard bundles, not here; a Started Component here would spawn a second drainer racing
  the shards' drainers over the same Redis buffer;
- call `EnqueueClosedTrade` on the Store's Component — closed-trade enqueue is a per-shard
  concern bound to a live `realize` fill, never off-shard;
- recompute any P&L or cost figure — project stored columns only;
- shape or compare the `TradingCash` or `AccountCash` return — return the raw signed int64(s);
  comparisons and presentation live in the caller (QualifyOnDemand for TradingCash, the
  `broker/reads` account_cash HTTP handler for AccountCash);
- drop the `participant_trades` join on the cost reads (there is no `account_id` on `trade_costs`);
- wrap `DurableTransfer` in a transaction or do a read-then-write;
- generalize the cost reads beyond India (country dispatch is Phase 6);
- shape, format, or paginate-for-presentation the rows (Phase 6);
- namespace the Store's Redis keys differently from the per-shard bundles — same empty
  `KeyNamespace` in prod so a snapshot written here is readable from the owning shard.

## Flags (surface in orientation, don't silently resolve)
- `ix_trades_account_recent` index — assumed from schema §2.2, **verify against migrations**.
- Confirm the `participant_trades` 15 columns + `trade_costs` India columns against the **real
  source** (drainer insert SQL + `india.go`) before writing the SQL.

## Tests
Integration against the test DB (reuse the L5 `l5_test` harness + migrations 0001–0003) — **no
golden-first, no auditor** (pure projections + guarded moves, objective):
- transfer: both directions × {success, guard-fail → `ErrInsufficient`, missing → `ErrNoAccount`,
  bad amount/direction → `ErrBadTransfer`}; **conservation** (trading+bank constant); the
  `ck_bank_nonneg` backstop rejects a buggy guard rather than corrupting.
- reads: projection correctness vs seeded rows; keyset pagination (`before` walks pages,
  `next_before` terminal); `empty ≠ error`; `n<=0` → `[]`; `maxRecent` cap.
- cost reads: the `participant_trades` join filters by account; the India column→`Line` mapper;
  `brokerage=0` drops from the per-trade breakdown.
- `TradingCash` (Phase 2): positive / zero / **negative** (signed) round-trips; missing row →
  `ErrNoAccount`; pool-closed → `ErrLoadFailed`; zero writes observable.
- `AccountCash` (pre-session cash view): positive / zero / **signed-negative trading** round-trips
  on both columns; missing row → `ErrNoAccount`; pool-closed → `ErrLoadFailed` (and
  **`ErrNoAccount` must not leak onto the transport-failure path** — the two signals stay
  distinguishable so the HTTP handler can map 404 vs 503); zero writes observable.
- Bringup integration: Store builds with a live Redis + unstarted Component; `Lifecycle()` is
  non-nil; `Store.Close()` closes redis then pool (idempotent / nil-safe); a second
  `Shutdown()` call is safe (double-close path, same `shutdownOnce` guard).
- `go test ./... -race` standing gate.
