Broker — Phase 1 Implementation Design (Wiring Root & Durable-Store Layer)

Scope: the four Phase-1 points — the wiring root that stands the broker up, plus the broker-owned durable-store layer where every off-shard durable operation lives. Phase 1 is "everything durable, and the bootstrapper that produces the funnel."

Status: all four points LOCKED (design + source-verified in this session).

Where it sits: after Phase 0 / Tier 0 (the funnel — done, BROKER_TIER0_IMPLEMENTATION_DESIGN.md) and before Phase 2 (session & account lifecycle / Tier 3). No order-core work here.

The four points:

Wiring root — the main/BringUp that loads config, opens infra, and births the funnel.
get_account_handle — the owner-identity → AccountID durable lookup.
Durable Store reads — the five closed-trade / P&L / cost accessors.
durableTransfer — the off-session bank↔trading move (a durable write).

Read rule: every type/column/signature below is grounded in the real economy + connectivity source and the locked broker design docs. Where a fact could not be confirmed from the Go source (DDL/indexes), it is flagged inline.

0. Orientation — two things Phase 1 introduces

Phase 1 introduces exactly two new structural pieces on top of the Tier-0 funnel:

The per-shard bundle model (Point 1) — how the funnel and its L5 infra are actually constructed. This supersedes a Tier-0 wiring decision (see §1.2 and the decision log).
The broker-owned Store (Points 2–4) — a single, shard-independent durable layer that owns its own Postgres pool and hosts every off-shard durable operation. Points 2 and 3 are reads on it; Point 4 is its first write.

Nothing in Phase 1 adds new runtime behaviour to the funnel itself — Point 1 only stands up what Tier 0 already designed; Points 2–4 are durable operations that never touch a shard.

1. Point 1 — The Wiring Root
1.1 Persistence question

The wiring root holds no per-account or per-session state — it is a bootstrapper. Its persistence responsibility is to birth the durable substrate everyone writes through (the Redis snapshot mirror + Postgres store + the closed-trade drainer, all stood up by capstone.Wire) and to tear it down in the correct order so no buffered closed-trade rows are lost. Nothing durable of its own.

1.2 The per-shard bundle model (the load-bearing decision)

Each shard owns its own full capstone.Wire bundle. BringUp calls capstone.Wire once per shard; shard k owns bundle[k] — its own pool, redis client, persistence component, cost module, and lifecycle service. From each bundle the shard uses {Pool, Redis, Persist, Cost, Lifecycle} and ignores the stub trio {Ledger, Reserve, Realizer} (the same ignore-the-stub rule Tier 0 locked, now applied per bundle). The shard builds its own reserve + realizer bound to itself as the AccountLedger (Tier-0 point 6), now over its bundle's Persist + Cost.

Why per-shard bundle (the gap it resolves). Tier 0 filed persistence as "one shared component." But persistence.Component's dirty map is a plain Go map with no lock ("no lock — owner-driven, single-writer," verified in source), and reserve/realize call MarkSnapshotDirty internally at six confirmed call sites (reserve.go, convert.go, release.go). With one shared component, N shard goroutines would write that one map → fatal error: concurrent map writes. Per-shard bundles give each shard its own component → own dirty map → single-writer by construction, no lock, no economy change.

This supersedes BROKER_TIER0_IMPLEMENTATION_DESIGN.md's "one shared persistence component" / point 6's "shared component handed to N shards." (See decision P1-D1; the Tier-0 doc's point 6 wiring is now superseded and should be annotated there.)

1.3 Config (BrokerConfig)

The broker's own dials, separate from connectivity.Config (which carries infra DSNs + pool sizing only). config.Load reads env once; the resulting connectivity.Config is handed to every capstone.Wire call (Wire does not read env, by its own contract).

go
type BrokerConfig struct {
    BrokerID   BrokerID  // identity of this broker instance
    ShardCount int       // N — funnel width (hash(AccountID) % N)
    InboxDepth int       // per-shard inbox buffer (still a tuning value — carried)
    Country    string    // selects the cost module + adapter (e.g. "IN")
}

Pool sizing (MaxConns/MinConns) rides in the connectivity.Config handed to each Wire (see decision P1-D2).

1.4 Bring-up
go
type Runtime struct {
    Broker  *Broker
    bundles []*capstone.Bundle   // one per shard, held for shutdown
    store   *Store               // off-shard durable layer (Points 2–4)
}
func BringUp(ctx context.Context, cfg BrokerConfig) (*Runtime, error)
UP:
  1. infra := connectivity.config.Load()                 // env read ONCE; fail-fast
  2. for k in 0..N-1:
         cost_k, adapter_k := cost.NewModule(cfg.Country)
         bundle[k] := capstone.Wire(ctx, capstone.Options{
             ConnConfig: infra,          // small MaxConns (P1-D2)
             RedisNamespace: "",         // prod: keys match L5 §7 verbatim
             Cost: cost_k, CostAdapter: adapter_k,
         })                              // opens own pool+redis, NewComponent + Start (own drainer)
         err → Close the bundles already built, return err
  3. storePool := connectivity.db.NewPostgres(infra)      // Store's OWN pool (small, same DB)
     store := NewStore(storePool)
  4. broker := NewBroker(cfg.BrokerID, cfg.InboxDepth, bundles, store)
         // per shard k: bind to bundle[k].{Pool,Redis,Persist,Cost,Lifecycle};
         //              build shard's own reserve/realizer bound to the shard;
         //              ignore bundle[k].{Ledger,Reserve,Realizer}
  5. broker.Start()                                       // spawn N owner goroutines (empty, parked)
  6. return &Runtime{broker, bundles, store}

End state: N shard owners (empty maps, parked on inboxes) + N closed-trade drainers + a ready Store. No accounts, no markets — Tier 0's terminal state, exchange-independent.

NewBroker signature change (superseding Tier 0): was NewBroker(brokerID, n, inboxDepth, cost, persist, lifecycle); now NewBroker(brokerID, inboxDepth, bundles []*capstone.Bundle, store *Store) (n = len(bundles)). A direct consequence of P1-D1.

1.5 Shutdown
go
func (rt *Runtime) Shutdown()
DOWN (strict order):
  0. caller has already stopped ingress          // Tier-0 contract: route() after inbox close panics
  1. rt.Broker.Stop()                            // close each inbox → owners drain their loop and exit
  2. for each bundle: bundle.Close()             // capstone LIFO: Persist.Stop → Redis.Close → Pool.Close
  3. rt.store pool close

Owners must fully exit before their bundle's persistence stops / redis closes — a shard's final MarkSnapshotDirty/snapshot write must not land on a closed handle. Un-drained closed-trade rows survive in Redis for next boot (persistence D5) — crash-safety, not a leak.

1.6 Ingress / egress & cross-layer touch

Ingress: BrokerConfig + env. Egress: a running empty Broker + held bundles + a ready Store. Touch: connectivity (config.Load, db.NewPostgres for the Store pool), economy/cost (per-shard cost module + adapter), economy/capstone (Wire per shard), and Tier-0's NewBroker/Start/Stop (signatures per §1.4). No exchange/L7 dependency to come up.

1.7 Known property — N drainers on one shared buffer

Per-shard bundle ⇒ each bundle's component.Start spawns a drainer, so N drainers all pop the one shared Redis closed-trade buffer (l5:buf:trades; same Redis, same DB — P1-D3). Redis pop is atomic, so each buffered row is claimed by exactly one drainer (work is partitioned, not duplicated); the cost insert is ON CONFLICT (trade_id) DO NOTHING as a replay backstop. Safe, but N goroutines do the work of one — carried as a possible consolidation (§8).

2. Point 2 — get_account_handle
2.1 Persistence question

Pure durable read — persists nothing, no snapshot, no Redis, no dirty state. The only persistence involved is that the Store owns the connection it reads through.

2.2 The Store skeleton (introduced here)

The broker-owned, shard-independent durable layer. Holds only a Postgres pool — no Redis (the snapshot mirror is the shards' per-bundle concern), no shard references.

go
type Store struct { pool *pgxpool.Pool }        // own pool → same shared Postgres DB
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

Constructed in the wiring root (§1.4 step 3), held on Runtime, closed last on shutdown.

2.3 The lookup
go
func (s *Store) AccountHandle(ctx context.Context, ownerType, ownerID string) (AccountID, error)
sql
SELECT account_id FROM accounts WHERE owner_type = $1 AND owner_id = $2

Both filters required, one row max — verified: the accounts table has account_id, owner_type, owner_id with uq_accounts_owner UNIQUE on (owner_type, owner_id) ("one account per owner"). (human,"x") and (agent,"x") are distinct accounts. QueryRow, not multi-row.

row.Scan(&aid):
  pgx.ErrNoRows → "", ErrNoAccount        // owner has no account (a SIGNAL, not a failure)
  other error   → "", wrap(ErrLoadFailed)
  nil           → aid, nil
2.4 Seam type & errors
ownerType is a string at this seam (decision P1-D5) — the accounts owner_type column stores the string form ("agent"/"human"); L5's typed lifecycle.OwnerType stays up in Phase 2's EnsureAccount, which does the .String() when it calls down. The Store speaks the DB's language, decoupled from lifecycle.
ErrNoAccount — not a failure; it is the signal Phase 2's EnsureAccount reads as "owner has no account yet." Also reused by Point 4 as its existence probe.
ErrLoadFailed — infra/query failure (wraps pgx error).
2.5 Scope boundary

Point 2 is the lookup primitive only. The full EnsureAccount get-or-create (calls L5's OpenAccount, falls back to this lookup on ErrAccountExists) is Phase 2, point 1 — it consumes this. No create logic, no starting-capital dial here.

3. Point 3 — Durable Store reads
3.1 Persistence question

All five are pure durable reads — nothing persisted, no Redis, no dirty state. Read-only through Store.pool.

3.2 Provenance

These reads and the surfaces that consume them (A10 trade-history, A14 agent snapshot, A16 cost view) are fully designed in BROKER_READ_SURFACES_tier4.md (Tier 4). Per the roadmap, Phase 1 builds the durable accessors; the endpoints/handlers stay in Phase 6. The accessor designs were cross-verified against source in this session (below).

3.3 The five accessors + types
go
func (s *Store) RecentTrades(ctx, accountID AccountID, n int) ([]TradeRow, error)
func (s *Store) TradePage(ctx, accountID AccountID, limit int, before *time.Time) ([]TradeRow, error)
func (s *Store) PnLSummary(ctx, accountID AccountID) (PnLSummary, error)
func (s *Store) CostsPerTradeIN(ctx, accountID AccountID, limit int, before *time.Time) ([]TradeCostBreakdown, error)
func (s *Store) CostsAggregateIN(ctx, accountID AccountID) (CostAggregate, error)

type TradeRow struct { // 1:1 with participant_trades' 15 columns (verified)
    TradeID string; AccountID AccountID; SessionID SessionID; InstrumentID InstrumentID
    Direction string; Quantity, EntryPrice, ExitPrice int64
    EntryAt, ExitAt time.Time
    GrossPnl, TotalCosts, NetPnl int64; PnlPct float32; ExitReason string
}
type PnLSummary        struct { TradeCount int; GrossPnl, TotalCosts, NetPnl int64; WinCount, LossCount int }
type TradeCostBreakdown struct { TradeID string; Lines []Line; Total int64 }
type CostAggregate      struct { TradeCount int; Lines []Line; Total int64 }
type Line               struct { Name string; Amount int64 }
3.4 The SQL (verified shapes)
recent: SELECT <15 cols> FROM participant_trades WHERE account_id=$1 ORDER BY exit_at DESC LIMIT $2
page: recent + AND ($2::timestamptz IS NULL OR exit_at < $2) (keyset, NULL = newest page)
pnl: count(*), coalesce(sum(gross_pnl|total_costs|net_pnl),0), count(*) FILTER (net_pnl>0|<0)
costs-per-trade: India cost columns, trade_costs JOIN participant_trades USING(trade_id) WHERE account_id=$1, keyset
costs-aggregate: sum() of India cost columns over the same join
3.5 Source verification
participant_trades 15 columns from the drainer's insertParticipantTradeSQL — exact match to TradeRow.
trade_costs India columns from india.go: trade_id, brokerage, stt, exchange, sebi, stamp, gst, autosquare_fee, total — exact match to §8.4.
trade_costs has no account_id (1:1 child, PK trade_id) → both cost reads must join to participant_trades to filter by account. Confirmed necessary, not optional.
brokerage is always 0 (source D2 — order-level, separate path) → it drops out of the per-trade breakdown's "drop zero-value lines." Consequence, not a bug.
Could not verify from Go source: the index ix_trades_account_recent (account_id, exit_at DESC) (DDL/migration, not in insert code). Taken from schema §2.2 per the Tier-4 doc's "verified." Check when touching migrations — recent/page performance depends on it.
3.6 Behaviour locked & shared internals

n<=0 → [], no query; cap n at maxRecent; empty ≠ error; before=nil → newest page; next_before = last row's exit_at (null → last page); all queries ctx-bound; raw rows returned (shaping happens at the Phase-6 response layer). Two shared internals: a TradeRow scan helper (recent + page) and a column→[]Line mapper for the India cost set (the one place India column knowledge lives). Buffer-lag is inherited and already decided (accept + document) in Tier 4 — see P1-D6.

3.7 Scope boundary

Only the five accessors. The endpoints (GET /v1/account/trades, /v1/account/costs), their handlers, response JSON, the list+summary fan-out, and the country→table registry + IN/US dispatch are Phase 6. The two cost accessors are India-only by design (column knowledge quarantined inside them); country selection lives in the Phase-6 handler (P1-D7).

4. Point 4 — durableTransfer
4.1 Persistence question

One Postgres UPDATE — no Redis, no snapshot. There is no session, so no crash-window; the value is durable the instant the UPDATE commits. The write is the persistence (the exact inverse of the in-session transfer's Redis mirror).

4.2 Scope

Only the Stage-2 durableTransfer primitive — the broker-owned single guarded UPDATE, the first off-shard write on the Store. The endpoint (POST /v1/account/transfer), the route-first "ask the shard if it's live" control flow, and Stage 1 (in-session MoveCash on the shard) are Phase 3 (BROKER_BANK_TRANSFER.md, both stages LOCKED). This is NEW because L5 has no durable transfer (its transfer is in-memory MoveCash; its durable writes are open_account at birth and save_account at session-save only — verified).

4.3 The method
go
func (s *Store) DurableTransfer(ctx context.Context, accountID AccountID,
        direction TransferDirection, amount int64) (available, bank int64, err error)
type TransferDirection int   // toTrading (Bank→Available) | toBank (Available→Bank)
sql
-- toTrading (Bank → Available):
UPDATE accounts SET bank_cash = bank_cash - $amt, trading_cash = trading_cash + $amt
 WHERE account_id = $id AND bank_cash >= $amt              -- can't overdraw bank
 RETURNING trading_cash, bank_cash
-- toBank (Available → Bank):
UPDATE accounts SET trading_cash = trading_cash - $amt, bank_cash = bank_cash + $amt
 WHERE account_id = $id AND trading_cash >= $amt           -- can't bank money you don't have
 RETURNING trading_cash, bank_cash
validate amount>0 + legal direction → else ErrBadTransfer
run the direction's UPDATE:
  0 rows → disambiguate via get_account_handle existence probe (P2 reuse):
             missing → ErrNoAccount ; guard failed → ErrInsufficient
  row    → (trading_cash, bank_cash), nil

One SQL statement = atomic guard-and-move, conserving by construction, no transaction wrapper, no read-then-write race.

4.4 Source verification
trading_cash int64 SIGNED (debt legal, no CHECK(>=0) by design) and bank_cash int64 >= 0 — confirms the asymmetric guards.
New finding: bank_cash carries a DB CHECK ck_bank_nonneg. It doesn't conflict — it backstops the toTrading guard (a passing guard keeps bank_cash >= 0; a failing guard writes 0 rows). Upshot: a buggy guard would be rejected by Postgres, not corrupt the balance. toBank only raises bank_cash; trading_cash has no such check.
Off-session sole-writer confirmed: only open_account (birth) and save_account (live close) write these columns; off-session, durableTransfer is the sole mutator → conservation holds.
Response mapping: persistence collapses the live Available/Reserved/Settled split into one signed trading_cash; off-session there are no reservations (XOR), so trading_cash == available and returning it as available is correct.
4.5 Safety (grounded in the XOR invariant)

An account is live on a shard only during a session (LAYER7_TIER0 T0.12 + broker read-split). So durableTransfer (off-session) and MoveCash (live) can never touch the same account concurrently — no TOCTOU. A session starting later just reads the post-transfer trading_cash (the retry model).

4.6 Scope boundary

The endpoint, route-first control flow, and Stage 1 are Phase 3. The doc's only open items (unit vocabulary, 422-vs-400 status) are endpoint concerns → Phase 3. The primitive itself is fully locked.

5. The Store, consolidated

One broker-owned, shard-independent durable layer. Holds only a Postgres pool (its own, same shared DB, sized small). Methods across Phase 1:

Method	Point	Kind	Tables
AccountHandle	2	read	accounts
RecentTrades / TradePage	3	read	participant_trades
PnLSummary	3	read	participant_trades
CostsPerTradeIN / CostsAggregateIN	3	read	trade_costs ⨝ participant_trades
DurableTransfer	4	write	accounts

No Redis, no shard, no lifecycle call, no funnel. Pool sized for the combined read load (Point-3 reads are the heaviest consumer), retunable via config.

6. Decisions made (Phase 1 decision log)
P1-D1 — Per-shard bundle. Each shard owns a full capstone.Wire bundle (own pool/redis/persistence/cost/lifecycle); uses {Pool,Redis,Persist,Cost,Lifecycle}, ignores the stub {Ledger,Reserve,Realizer}, builds its own reserve/realizer bound to the shard. Resolves the shared-dirty-map race with no lock and no economy change. Supersedes the Tier-0 "one shared persistence component" / point-6 wiring and changes NewBroker's signature. LOCKED.
P1-D2 — Pool sizing. Each shard's pool MaxConns >= 2 (one for the shard's serial work, one for its own drainer — MaxConns=1 self-deadlocks against the drainer), MinConns low so idle cost is near-zero; N kept low for now. Via config, not hardcoded. LOCKED.
P1-D3 — One backing store for all. One Postgres cluster + one DB + one Redis cluster for every shard; N pools/clients, one backing store (one shared closed-trade buffer). LOCKED.
P1-D4 — Broker-owned Store. All off-shard durable operations live on a single broker-owned Store (its own pool, shard-independent, initialised/held/closed by the wiring root). LOCKED.
P1-D5 — ownerType is a string at the Store seam (matches the DB column; typed lifecycle.OwnerType stays in Phase 2). LOCKED.
P1-D6 — Buffer-lag: accept + document (inherited from Tier 4). A just-closed trade may sit in the Redis buffer for one drain interval before a Postgres read sees it; not merged, not flushed. LOCKED.
P1-D7 — Country hardcoded IN for the cost reads. CostsPerTradeIN/CostsAggregateIN quarantine India column knowledge; the country→table registry + dispatch is the Phase-6 handler's job. Country-agnostic path is a later refinement, not built for one market. LOCKED.
P1-D8 — durableTransfer reuses get_account_handle as its 0-rows existence probe (missing vs insufficient). LOCKED.
P1-D9 — The starter is broker-owned. The thing that triggers bring-up and stands up the funnel is the broker's, full stop. The L7 partial-ownership nuance does not move this and is deferred (§8). LOCKED.
7. Constraints / invariants honored
Single-writer per account — the funnel (unchanged from Tier 0); per-shard bundles keep each shard's dirty map single-writer.
XOR (live ⊕ off-session) — underpins durableTransfer safety and the trade-read split (durable reads never race a live shard for the same account's money state).
One-writer principle — the Store is the sole off-shard writer of accounts cash columns; combined with L5's open_account/save_account being live/birth-only, conservation holds.
Country-agnostic — cost column knowledge quarantined inside the per-country Store methods; everything above is country-blind.
Persistence-with-every-functionality — each point opened with its persistence question: the reads persist nothing, durableTransfer is durable-on-commit with no crash-window, the wiring root births + tears down the substrate in order.
8. Deferred / carried items
EnsureAccount (get-or-create wrapper over OpenAccount + get_account_handle) — Phase 2.
Endpoints/handlers for Point 3 (trade/cost reads) — Phase 6; for Point 4 (transfer) — Phase 3.
Stage 1 in-session transfer (MoveCash on the shard) and the route-first control flow — Phase 3.
L7 partial-ownership nuance — later Phase 1 / Tier 3; does not touch the funnel-starter (P1-D9).
Inbox-depth value — still an open tuning value (config).
N (shard count) — kept low for now; a retunable config value.
N-drainer consolidation — N bundles ⇒ N drainers on the one shared buffer (safe, atomic pop partitions the work); optionally consolidate to a single drainer later (§1.7).
Snapshot-flush cadence — each shard now owns its dirty map (via its bundle), but when/how a shard drains its dirty set and writes snapshots is designed with the money-handling / teardown work, not here.
ix_trades_account_recent index — assumed per schema §2.2; verify when touching migrations.
Cost-view country parameterization — India-only now; generalize when a second market lands.
Endpoint cosmetics — unit vocabulary + 422-vs-400 for insufficient funds (Phase 3).
9. Cross-doc note

P1-D1 supersedes BROKER_TIER0_IMPLEMENTATION_DESIGN.md's "one shared persistence component" (point 6's "shared component handed to N shards"). The Tier-0 doc should carry an annotation pointing here, so the two stay consistent for a cold reader.