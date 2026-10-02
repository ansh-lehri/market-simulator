# broker — Money-Orchestration (Funnel) Constitution

<!-- Domain CLAUDE.md. READ ORDER for any session working in any broker/ subpackage,
     all apply together:
       1) /CLAUDE.md              — root constitution (tech stack, integer-only, message-seam, tests, workflow)
       2) THIS file               — the broker (funnel) constitution: rules every broker package obeys
       3) broker/<pkg>/CLAUDE.md  — what is specific to that package
       4) docs/broker/*.md        — the authority for behavior
     Does not repeat root; it pins root to this domain and states what is broker-wide. -->

## What the broker IS (one line)
The sharded single-writer **funnel** plus the tiers that run inside it (order intake, settlement,
session lifecycle, reads, bank moves). It **holds money and places orders**: it sits on **L5
(`economy/`)** for every money mutation and talks to the **exchange** only through the `contract`
wire seam. It does no matching (that is the exchange) and reimplements no money math (that is L5).

## The load-bearing spine (settled; never re-litigate — the Tier-0 doc §1)
- **One broker, one process**, in the same process as the exchanges, serving the whole
  participant population, fanning out to the N exchange handles L7 provides. Not one broker per
  exchange, not one per session.
- **The funnel = sharded single-writer.** A fixed pool of N owner (shard) goroutines; each account
  routes by `hash(AccountID) % N` to exactly one shard, which is the **sole caller into the L5
  hands** for its accounts. Routing key is **`AccountID`**, aliased to `account.AccountID` (the L5
  type — no conversion at the L5 seam).
- **The broker is one entity; shards are internal.** Callers hit `Broker` methods; the router
  hashes `AccountID` and hands to the right shard. **N is an internal tuning dial with no external
  contract.**
- **Owner tied to a live `Account` instance**, not a human identity. State is
  `map[AccountID]*ownedSession`; a session dimension keys in later without restructuring.
- **One live session per user** (canonical XOR on-demand; no mid-session switching).

## The single-writer rule (WHY the funnel exists — hold exactly)
The L5 hands (`MoveCash`, `MovePosition`, `CreditPosition`, `RemovePosition`, `CreditCash`,
`DebitCash`) do **check-then-mutate with NO internal locking** — safe **only** if exactly one
goroutine touches a given `Account` at a time. `account` assumes this and does **not** enforce it.
Therefore:
- Only the **owning shard** ever touches its accounts' state — the L5 hands, the reservation book,
  the realizer, the OrderTracks.
- **Reads are commands too** — a live read runs on the owner and returns a **by-value copy**; it
  never reaches into another shard's state.
- Single-writer is **assumed, not enforced** → tests run single-threaded per account, and
  **`-race` is a standing gate** on every broker package.

## Per-shard bundle ownership (decision P1-D1 — SUPERSEDES Tier-0's "one shared component")
**Each shard owns its own full `capstone.Wire` bundle** (own pool, redis client, persistence
component, cost module, lifecycle service). From that bundle the shard **uses**
`{Pool, Redis, Persist, Cost, Lifecycle}` and **ignores** the stub trio
`{Ledger, Reserve, Realizer}`; the shard **builds its own `reserve` + `realizer`** bound to itself
as the `AccountLedger`, over its bundle's `Persist` + `Cost`.
**Why:** `persistence.Component`'s `dirty` map is an **unlocked** Go map, and `reserve`/`realize`
call `MarkSnapshotDirty` internally — one shared component across N shard goroutines would be a
`fatal error: concurrent map writes`. Per-shard bundles make each dirty map single-writer **by
construction: no lock, no economy change.** (The Tier-0 doc's point-6 "shared component" wiring is
annotated as superseded by this.)

## Money comes from L5 — never reimplemented
- Every cash/position change goes through the **`economy` hands/services**. The broker **never**
  does cost math, **never** mutates cash or positions outside L5, **never** duplicates reservation
  money-state.
- **`OrderTrack` is thin** — exchange-seam facts only (order id, instrument, side, qty, live
  flags). All reservation money-state is **read from L5**, never mirrored on the shard.

## Integer + message-seam discipline (inherited from root, pinned here)
- **int64 ticks/coins** everywhere; **no floats cross any seam.**
- The exchange seam is the `contract` package: **punch** with `InboundMessage` (wrapping
  `FundedOrder` / `CancelRequest`), **settle** from `EngineResult` — **values, not pointers, across
  the seam.**
- **`OrderID` shape is `"<AccountID>:<seq>"`** — it encodes the account so result ingress parses
  the owner out of the echoed id (no cross-shard `OrderID → AccountID` map). Whatever tier mints
  ids **must** honor this shape.

## Handlers are empty seams until their tier
The funnel (Tier 0 / Phase 0) is the **machine**; every handler body is a **body that plugs into it
later** — order intake/punch, settlement outcomes, bank moves, reads, stop fires each land in their
**own** tier. **Do not implement a handler body outside its tier.** An empty, clearly-marked seam is
the correct Phase-0 state.

## Persistence-with-every-functionality
Open every flow's design with its persistence question and settle **both** the Redis
crash-recovery snapshot mirror **and** the Postgres durable truth as part of that flow — never
leave state in-memory-only by default. Off-shard durable work lives on the broker-owned **`Store`**
(its own pool), never on a shard.

## Doc map (docs/broker/ — the authority for each area)
- **Funnel / router / reply / session binding** → the Tier-0 concurrency-and-routing doc (+ its
  Phase-0 implementation doc).
- **Wiring root + durable `Store` + `durableTransfer`** → the Phase-1 implementation doc.
- **Session & account lifecycle (load / save / discard / teardown)** → the Phase-2 implementation
  doc + the account-session-lifecycle doc.
- **Bank transfer endpoint** → the Phase-3 implementation doc.
- **Order intake / punch / settlement** → the order-lifecycle (intake-punch-settle) doc.
- **Read surfaces (balance / positions / orders / trades / P&L / costs)** → the read-surfaces
  (Tier-4) doc.
- **Market data + streaming** → the market-data-and-streaming doc (a separate `marketdata/` axis).
- **Seam obligations** → broker-obligations (to the exchange) + expectations-from-L7 (from L7).

## Code-wins-over-doc
Where a broker doc's illustrative sketch (SQL, a signature, a key layout) diverges from the
**built** `economy` / `exchange` / `connectivity` source, **the built code wins** — surface the
divergence as a **flag in orientation**, never silently follow either side.

## Do NOT (domain-level)
- reimplement any L5 money math, or mutate cash/positions outside the `economy` hands;
- touch an account a shard does not own, or read another shard's state directly;
- duplicate reservation money-state (keep `OrderTrack` thin);
- share one `persistence.Component` across shards;
- put a lock or a map lookup on the punch hot path (the session binding is a co-located field read);
- implement a handler body outside its own tier.