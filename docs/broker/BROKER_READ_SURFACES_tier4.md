Broker — Read & View Surfaces (Tier 4)

What this doc covers. Every consumer-facing HTTP surface the broker exposes for reading state and for the two write-compose actions that belong at the interface tier. Concretely: account balance, positions (with live marks), open orders, the agent observation snapshot, closed-trade history / P&L, the country-parametric cost & tax view, and the modify-order endpoint. This is the Tier-4 read/view layer that sits on top of the Tier-0 funnel and the Tier-1 order core.

Companion docs.

BROKER_CONCURRENCY_AND_ROUTING.md — the shard/funnel machine every command routes through (Tier 0).
BROKER_ORDER_LIFECYCLE_intake_punch_settle.md — place/cancel/modify/settle order core (Tier 1); the modify endpoint here composes its handleCancel + handlePlaceOrder.
BROKER_RESPONSIBILITIES_AND_PLAN.md — responsibility list + tier order.
BROKER_DESIGN_TRACKER.md — live status of every component.
L5 docs (LAYER_5_1_READ_OPERATIONS.md, LAYER_5_4_REALIZATION.md, LAYER_5_PERSISTENCE.md, LAYER_5_3_COST_ENGINE.md) — the in-memory account state, the mark/weighted-avg compute functions, the Postgres schema, and the country-agnostic cost representation this tier reads and composes.
0. The standing rules for the whole tier (read these first)

Every item below obeys these. They were decided once and apply everywhere; individual sections do not re-argue them.

0.1 No auth, anywhere

There is no authentication or identity resolution on any Tier-4 endpoint. This was considered (an early design carried a resolveCaller(ctx) → AccountID identity seam) and then removed by decision. Consequence: account_id is an explicit request parameter on every account-scoped endpoint — a query parameter on GET, a body field on POST. There is no ownership check; a caller naming an account may read it. (In a real product this would be a token→account resolution plus an ownership check; it is deliberately out of scope for the demo.)

0.2 The full HTTP flow is the unit of design

No item is "done" at the shard handler. Every item is designed across five layers:

Endpoint — method, path, params, body.
HTTP handler (request goroutine) — parse, validate, set timeout/context, address the funnel.
Funnel hop (request goroutine) — route by hash(account_id) % N, enqueue to the shard inbox, receive on a self-addressed reply channel.
Shard handler (owner goroutine) — the actual in-memory state access.
Response shaping (request goroutine) — reply → HTTP status + JSON body, every error mapped.
0.3 The live/durable read split (inherited from Tier 0)
Live session reads (balance, positions, live marks, open orders) go through the owner shard as a command, and the shard returns a by-value copy of its freshest in-memory truth. Never Redis (it lags the in-memory copy on a snapshot cadence), never Postgres (it has no live split at all).
Cross-session durable reads (closed-trade history, cost rows) read Postgres directly via the Store layer (§0.6), not the owner shard — that data is not in the live account.
0.4 Response units

Money is returned as raw integer minor units (paise/ticks) plus a unit indicator field (e.g. "unit":"paise"). No formatting to major units in the broker. This is provisional — it may change to formatted units later; the frontend formats for now.

0.5 Standard status codes
400 — missing / malformed required param (account_id, country, cursor, body fields).
409 — no live session for the account (you exist but are not currently trading). Used by the live/funnel reads.
404 — durable resource not found (a keyed durable row that does not exist). Used by durable reads. The 409-vs-404 split is deliberate: live-session state → 409, durable resource → 404.
504 — the read timed out (shard wedged, inbox full past deadline, or DB slow). Every handler bounds its wait with a context timeout it owns.
500 — a genuine query/internal failure on a core payload.
501 — a defined-but-unbuilt slot (used by A16 for a country whose table does not exist yet).
0.6 The durable-read layer is Store (locked convention)

All Postgres-direct read accessors are methods on our own Store wrapper type, never on the raw driver client:

go
type Store struct {
    pool *pgxpool.Pool   // the real pgx pool/client — connection pooling, raw queries
}
// domain methods hang off Store and call store.pool.Query(...) internally:
//   Store.RecentTrades / TradePage / PnLSummary / CostsPerTradeIN / CostsAggregateIN / TradeCosts

Store is the concrete meaning of "the broker reads Postgres directly": one home for all schema SQL, a mockable seam for handler tests, and the durable-read surface the read-split rule refers to. Naming: the type is Store (chosen over DB precisely so it does not read as "the raw Postgres client"); the field store.pool is the real client one layer down.

0.7 Verify-when-in-doubt

Before designing against any assumed field, table, or seam, confirm it against the actual L5 struct/schema/doc rather than memory. (This rule exists because two Tier-4 items — the position net mark and the cost view — were initially designed on unverified assumptions and had to be corrected.)

1. The read-command pattern (shared by all live/funnel reads)

A8, A9, A11, and the live half of A14 all use the same shape. Design it once here.

Command envelope (same family as the Tier-1 write commands, into the same per-shard inbox):

go
type ReadXxxCmd struct {
    AccountID AccountID
    // ... item-specific filters (e.g. Instrument) ...
    Reply     chan XxxSnapshot   // self-addressed, BUFFERED(1)
}

Why buffered(1): the shard's Reply <- snap must never block the shard goroutine. If the HTTP handler has already walked away (timeout / client disconnect), a buffered channel lets the shard's send still succeed and the value is simply GC'd. An unbuffered channel would wedge the single-writer goroutine behind a gone consumer — unacceptable, since that goroutine also matches orders.

The flow, every live read:

HTTP handler (request goroutine):
  1. ctx = r.Context()                                   // carries client-disconnect
  2. accountID = parse param                             → missing/bad → 400
  3. ctx, cancel = context.WithTimeout(ctx, readTimeout) // handler OWNS the timeout
     defer cancel()
  4. reply = make(chan XxxSnapshot, 1)
     cmd   = ReadXxxCmd{AccountID: accountID, ..., Reply: reply}
  5. shard = s.shards[ hash(accountID) % N ]
     select { case shard.inbox <- cmd: ; case <-ctx.Done(): → 504 }   // ctx-guarded enqueue
  6. select { case snap := <-reply: → shape ; case <-ctx.Done(): → 504 }

Shard handler (owner goroutine):
  a. sess, ok := s.sessions[cmd.AccountID]
     └─ !ok → cmd.Reply <- XxxSnapshot{Present:false}; return   // → 409
  b. read in-memory state, build by-value snapshot
  c. cmd.Reply <- XxxSnapshot{Present:true, ...}

Key properties of the pattern:

Reads share the inbox with writes, so a read is naturally serialized after any queued mutations for that account — the consumer sees post-mutation state, never a half-applied mutation.
The timeout lives in the handler, not the shard — the shard cannot know a caller's SLA; the handler bounds the wait and gives up cleanly.
Present bool on the snapshot is how the shard signals "no live session" across a channel (a channel cannot cleanly return an error). The handler maps Present:false → 409.
By-value copy only — replies carry scalars/value structs, never *Account / *Position / *Cash. A consumer physically cannot reach into and mutate live account state through a read reply. This is a structural isolation guarantee, not a convention.
2. A8 — Balance Read

What it serves. The live four-bucket cash split for an account, as of the freshest in-memory truth: Available (signed), Reserved, Settled, Bank. The pattern-setter for the tier.

Seam (verified). The live split lives in the in-memory Account.Cash struct, owned by exactly one shard goroutine. Per the read-split rule this is a funnel read of the owner's in-memory copy — not an L5 read primitive. (L5's read_available_cash / op-14 exists for the order hot path's internal use and returns only available; A8 needs the full split for display, so it copies Account.Cash directly on the owner goroutine.) Redis is not read — it lags the in-memory copy; Postgres has no split.

2.1 Endpoint
GET /v1/account/balance?account_id={id}

account_id is a query param (not a path segment). Rationale: it is a scope/filter, and — with no auth — putting it in the path would invite "read account X by editing the URL" with no ownership check to stop it. A query param keeps it a plain lookup key. Missing/malformed → 400.

2.2 Reply payload — raw buckets, nothing derived
go
type BalanceSnapshot struct {
    Present   bool    // false → no live session (→409)
    Available int64   // SIGNED — may be negative (Option-4 debt); raw, NOT floored
    Reserved  int64   // locked against resting orders
    Settled   int64   // realized, not yet folded into available
    Bank      int64   // the parked reserve (never negative)
}
2.3 Shard handler
handleReadBalance(cmd):
  sess, ok := s.sessions[cmd.AccountID]
  if !ok: cmd.Reply <- BalanceSnapshot{Present:false}; return
  cmd.Reply <- BalanceSnapshot{
      Present:   true,
      Available: sess.Account.Cash.Available,
      Reserved:  sess.Account.Cash.Reserved,
      Settled:   sess.Account.Cash.Settled,
      Bank:      sess.Account.Cash.Bank,
  }
2.4 Response shaping
!Present → 409 {error:"no live session for account"}
Present  → 200 {available, reserved, settled, bank, unit:"paise"}   // available signed
2.5 Decisions & rationale
Raw buckets, no derived views. The shard returns the four numbers only. "Total equity", "buying power", "net worth incl. positions" are not computed here because (a) some need position marks, not cash alone, and (b) computing them on the shard puts presentation logic on the single-writer goroutine that also matches orders. Derived views, if ever built, are composed one layer out on the request goroutine from this snapshot + the A9 position read. ("API layer" here means the request goroutine handling the HTTP call, not the shard — both are "the API," but only the request goroutine, off the hot path, does any shaping/derivation.)
Signed Available, un-floored. Debt is a negative available balance (L5 Option 4), returned signed and raw. No separate debt field, no clamp to zero. Presentation may render it red; the API stays honest.
By-value copy (four int64s), never a pointer into Account.
2.6 Open product question (flagged, decided live-only for now)

Should A8 fall back to a durable Postgres read (trading_cash) when there is no live session — i.e. show a "standing balance" to a not-currently-trading account? Decided: no, live-only for the demo. A durable standing-balance read is a separate concern that would blur the live/durable partition that is the spine of this tier. If a logged-out balance view is ever needed, it is its own durable endpoint.

3. A9 — Position Read (gross-only marks)

What it serves. Per open instrument: quantity held, weighted-avg entry ("buy average"), and the gross unrealized mark to LTP. One instrument or all for the account. Net unrealized is not served — see §3.5, a verified finding.

Seams (verified). Composes three things:

the in-memory Position{Available, Reserved, OpenLots} (owner-held);
5.4 ComputeWeightedAvgEntry(lots) — pure, zero-I/O;
5.4 ComputeUnrealizedMark(lots, ltp, incurredEntryCosts) → (gross, net) — pure, zero-I/O; needs LTP from the market-data cache.

Lot{EntryPrice, EntryAt, RemainingQty} and Position{Available, Reserved, OpenLots} were read directly to confirm their fields (this matters for §3.5).

3.1 The market-data-cache dependency (interface now, writer later)

The gross mark needs "last price for this instrument," which comes from the MarketDataCache whose writer is the tape relay (B10, Tier 5 — not built). Per the same pattern Tier-1's market-buy pricing uses, A9 codes against the MarketDataCache interface now; when the cache is cold (Tier 5 absent, or no print yet) the mark is simply unavailable, flagged mark_available:false. A9 is not blocked — position + weighted-avg work fully without LTP; only the mark degrades gracefully.

3.2 Compute-on-shard (Option b) — decided

Two options were weighed:

(a) shard copies raw lots out, request goroutine computes avg + mark;
(b) shard runs the two pure functions inline, returns finished numbers, lots never escape. Decided (b). The "keep the shard dumb" rule is really "no I/O, policy, or presentation on the shard" — a pure summation over a slice is none of those. (b) avoids copying the []Lot slice out on every read and keeps lot internals from escaping the owner. The LTP lookup is an in-memory map read, also non-blocking.
3.3 Endpoint
GET /v1/account/positions?account_id={id}                  → all open positions
GET /v1/account/positions?account_id={id}&instrument={sym} → one instrument
3.4 Reply payload
go
type PositionSnapshot struct {
    Present   bool           // false → no live session (→409)
    Positions []PositionView // empty slice = live but FLAT (→200 {positions:[]})
}
type PositionView struct {
    Instrument       InstrumentID
    Qty              int64   // open (available) units, free to sell
    ReservedQty      int64   // units locked behind resting sells
    WeightedAvgEntry int64   // ComputeWeightedAvgEntry(lots) — the "buy average"
    MarkAvailable    bool    // false when LTP cache is cold (Tier 5 not populated)
    LTP              int64   // price marked against; 0 when !MarkAvailable
    GrossUnrealized  int64   // Σ (ltp − lot.EntryPrice) × lot.RemainingQty; 0 when !MarkAvailable
    // NO NetUnrealized — see §3.5
}
3.5 Shard handler
handleReadPositions(cmd):
  sess, ok := s.sessions[cmd.AccountID]
  if !ok: cmd.Reply <- PositionSnapshot{Present:false}; return
  targets = (cmd.Instrument=="" ? all sess.Account.Positions : the one)
  views = []
  for instrument, pos in targets:
      if pos.Available == 0 && pos.Reserved == 0: continue        // fully flat → no row
      avg, _ := ComputeWeightedAvgEntry(pos.OpenLots)             // pure
      v := PositionView{Instrument, Qty:pos.Available, ReservedQty:pos.Reserved, WeightedAvgEntry:avg}
      ltp, ok := s.marketData.LastPrice(instrument)               // cache interface; ok=false if cold
      if ok:
          gross, _ := ComputeUnrealizedMark(pos.OpenLots, ltp, 0) // incurredEntryCosts=0; take GROSS only
          v.MarkAvailable, v.LTP, v.GrossUnrealized = true, ltp, gross
      views = append(views, v)
  cmd.Reply <- PositionSnapshot{Present:true, Positions:views}

ComputeUnrealizedMark(..., 0) passes incurredEntryCosts=0 and reads only the gross return; net is inert and discarded (see finding). Gross is fully correct — it needs only lots + ltp.

3.6 Response shaping
!Present            → 409 {error:"no live session for account"}
Present, empty      → 200 {positions:[]}       // FLAT is a valid 200, not an error
Present, has rows   → 200 {positions:[ {instrument, qty, reserved_qty, weighted_avg_entry,
                                        mark_available, ltp, gross_unrealized, unit:"paise"}, ... ]}

A specified instrument the account does not hold → 200 {positions:[]} (same as flat).

3.7 FINDING — IncurredEntryCosts is not tracked on the live position (why net is gross-only)

ComputeUnrealizedMark takes incurredEntryCosts as a caller-supplied parameter, but nothing in L5 stores it. Verified against the actual structs: Position{Available, Reserved, OpenLots} and Lot{EntryPrice, EntryAt, RemainingQty} — no cost field on either. Entry costs are charged per-fill in Convert (via DebitCash) but are never attributed back to the position's lots, so there is no accumulator to feed the parameter.

Three options were considered:

Add an IncurredEntryCosts accumulator to Position — incremented in Convert on each entry fill, reduced proportionally as lots are sold; plus a Redis-snapshot schema change to carry it. Correct and complete, but touches locked L5's write path — not a Tier-4-internal change.
Serve gross only — gross needs only lots + ltp; net deferred. Unblocks A9 completely and honestly, touches nothing in L5.
Recompute incurred costs on the fly at read time — rejected: recomputes costs already actually charged (drift risk from rounding / mid-session rule change) and puts 5.3 cost logic on the read path.

Decision: option 2 (serve gross only), option 1 logged as the deferred proper fix. Gross unrealized is the headline paper-P&L number most retail apps show anyway; net-of-cost is usually a close-time figure. The net_unrealized field is intentionally absent from the payload (not a 0 or a false flag) so the API does not imply a net number exists.

Deferred fix (tracked): Position entry-cost accumulator → net unrealized mark. Add IncurredEntryCosts to Position, wire into Convert + the Redis snapshot; then A9/A14 add NetUnrealized. Touches locked L5 — a scoped cross-layer change, done only when net mark is demo-critical.

3.8 Other decisions
Flat ≠ no-session. A live account holding nothing → 200 {positions:[]}; only no live session → 409. Consumers (agents especially) must distinguish "I am flat" from "I am not trading."
Available vs Reserved surfaced separately — free-to-sell vs locked behind resting sells.
4. A11 — Open-Order Read

What it serves. The account's live/working orders — id, instrument, side, type, status, remaining (unfilled) qty, filled qty, reserved amounts, cancel-in-flight. The "open orders" panel. Terminal orders (filled/cancelled/rejected) are not here; they are finalized off tracks and belong to trade history (A10).

Seams (verified). Each row is a two-source join:

from the broker's OrderTrack (OrderID, AccountID, Instrument, Status, CancelInFlight, IsMarketBuy): Status, Instrument, CancelInFlight, IsMarketBuy;
from L5's ReservationRecord (Side, IsMarket, ReservedCash, ReservedUnits, RemainingQty, OrigQty): the qty/reservation facts. filled_qty = OrigQty − RemainingQty (derived). This is exactly the Tier-1 discipline (exchange-seam facts on OrderTrack, reservation facts in L5) upheld at read time — neither side duplicates the other.

Join-key note (verified). s.tracks is map[OrderID]*OrderTrack — keyed by order, not account. To list an account's open orders the shard scans s.tracks filtered by AccountID. Bounded to one shard; fine for demo scale. A secondary accountID → []OrderID index is a possible future optimization, not needed now.

4.1 Endpoint
GET /v1/account/orders?account_id={id}                  → all open orders
GET /v1/account/orders?account_id={id}&instrument={sym} → open orders in one instrument

Open-only (Working / PartiallyFilled). No status= filter — terminal orders are not in tracks.

4.2 Reply payload
go
type OpenOrdersSnapshot struct {
    Present bool         // false → no live session (→409)
    Orders  []OrderView  // empty = live but nothing resting (→200 {orders:[]})
}
type OrderView struct {
    OrderID        OrderID
    Instrument     InstrumentID
    Side           Side          // Buy | Sell        (reservation record)
    Status         OrderStatus   // Working | PartiallyFilled (OrderTrack)
    IsMarket       bool          // market vs limit    (reservation record)
    OrigQty        int64
    RemainingQty   int64
    FilledQty      int64         // OrigQty − RemainingQty (derived)
    ReservedCash   int64         // locked (buys; 0 for sells)
    ReservedUnits  int64         // locked (sells; 0 for buys)
    CancelInFlight bool          // a cancel is punched, awaiting ack (OrderTrack)
    // NO LimitPrice today — see §4.5
}
4.3 Shard handler
handleReadOpenOrders(cmd):
  sess, ok := s.sessions[cmd.AccountID]
  if !ok: cmd.Reply <- OpenOrdersSnapshot{Present:false}; return
  views = []
  for oid, tr in s.tracks:                                   // scan this shard's tracks
      if tr.AccountID != cmd.AccountID: continue              // filter to this account
      if cmd.Instrument != "" && tr.Instrument != cmd.Instrument: continue
      if tr.Status != Working && tr.Status != PartiallyFilled: continue   // open only
      rec, ok := s.resv.Record(oid)                           // L5 reservation read accessor
      if !ok: continue                                        // track present, resv gone = mid-finalize; skip
      views = append(views, OrderView{
          OrderID:oid, Instrument:tr.Instrument, Side:rec.Side, Status:tr.Status,
          IsMarket:rec.IsMarket, OrigQty:rec.OrigQty, RemainingQty:rec.RemainingQty,
          FilledQty:rec.OrigQty - rec.RemainingQty,
          ReservedCash:rec.ReservedCash, ReservedUnits:rec.ReservedUnits,
          CancelInFlight:tr.CancelInFlight,
      })
  cmd.Reply <- OpenOrdersSnapshot{Present:true, Orders:views}

The !ok skip is defensive; since it is all one goroutine, a Working/Partial track will have its record.

4.4 Response shaping
!Present          → 409 {error:"no live session for account"}
Present, empty    → 200 {orders:[]}
Present, has rows → 200 {orders:[ {order_id, instrument, side, status, is_market, orig_qty,
                                   remaining_qty, filled_qty, reserved_cash, reserved_units,
                                   cancel_in_flight, unit:"paise"}, ... ]}
4.5 FINDING — a resting limit order's price is not retained broker-side

Exhaustively verified across L5 (in-memory structs, Redis, Postgres) and all broker docs: the limit price the user enters arrives on the inbound order, is used once to compute the reservation (requiredCash = LimitPrice × Qty, stored only as the product ReservedCash) and is forwarded to the exchange, and is then not retained by OrderTrack, ReservationRecord, ownedSession, the shard, Redis, or Postgres. The exchange's own order book is the only place a resting limit's price persists. (It cannot be reliably reconstructed: ReservedCash / RemainingQty recovers it for a resting buy but not for a sell — which reserves units, not cash — and is meaningless for market orders.)

Working assumption (Ansh, provisional): if the open-orders panel needs to show "resting at ₹X", add a LimitPrice field to OrderTrack set at intake (STEP 5 already has the value in hand — a one-field, LimitPrice: o.LimitPrice addition). May change later (e.g. read it back from the exchange resting-order state instead). Not built until the UI needs it.

4.6 Decision — reservation-info display skipped for now

Whether to surface reserved_cash / reserved_units to the consumer was raised; decided to skip reservation-detail display for now (the fields exist on OrderView for completeness but the panel need not show them). Minor; revisit if the UI wants it.

5. A14 — Agent Observation / REACT Snapshot

What it serves. The agent's "everything I need to decide this tick" read in one HTTP round-trip: balance + positions (gross marks) + open orders + recent closed trades. It exists so an agent gets its full world-state in one call instead of four (A8+A9+A11+A10-recent). The single-round-trip property is the whole reason it is its own surface.

Spec source (verified). BROKER_RESPONSIBILITIES_AND_PLAN.md: "Composed 'what I need to decide' read — balance + positions + marks + recent fills in one shot, shaped for the agent decision loop." Note it includes recent fills — a durable read, a different source from the live trio.

5.1 The core tension and the decision it forces

The four pieces come from two sources:

Piece	Source	Path
balance	live shard (in-memory Account.Cash)	funnel → owner
positions + gross mark	live shard (Position + LTP cache)	funnel → owner
open orders	live shard (tracks + reservation)	funnel → owner
recent closed trades	Postgres participant_trades	Store direct — NOT the shard

The recent-trades read is I/O and must not run on the shard goroutine (a DB call would block the single-writer behind network latency). So "one shot" means one HTTP call, fanned out internally into two concurrent reads — one shard command for the live trio + one Store Postgres read — merged on the request goroutine. The shard stays pure (no DB); the agent stays single-round-trip; total latency ≈ max(shard, Postgres), not the sum.

5.2 One combined shard command (not three)

A14 sends a single ReadObservationCmd, not three separate live commands. The shard reads all three in-memory structures in one goroutine turn and returns them together. Two wins: one inbox trip (not three), and a coherent-instant snapshot — balance, positions, and orders all reflect the same moment, with no mutation interleaving between them. The A8/A9/A11 shard logic is factored into shared helpers (copyCash, buildPositionViews, buildOrderViews) that both the standalone commands and A14 call — zero duplication.

5.3 Endpoint
GET /v1/account/observation?account_id={id}[&recent_n={k}]

recent_n optional (default 5, clamped [0,20]) — number of recent closed trades to include.

5.4 Payloads
go
// internal — the single combined live command's reply
type LiveObservation struct {
    Present   bool
    Cash      BalanceSnapshot   // A8 payload
    Positions []PositionView    // A9 payload (gross marks)
    Orders    []OrderView       // A11 payload
}
// external merged response:
// { present, cash{...}, positions[...], orders[...],
//   recent_trades[ {trade_id, instrument, direction, quantity, entry_price, exit_price,
//                   entry_at, exit_at, gross_pnl, total_costs, net_pnl, pnl_pct, exit_reason} ],
//   recent_trades_available:bool }
5.5 Handler — fan-out / merge (request goroutine)
handleGetObservation(w, r):
  ctx = r.Context(); accountID = parse (→400); recentN = parse (default 5, clamp [0,20])
  ctx, cancel = WithTimeout(ctx, readTimeout); defer cancel()
  // FAN OUT — two concurrent reads
  liveReply = make(chan LiveObservation, 1)
  route ReadObservationCmd{accountID, Reply:liveReply} → shard.inbox   (ctx-guarded)
  tradesCh = make(chan tradesResult, 1)
  go { rows, err := store.RecentTrades(ctx, accountID, recentN); tradesCh <- {rows, err} }
  // GATHER both (or time out)
  for i:=0; i<2; i++ {
      select { case live = <-liveReply: ; case trades = <-tradesCh: ; case <-ctx.Done(): → 504 }
  }
  → merge + shape
5.6 Shard handler (one turn, three reads)
handleReadObservation(cmd):
  sess, ok := s.sessions[cmd.AccountID]
  if !ok: cmd.Reply <- LiveObservation{Present:false}; return
  cmd.Reply <- LiveObservation{
      Present:true,
      Cash:      copyCash(sess.Account.Cash),                         // A8 helper
      Positions: buildPositionViews(sess.Account.Positions, s.marketData), // A9 helper (gross)
      Orders:    buildOrderViews(s.tracks, s.resv, cmd.AccountID),    // A11 helper
  }
5.7 Merge + shaping — degrade, don't fail
!live.Present  → 409 {error:"no live session for account"}          // agent not trading; nothing to observe
else 200 {
   present:true, cash:live.Cash, positions:live.Positions, orders:live.Orders,
   recent_trades: (trades.err==nil ? trades.rows : []),
   recent_trades_available: (trades.err==nil),
}

Degrade decision: if the live read says no-session → 409. But if the live read succeeds and only the Postgres recent-trades read fails, return the live state with recent_trades:[] and recent_trades_available:false — do not fail the whole observation. An agent needs its live state far more than history; losing history must not blind it to its current position. Same philosophy as A9's cold-LTP mark.

5.8 Decisions & rationale (recap)
One HTTP call, two concurrent internal reads, merged — "one shot" is at the agent boundary; internally a parallel shard+Postgres fan-out. Shard never does DB I/O.
Single combined shard command — one inbox trip, one coherent-instant snapshot.
Shard logic factored into shared helpers — A14 reuses A8/A9/A11 verbatim.
Recent trades from Postgres via the (account_id, exit_at DESC) index (Store.RecentTrades).
Degrade, don't fail — live-present but history-failed still returns a usable observation.
Gross-only marks — inherits A9's decision; net still deferred.
6. Store.RecentTrades — Postgres-direct recent-trades accessor

What it is. The broker's first Postgres-direct read: "the last N closed trades for an account, newest first." Backs A14, and is reused (with a cursor) by A10.

Schema (verified). participant_trades (append-only round-trip log; one row per logical closed trade):

trade_id PK, account_id FK, session_id, instrument_id, direction('long'|'short'), quantity,
entry_price(ticks), exit_price(ticks), entry_at, exit_at, gross_pnl(signed), total_costs(≥0),
net_pnl(signed), pnl_pct(REAL, signed, on net), exit_reason
INDEX ix_trades_account_recent (account_id, exit_at DESC)   -- the load-bearing "last N" index

Query (the schema doc hands the exact shape):

sql
SELECT <15 cols> FROM participant_trades
WHERE account_id = $1 ORDER BY exit_at DESC LIMIT $2

Index-only ordered scan on ix_trades_account_recent; no sort step.

Accessor & row type:

go
func (s *Store) RecentTrades(ctx context.Context, accountID AccountID, n int) ([]TradeRow, error)
type TradeRow struct {
    TradeID string; AccountID AccountID; SessionID SessionID; InstrumentID InstrumentID
    Direction string; Quantity int64; EntryPrice int64; ExitPrice int64
    EntryAt time.Time; ExitAt time.Time
    GrossPnl int64; TotalCosts int64; NetPnl int64; PnlPct float32; ExitReason string
}

Flow & decisions:

n <= 0 → return []TradeRow{}, nil with no query (empty ask = empty result, not an error).
ctx-honored — the query is bound to the caller's context; an A14 timeout / disconnect cancels the DB query too (no orphaned query outliving the request). This is why A14 can pass ctx into the goroutine.
Caller clamps upper bound; accessor also caps n at maxRecent defensively — no caller can request LIMIT 1000000.
Empty result ≠ error — a new account with zero trades returns []TradeRow{}, nil.
Returns raw rows, no shaping — integer ticks, raw enums; presentation happens at the response layer of the caller (A14 / A10 may shape differently). Consistent with the raw-buckets philosophy.

KNOWN PROPERTY — buffer-lag (decided: accept + document). Closed trades are written through the Redis write-behind buffer (l5:buf:trades) and drained to Postgres asynchronously. So a just-closed trade may sit in the buffer, not yet in Postgres, for one drain interval — a RecentTrades read (Postgres-only) will not see it yet. Options considered: (a) accept the lag; (b) read buffer + Postgres and merge; (c) force a flush before reading. Decided (a). The agent's actionable state (balance, positions, orders) is always shard-accurate in the same A14 call; only the backward-looking trade log lags by the drain interval. (b)/(c) trade real complexity/defeat write-behind for a cosmetic freshness gain. This is a known property, not a bug — documented so it is not mistaken for one later.

7. A10 — P&L / Trade-History Read

What it serves. The closed-trade history (paginated, newest first) plus an aggregate P&L summary across the account. Purely durable — reads participant_trades from Postgres, never the shard (closed trades are cross-session; the shard knows only the live session). The first fully-durable Tier-4 item: no funnel, no shard involvement at all.

7.1 Endpoint
GET /v1/account/trades?account_id={id}[&limit={n}][&before={exit_at_cursor}][&summary={bool}]
limit default 20, clamp [1,100].
before — a keyset pagination cursor (an exit_at timestamp; returns trades older than it). Keyset on the (account_id, exit_at DESC) index — not OFFSET (which degrades on deep pages and is unstable under inserts). Absent = newest page.
summary default true (see §7.5).
7.2 Two accessors
sql
-- Store.TradePage — RecentTrades + a cursor predicate
SELECT <15 cols> FROM participant_trades
WHERE account_id = $1 AND ($2::timestamptz IS NULL OR exit_at < $2)   -- keyset; NULL = newest page
ORDER BY exit_at DESC LIMIT $3
sql
-- Store.PnLSummary — the aggregate (one row, always, via coalesce/count)
SELECT count(*)                            AS trade_count,
       coalesce(sum(gross_pnl),0)          AS gross_pnl,
       coalesce(sum(total_costs),0)        AS total_costs,
       coalesce(sum(net_pnl),0)            AS net_pnl,
       count(*) FILTER (WHERE net_pnl > 0) AS win_count,
       count(*) FILTER (WHERE net_pnl < 0) AS loss_count
FROM participant_trades WHERE account_id = $1

TradePage's next_before = the exit_at of the last row returned (null if fewer than limit rows came back → last page). TradeRow type is shared with RecentTrades (TradePage = RecentTrades + cursor; RecentTrades stays as the no-cursor convenience for A14).

7.3 Handler — two concurrent reads
handleGetTrades(w, r):
  ctx = r.Context(); accountID = parse (→400); limit = clamp [1,100]; before = parse (bad→400)
  wantSummary = parse (default true)
  ctx, cancel = WithTimeout(...); defer cancel()
  go store.TradePage(ctx, accountID, limit, before) → listCh
  if wantSummary: go store.PnLSummary(ctx, accountID) → sumCh
  gather (ctx.Done → 504)
  → shape
7.4 Response shaping
list query err     → 500 {error:"trade history unavailable"}         // list is the core payload
summary query err  → DEGRADE: trades + summary:null + summary_available:false
empty account      → 200 {trades:[], next_before:null, summary:{all zeros}}
ok                 → 200 { present:true,
                            trades:[TradeRow-shaped...],
                            next_before:"<exit_at>"|null,
                            summary:{trade_count, gross_pnl, total_costs, net_pnl,
                                     win_count, loss_count, unit:"paise"} }
7.5 Decisions & rationale
Pure Postgres, no shard — the read-split rule in its clean form; first fully-durable Tier-4 item.
Keyset pagination (before cursor), not OFFSET — rides the index, stable under inserts, no deep-page degradation.
List + summary as two concurrent reads — same fan-out pattern as A14; max latency, not sum.
Summary FOLDED IN, opt out via summary=false (decided). Matches how a real trade-history screen renders (list + totals together); the opt-out keeps it cheap for callers that only want the list.
Degrade on summary failure — the list is the core payload; a failed aggregate returns summary:null, does not 500 the whole call. A list failure does 500 (it is the payload).
Win/loss by net_pnl sign — net>0 win, net<0 loss, net==0 breakeven (counted in neither).
No-account handling — an account with zero trades and a nonexistent account both return 200 with empties; A10 does not verify account existence (a nonexistent account simply has no trades). A hard 404 for unknown accounts is a separate existence check, not worth it for the demo.
Buffer-lag inherited from RecentTrades (§6).
FINDING (small, non-issue for now): PnLSummary sums over all an account's trades each call — trivially cheap at demo scale; at real scale, cache or maintain a running aggregate. Not built now.
8. A16 — Tax / Cost View (country-parametric)

What it serves. The itemized cost/tax breakdown for an account: per-trade breakdowns and the aggregate total across all trades, broken down by line item. Human-facing.

The load-bearing principle (country-agnostic — a locked platform rule). Costs/tax are driven by a country-agnostic rulebook with per-country slots (India filled, US empty — decision log). 5.3 represents cost output as a country-agnostic CostBreakdown{Lines map[string]int64, Total int64} — line items are generic name → amount, never hardcoded per-country keys anywhere above 5.3. A16's API contract mirrors that: country is a request parameter; country selects the cost table; line items are returned generically. (An earlier draft hardcoded India's stt/sebi/stamp into the response payload and keyed off the India-only table — that was wrong against this locked rule and was corrected.)

8.1 The country→table registry (the one place country binds to storage)
go
// The ONLY place a country maps to a cost table. Adding a country = one entry.
var costTableByCountry = map[string]string{
    "IN": "trade_costs",    // India (renames to india_trade_costs later — change here only)
    "US": "us_trade_costs", // future slot — table not built yet
}

Everything else is country-blind. Injection-safety: the table name comes only from this fixed allowlist (validated against the map's keys); a raw country string is never concatenated into SQL.

Schema direction (Ansh, logged): per-country tables (trade_costs → later india_trade_costs, plus us_trade_costs) — not a generic (trade_id, line_name, amount) rows table. Country is not stored on the trade or account (verified: rates are injected rulebook config, not per-account durable data), which is exactly why country must be a request param.

8.2 Endpoint
GET /v1/account/costs?account_id={id}&country={IN|US}[&limit={n}][&before={cursor}]
country is a required param, no default (defaulting would re-bake India). Missing or not in the registry → 400.
limit/before — keyset pagination for the per-trade list, same as A10.
8.3 Response — generic line items, per-trade + aggregate
json
{
  "present": true, "account_id": "...", "country": "IN",
  "per_trade": [
    { "trade_id":"...", "lines":[ {"name":"brokerage","amount":2000},
                                  {"name":"stt","amount":1500}, ... ], "total":4368 }, ...
  ],
  "next_before": "<exit_at>" | null,
  "aggregate": { "trade_count":42,
                 "lines":[ {"name":"brokerage","amount":84000}, ... ],  // each line summed across trades
                 "total":183456 },
  "unit": "paise"
}

lines is a generic [{name, amount}] list mirroring CostBreakdown — India returns Indian names, US will return sec/taf/finra; the JSON structure is identical. Frontends/agents render whatever line names arrive; nothing hardcodes stt.

8.4 Accessors (per country — quarantine the column knowledge)
go
func (s *Store) CostsPerTradeIN(ctx, accountID, limit, before) ([]TradeCostBreakdown, error)
func (s *Store) CostsAggregateIN(ctx, accountID) (CostAggregate, error)
type TradeCostBreakdown struct { TradeID string; Lines []Line; Total int64 }
type CostAggregate      struct { TradeCount int; Lines []Line; Total int64 }
type Line               struct { Name string; Amount int64 }
Per-trade: SELECT the (India) columns joined trade_costs→participant_trades on trade_id filtered by account_id; in Go map columns → []Line{{"brokerage",v},{"stt",v},...}, dropping zero-value lines (a trade with no auto-square fee does not list it).
Aggregate: SELECT sum(brokerage), sum(stt), ... over the account's trades, then the same column→line mapping.
The India-specific column knowledge lives only inside these India Store methods — the handler and response are country-blind. Exactly like 5.3: the India cost module knows India's line names internally; Convert above it sees only map[string]int64.

trade_costs schema (verified): 1:1 child of participant_trades, PK = trade_id; India columns brokerage, stt, exchange, sebi, stamp, gst, autosquare_fee, total.

8.5 Handler — country dispatch
handleGetCosts(w, r):
  ctx = r.Context()
  accountID = parse (→400)
  country   = parse; uppercase; if country not in costTableByCountry → 400 {error:"unsupported country"}
  limit/before as A10
  dispatch:
    "IN": perTradeFn=CostsPerTradeIN; aggFn=CostsAggregateIN
    "US": → 501 {error:"US cost view not yet available"}      // slot defined, table empty
  ctx, cancel = WithTimeout(...); defer cancel()
  two concurrent Store reads (per-trade page + aggregate)     // A10 fan-out pattern
  gather (ctx.Done → 504); per-trade err → 500; aggregate err → degrade (aggregate:null)
  → shape 200
8.6 Decisions & rationale
Country required, no default — the system does not store a trade's country; the caller states it.
Country → table via fixed allowlist registry — one place, injection-safe, the only country→storage binding.
Generic {name, amount} line items — mirrors CostBreakdown; frontends/agents hardcode no names.
Per-country column knowledge quarantined in per-country Store methods — everything above is country-blind, exactly like 5.3's CostModule.
Both views — per-trade breakdown (paginated) + account aggregate (per-line summed + grand total + count). The aggregate is the "total tax/cost you paid", line-item-broken-down, not one lump.
US = honest 501 — slot defined, table empty; same posture as the empty US cost module.
Zero-value lines dropped from per-trade breakdowns.
Same buffer-lag as A10 — trade_costs writes through the same write-behind buffer and drains with its parent trade, so A10 and A16 stay mutually consistent (if a trade shows in history, its cost row is there too).
9. A3-endpoint — Modify Order (compose cancel + place)

What it serves. A single consumer-facing "modify my order" HTTP action. There is no handleModify primitive (Tier-1 decision): modify = cancel + place. This endpoint is the interface-tier composer that presents that as one action and stitches the two local results into one response. It is a write surface (grouped in Tier 4 as an interface-tier compose item, alongside A13).

Seam (verified — and it reshapes the failure model). From the Tier-1 order core:

handleCancel is async and releases nothing — it validates, sets CancelInFlight = true, punches the cancel, and returns CancelPending immediately (in-memory check + channel send — microseconds, not an exchange round-trip). The real cancel outcome (Cancelled / CancelRejected) arrives later via settlement (B3).
Modify = punch cancel, then immediately punch place, no waiting (doc line 215); same-shard channel order guarantees the exchange processes the cancel before the new place.

So both operations are fire-and-forget punches returning "pending"; there is no synchronous "cancel-executed-but-place-failed" moment. The endpoint composes two local acceptance results, not two execution results.

9.1 Endpoint
POST /v1/orders/modify
body: { account_id, order_id, new_qty, new_price?, side, instrument, is_market }

POST, not idempotent — it creates a new order with a fresh OrderID; replaying it would cancel+place again. Body (structured write params). account_id explicit (no auth).

9.2 The compose — Option 1 (gate on fast LOCAL acceptance), decided
handleModifyOrder(w, r):
  1. parse + validate body → 400 on missing account_id/order_id/terms
  2. ctx, cancel = WithTimeout(...); defer cancel()
  3. STEP A — punch cancel
     cancelReply = make(chan CancelResult, 1)
     route CancelOrderCmd{account_id, order_id, Reply:cancelReply} → shard.inbox
     cancelRes = <-cancelReply   (ctx.Done → 504)
  4. GATE on cancel's LOCAL acceptance:
     if cancelRes.Status == CancelFailed:
         → DO NOT place. Return 409 with cancelRes.Reason. Original order UNTOUCHED.
     // CancelPending (incl. idempotent already-in-flight) → proceed
  5. STEP B — punch replacement place (same shard, ordering preserved)
     placeReply = make(chan PlaceResult, 1)
     route PlaceOrderCmd{account_id, new terms} → shard.inbox
     placeRes = <-placeReply   (ctx.Done → 504)
  6. compose response from BOTH local results → shape

Why Option 1 (gate on local acceptance), and why not the alternatives:

Option 1 — gate on fast local acceptance (CHOSEN). The "wait" is two microsecond in-memory shard hops, not an exchange round-trip — the user is not meaningfully blocked. The gate buys real correctness: if the cancel fails its local pre-check (unknown order, already filled → not cancellable), the replacement is never punched, so the original is left exactly as it was and a "modify a filled order" does not silently become a naked new order.
Option 2 — fire both punches together, no gate (rejected). Marginally faster (sub-millisecond either way), but punches the replacement even when the cancel was doomed at the local pre-check → leaks a naked order on the "nothing to cancel" case, and forces unwinding a placed order's reservation — more work and more race surface than simply not placing it.
Option 3 — wait for the exchange to actually confirm the cancel, then place (rejected). This is the bad blocking: holding the HTTP request open across the full async settlement round-trip, plus correlating that async result back to a waiting request. This is the "don't block the user" concern — and it is exactly what Option 1 avoids. Both Option 1 and Option 2 return before the cancel completes at the exchange; only Option 3 blocks on the slow path.
9.3 The partial-failure seam (cancel accepted, place rejected at Gate 4)

The cancel is in-flight (punched, will likely cancel the old order) but the replacement was locally rejected (e.g. insufficient funds at the new price/qty). We cannot cleanly roll back a punched cancel — it may already be processing at the exchange. So the endpoint reports the state truthfully rather than faking or undoing it:

json
{ "modify_status":"partial",
  "cancel":{"status":"pending","order_id":"old-123"},
  "replace":{"status":"rejected","reason":"insufficient_funds"},
  "warning":"original order is being cancelled; replacement was rejected — you may be left with no order" }

No funds pre-check before punching the cancel — rejected as TOCTOU-racy (funds can change between pre-check and the real Gate 4) and duplicative of Gate 4. Punch, gate on the real result, report.

9.4 Response shaping
400 → bad/missing body
504 → either punch timed out
cancel CancelFailed        → 409 {modify_status:"rejected", cancel:{failed, reason}, replace:null}  // nothing changed
cancel pending + place ok  → 200 {modify_status:"ok",
                                   cancel:{pending, old_order_id}, replace:{accepted, new_order_id}}
cancel pending + place rej → 200 {modify_status:"partial", cancel:{pending},
                                   replace:{rejected, reason}, warning:...}

modify_status:"ok" means both punches were locally ACCEPTED — not executed. Real cancel/fill outcomes arrive async via settlement (B3) and appear in later A11/A14 reads. The endpoint never claims "done." (200 + status-field for the partial case, not 207 Multi-Status — simpler for consumers.)

9.5 Decisions & rationale (recap)
Compose, no primitive — reuses handleCancel + handlePlaceOrder unchanged.
Cancel gates place (Option 1) — non-blocking in the way that matters; prevents the naked-order footgun.
No rollback of a punched cancel — the partial case is reported, not undone (undoing fights the async discipline → worse races).
No funds pre-check — TOCTOU-racy, duplicative of Gate 4.
POST, not idempotent — fresh OrderID; not safe to replay.
ok = accepted, not executed — real outcomes are async.
10. Deferred within Tier 4 (conscious, with reasons)
10.1 A13 — Bulk / batched actions — DEFERRED (optimization, build-if-needed)

"Place/cancel many orders in one HTTP call" for agent-population efficiency. Deferred as a pure ingress optimization with no correctness content. Reasons: orders still process one-at-a-time per shard (single-writer) — A13 batches the HTTP call, not the execution; a batch spanning accounts fans out to different shards anyway. The demo can drive agents with N single B2/A2 calls; A13 only earns its keep at scale or under a latency budget not yet established. It is a clean bolt-on later — a thin wrapper over B2/A2 that changes nothing downstream, same cost now or later.

When built, the spec is fixed: one endpoint, typed place/cancel elements; cross-shard fan-out (group by shard → enqueue each → gather — A14's pattern generalized to writes); per-order independent results (best-effort, not all-or-nothing — orders are independent, no cross-account transaction, and rolling back good reserves for one failure is both wrong and hard); FIFO within an account (same inbox, array order).

10.2 Net-unrealized mark — DEFERRED (needs an L5 write-path change)

A9/A14 serve gross-only marks. Net requires an IncurredEntryCosts accumulator on Position, wired into Convert + the Redis snapshot — a change to locked L5's write path, not a Tier-4-internal change. See §3.7. Done only when net mark is demo-critical.

11. Tier-4 status summary
ID	Surface	Status	Source
A8	Balance read	Designed	live / funnel
A9	Position read (gross marks)	Designed	live / funnel + MarketDataCache iface
A11	Open-order read	Designed	live / funnel (OrderTrack ⋈ L5 reservation)
A14	Observation / REACT snapshot	Designed	live funnel ⊕ Postgres (fan-out)
A10	Trade history / P&L	Designed	Postgres (Store)
A16	Cost / tax view (country-parametric)	Designed	Postgres (Store), per-country tables
A3-endpoint	Modify order (compose)	Designed	interface-tier compose of cancel+place
Store.RecentTrades	recent-trades accessor	Built	Postgres
Store.TradePage / PnLSummary	history + aggregate	Designed	Postgres
Store.CostsPerTradeIN / CostsAggregateIN	India cost reads	Designed	Postgres
A13	Bulk / batched actions	Deferred (optimization)	—
Net-unrealized mark	net paper P&L	Deferred (L5 write-path change)	—

Cross-tier dependencies noted:

A9/A14 live marks depend on the MarketDataCache whose writer is B10 (Tier 5) — coded against the interface now; marks degrade (mark_available:false) until Tier 5 populates it.
Everything account-scoped assumes a live session exists on a shard — session spin-up/tear-down is Tier 3 (paused pending L7). Tier-4 reads return 409 when no live session is present.