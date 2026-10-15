Broker — Phase 6 Implementation Design (Tier 4 — Read Surfaces)

Scope: the eight account GET read endpoints — A8 balance, A9 positions, A11 open/working orders, A14 agent observation, A10 trade history + P&L, A16 cost/tax view — their shard handlers, the broker command/facade seams they ride, the shared GET-read skeleton, and every cross-layer touch each one needs (L5 economy/reserve edits, the MarketPriceCache interface, bringup wiring).

Status: all six points LOCKED. The two previously-held items are resolved (timeout = configurable; money = bare units, no currency — see §0.6). One real L5 change (shared with Phase 5). Marks depend on a Phase-7 cache (degrade-don't-fail until then).

Authority (design): BROKER_READ_SURFACES_tier4.md (the Tier-4 design, §1 shared read pattern, §2 A8, §3 A9, §7 A10, §8 A16, §A11/§A14), BROKER_IMPLEMENTATION_SEQUENCE.md (Phase 6), BROKER_TIER0_IMPLEMENTATION_DESIGN.md (the funnel/command/reply substrate), BROKER_PHASE1_IMPLEMENTATION_DESIGN.md (the Store durable layer), BROKER_PHASE3_IMPLEMENTATION_DESIGN.md (the HTTP handler pattern), BROKER_PHASE5_IMPLEMENTATION_DESIGN.md (the order core + the P5-D22 completion-read assumption this phase resolves).

Authority (source — verified against the real code this phase was written against):

broker/funnel/ — commands.go, results.go, broker.go, shard.go, shard_handlers.go, track.go, session.go, types.go, reply.go
broker/store/ — trades.go, costs_in.go, types.go
broker/transfer/ — http.go, config.go, ports.go (the HTTP-handler precedent)
broker/bringup/ — config.go, country.go
economy/account/ — types.go (Cash, Account, Position, Lot), position.go, reads.go
economy/realize/ — compute.go (ComputeWeightedAvgEntry, ComputeUnrealizedMark)
economy/reserve/ — book.go (ReservationRecord, ReservationBook), service.go, reserve.go, types.go (Order)
economy/lifecycle/ — types.go (LoadedAccount)
0. Orientation
0.1 What already exists in code (we only fill seams)

The read path is wired end-to-end except the bodies. Do not re-create these:

Commands ReadBalance{AccountID, ReplyTo} and ReadPositions{AccountID, ReplyTo} — commands.go.
Facades Broker.ReadBalance(ctx, aid) and Broker.ReadPositions(ctx, aid) — broker.go (each: fresh buffered(1) reply → route → select reply-vs-ctx → ErrBrokerTimeout).
Dispatch cases for both — shard_handlers.go.
Store durable reads — all built & tested in store/trades.go + store/costs_in.go: RecentTrades, TradePage, PnLSummary, CostsPerTradeIN, CostsAggregateIN, plus the shapes TradeRow, PnLSummary, Line, TradeCostBreakdown, CostAggregate.
The country allowlist pattern — bringup/country.go (buildCountryStack); A16 mirrors it on the read side.
The HTTP handler pattern — transfer/http.go (+ config.go, ports.go): a per-concern package, NewHandler(ports…, cfg), exported const Path, method guard → recover→500 → parse→400 → ctx timeout → facade/store call → status dispatch.
0.2 What Phase 6 adds
Handler bodies for handleReadBalance / handleReadPositions (currently return XSnapshot{} stubs).
Snapshot fields on BalanceSnapshot / PositionsSnapshot (currently empty struct{}).
Two new commands + facades + dispatch + handlers: ReadOpenOrders (A11), ReadObservation (A14).
Shared shard read helpers so A8/A9/A11 bodies and A14's composite share one implementation.
A new broker/reads HTTP package carrying all eight endpoints, its ports + Config.
Cross-layer edits (see §0.4): the L5 economy/reserve change (A11), and the MarketPriceCache interface + nil-able shard field (A9/A14).
0.3 Live vs durable — the two read families
Live reads (A8, A9, A11, A14): go through the funnel to the owning shard; the data is in-memory L5 state. !Present (no live session) → 409. Timeout → 504 (ErrBrokerTimeout).
Durable reads (A10, A16): go straight to Store (Postgres), off-shard, no funnel, no live session needed. Empty = 200 (never 404/409). Infra failure (ErrLoadFailed) → 503.
A14 straddles: one live shard read plus one durable Store.RecentTrades, fanned out concurrently.
0.4 CROSS-LAYER WORK — the complete list (read this before implementing)

Phase 6 is not self-contained. These are every non-broker/reads change it requires:

#	Layer / package	Change	Needed by	Blocking?
X1	L5 economy/reserve/book.go	add field LimitPrice int64 to ReservationRecord	A11, A14	yes — in-repo edit
X2	L5 economy/reserve/reserve.go	set LimitPrice: order.LimitPrice in both reserveBuy and reserveSell record builds	A11, A14	yes — in-repo edit
X3	L5 economy/reserve/service.go	add exported View(orderID) (ReservationRecord, bool) (copy-out via service.book.get)	A11, A14 (also resolves Phase-5 P5-D22)	yes — in-repo edit
X4	broker/funnel	extend the built MarketPriceCache interface (currently `BestAsk(instrument)` only, Phase-5) by (a) adding `LastPrice`, (b) keying BOTH methods by `(ExchangeID, InstrumentID)` — supersedes the single-arg sketch below; (c) adding mktCache field to shard with nil → nopMarketCache{} substitution in newShard; (d) threading mktCache through NewBroker/newShard (nil-safe)	A9, A14 marks (also Phase-5 market-buy's existing BestAsk call)	interface + nop impl now; real writer = Phase 7
X5	broker/funnel	new commands/facades/dispatch/handlers ReadOpenOrders, ReadObservation; fill handleReadBalance/handleReadPositions; fill snapshot structs; add shared read helpers	A8/A9/A11/A14	yes — in-repo edit
X6	broker/bringup	supply reads.Config{ReadTimeout}; register the eight read routes on the HTTP mux; pass the (nil for now) MarketPriceCache to NewBroker	all	yes — wiring
X7	Phase 7 (market-data block)	implement the real MarketPriceCache (one ingestor goroutine per exchange — same pattern as the settlement consumer — stamps its own ExchangeID on each Public print and writes under that key, so the cache is implicitly map[ExchangeID]map[InstrumentID]… with single-writer-per-(ExchangeID,InstrumentID) by construction) + wire it into NewBroker	A9/A14 marks light up	deferred — A9/A14 degrade until then
X8	Tier-1 order intake (Phase 5, currently a stub)	populates shard.tracks + reservation records	A11/A14 show orders once it lands	dependency, not a Phase-6 edit
0.5 The GET-read skeleton (established at A8, reused by every live read)

Every live-read HTTP handler is the same five layers:

Endpoint — GET, params in the query string, no auth (account_id is the trusted param).
Handler (request goroutine) — method guard (GET), recover→500, parse+validate→400, ctx with cfg.ReadTimeout, call the facade, map result→status.
Funnel — the facade's route(cmd) (Tier-0, unchanged).
Shard handler — runs on the owner goroutine; returns a by-value snapshot; dispatch sends it on ReplyTo.
Shaping — !Present→409, timeout→504, success→200 JSON.

The shard signals "no live session" with a Present bool on the snapshot (a channel cannot carry an error); the HTTP layer maps Present:false→409.

0.6 Money = bare units (no currency) — LOCKED, with a carried issue

All account money figures (balance buckets, position marks, reserved amounts, order reserved cash/units, trade P&L gross/net/costs, cost totals/lines) are emitted as plain int64 units with NO unit/currency field. Taxes/costs (A16) are still computed per the chosen market (country-parametric), but their amounts are presented the same way — plain integer units.

KNOWN DELIBERATE CARRIED ISSUE (fix later): there is no currency/FX model. 1 unit ≡ 1 INR ≡ 1 USD, no conversion. Multi-currency-within-an-account and FX-on-bank-transfers are out of scope. OHLCV (Phase 7) may show currency later.

This supersedes BROKER_READ_SURFACES_tier4.md §0.3's provisional "unit":"paise" and retires P6-D6 (there is no unit field). transfer's response already emits no unit, so the whole account money surface is now uniform: integers, no labels.

0.7 Timeout — configurable, LOCKED

The broker/reads package carries Config{ ReadTimeout time.Duration } with a Validate() that rejects <= 0 (a zero deadline collapses every request to instant DeadlineExceeded). The value is supplied at bringup, not pinned here — exactly mirroring transfer.Config.TransferTimeout. The one ctx bounds the live funnel round-trip and (for A10/A16) the durable Store query.

0.8 Copy-out rule (single-writer safety)

A read handler runs on the owner goroutine and must return by-value data only — never a pointer into the live *Account / *ReservationRecord / lot slice. Cash is a value struct; View returns *record dereferenced; position views carry scalars. Nothing that aliases live state escapes the shard.

1. Shared substrate
1.1 The broker/reads HTTP package (new)

One package owns all eight read endpoints (they're all GETs sharing the skeleton — unlike transfer, which is one concern). Files:

broker/reads/
  config.go      // Config{ReadTimeout} + Validate   (mirror transfer/config.go)
  ports.go       // brokerPort + storePort interfaces (mirror transfer/ports.go)
  common.go      // shared helpers: parseAccountID, writeJSON, writeError, status mapping
  balance.go     // A8  GET /v1/account/balance
  positions.go   // A9  GET /v1/account/positions
  orders.go      // A11 GET /v1/account/orders
  observation.go // A14 GET /v1/account/observation
  trades.go      // A10 GET /v1/account/trades   + GET /v1/account/pnl
  costs.go       // A16 GET /v1/account/costs     + GET /v1/account/costs/summary

config.go:

go
package reads

type Config struct { ReadTimeout time.Duration }

var ErrBadConfig = errors.New("broker/reads: bad Config")

func (cfg Config) Validate() error {
    if cfg.ReadTimeout <= 0 {
        return fmt.Errorf("ReadTimeout=%v (must be > 0): %w", cfg.ReadTimeout, ErrBadConfig)
    }
    return nil
}

ports.go (both ports; live handlers use brokerPort, durable handlers use storePort, A14 uses both):

go
package reads

type brokerPort interface {
    ReadBalance(ctx context.Context, accountID funnel.AccountID) (funnel.BalanceSnapshot, error)
    ReadPositions(ctx context.Context, accountID funnel.AccountID, instrument funnel.InstrumentID) (funnel.PositionsSnapshot, error)
    ReadOpenOrders(ctx context.Context, accountID funnel.AccountID) (funnel.OpenOrdersSnapshot, error)
    ReadObservation(ctx context.Context, accountID funnel.AccountID) (funnel.ObservationLive, error)
}
type storePort interface {
    RecentTrades(ctx context.Context, accountID store.AccountID, n int) ([]store.TradeRow, error)
    TradePage(ctx context.Context, accountID store.AccountID, limit int, before *time.Time) ([]store.TradeRow, error)
    PnLSummary(ctx context.Context, accountID store.AccountID) (store.PnLSummary, error)
    CostsPerTradeIN(ctx context.Context, accountID store.AccountID, limit int, before *time.Time) ([]store.TradeCostBreakdown, error)
    CostsAggregateIN(ctx context.Context, accountID store.AccountID) (store.CostAggregate, error)
}

*funnel.Broker and *store.Store satisfy these in production; tests inject stubs. Each handler is built by a NewXHandler(port, cfg) http.Handler and exposes a const PathX.

1.2 Shared HTTP helpers (common.go)
go
func writeJSON(w http.ResponseWriter, status int, body any) { /* set header, WriteHeader, Encode, debug-log on write err */ }
func writeError(w http.ResponseWriter, status int, msg string) { writeJSON(w, status, map[string]string{"error": msg}) }

// query-param account_id, required
func parseAccountID(r *http.Request) (string, bool) {
    aid := r.URL.Query().Get("account_id")
    return aid, aid != ""
}
// optional before= RFC3339 cursor → *time.Time (nil when absent); ok=false on malformed
func parseBefore(r *http.Request) (*time.Time, bool) { /* "" → (nil,true); parse RFC3339 else (nil,false) */ }
// optional limit= int with default; ignore >maxRecent (store caps anyway)
func parseLimit(r *http.Request, def int) (int, bool) { /* "" → (def,true); Atoi else (0,false) */ }

Common recover+method-guard wrapper (same shape as transfer's ServeHTTP preamble) applied to every handler.

1.3 MarketPriceCache interface + shard field (cross-layer X4) — RESOLVED

LOCKED decision — exchange keying. The exchange wire does NOT carry an ExchangeID (verified against `contract/` — `InboundMessage`/`EngineResult`/`MatchResult`/`Trade`/`RestingOrder` all carry `BrokerID` but no `ExchangeID`). ExchangeID is a broker-side identifier that L7 stamps at session binding (`SessionBinding.ExchangeID`, "routing sanity / logging"). The Phase-7 ingestor goroutine knows its own ExchangeID the same way the Phase-4 settlement consumer does — WireInstance hands it in at construction. So the ingestor stamps ExchangeID on receipt and writes under that key; the cache is implicitly `map[ExchangeID]map[InstrumentID]…` with single-writer-per-(ExchangeID,InstrumentID) by construction. Readers pass `sess.binding.ExchangeID` (which the shard already carries from session install).

The file already exists as broker/funnel/mkt_cache.go (built in Phase 5 with just BestAsk). Phase 6 EXTENDS it in place — do NOT create a new funnel/marketdata.go:

go
// MarketPriceCache is the market-data read seam shared by Phase-5 Point-1
// (market-buy pricing) and Phase-6 Point-2 (position mark). Concrete impl
// is Tier-5 (B10 tape relay), not built; this package knows only the
// interface and the production stub below. Both methods are keyed by
// (ExchangeID, InstrumentID); readers pass sess.binding.ExchangeID.
type MarketPriceCache interface {
    // BestAsk returns the best-ask tick for an instrument on the named
    // exchange, or ok=false on a cold cache. Phase-5 market-buy rejects
    // with ReasonNoMarketPrice on ok=false.
    BestAsk(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)

    // LastPrice returns the last-traded tick for an instrument on the
    // named exchange, or ok=false on a cold cache. Phase-6 A9 degrades
    // to MarkAvailable:false on ok=false; position + avg still returned.
    LastPrice(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)
}

// nopMarketCache is the production stand-in: both methods always miss.
// Phase-5 market-buy rejects with ReasonNoMarketPrice; Phase-6 marks
// return MarkAvailable:false. Also used as the newShard substitution
// when the caller passes a nil mktCache.
type nopMarketCache struct{}
func (nopMarketCache) BestAsk(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool)   { return 0, false }
func (nopMarketCache) LastPrice(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) { return 0, false }

Shard field (shard.go) — unchanged shape, documented role expanded:

go
type shard struct {
    inbox    chan Command
    brokerID BrokerID
    sessions map[AccountID]*ownedSession
    tracks   map[OrderID]*OrderTrack
    seq      uint64
    mktCache MarketPriceCache // Phase-5 BestAsk + Phase-6 LastPrice seam; non-nil post-construction (newShard substitutes nopMarketCache{} on nil input)
    slippageBufferBps int64
    bundle   *capstone.Bundle
    resv     *reserve.ReservationService
    realiz   *realize.Realizer
}

Thread one shared instance through construction. newShard's nil-substitution means callers (bringup, funnel-internal tests) that have no real cache to inject can pass nil without the Phase-5 place handler nil-dereferencing on `mktCache.BestAsk(...)`:

go
func NewBroker(brokerID BrokerID, inboxDepth int, bundles []*capstone.Bundle, store *Store, money MoneyConfig, mktCache MarketPriceCache) *Broker { ... }
func newShard(brokerID BrokerID, inboxDepth int, bundle *capstone.Bundle, mktCache MarketPriceCache) *shard {
    if mktCache == nil { mktCache = nopMarketCache{} }
    ...
}

Bringup passes nil for now (X6); Phase 7 passes a real cache (B10 ingestor-fed). Because newShard substitutes, mark branches do NOT need `if ownerShard.mktCache != nil` guards — the field is always safe to dereference. Degrade logic is driven by the method's ok=false return, not by nil-checks.

Note: adding the NewBroker param touches a built signature. Keep it last in the arg list; bringup is the only caller. (Funnel-internal tests that build a Broker directly pass nil.) The place.go call site is updated from `mktCache.BestAsk(order.Instrument)` to `mktCache.BestAsk(liveSession.binding.ExchangeID, order.Instrument)` — Phase-5 code edited in place (one line).

Cross-doc correction. The X4 row above supersedes BROKER_READ_SURFACES_tier4.md §3.1 which showed single-arg method sketches — those predate the ExchangeID-keying resolution; the interface is `(ExchangeID, InstrumentID)` on both methods.

1.4 Shared shard read helpers (shard_handlers.go or a new funnel/reads.go)

One implementation each, reused by the single-read handlers and the A14 composite. All take an already-resolved live session / shard and return by-value views:

go
// readBalance — the 4-bucket copy-out (A8 body + A14).
func readBalance(sess *ownedSession) BalanceSnapshot {
    c := sess.account.Account.Cash
    return BalanceSnapshot{Present: true, Available: c.Available, Reserved: c.Reserved, Settled: c.Settled, Bank: c.Bank}
}

// readPositions — positions + gross marks; instrument=="" means all (A9 body + A14).
func readPositions(ownerShard *shard, sess *ownedSession, instrument InstrumentID) []PositionView {
    acct := sess.account.Account
    views := make([]PositionView, 0, len(acct.Positions))
    emit := func(instr InstrumentID, pos *account.Position) {
        if pos.Available == 0 && pos.Reserved == 0 { return } // flat in this instrument → no row
        avg, _ := realize.ComputeWeightedAvgEntry(pos.OpenLots)
        v := PositionView{Instrument: instr, Qty: pos.Available, ReservedQty: pos.Reserved, WeightedAvgEntry: avg}
        // mktCache is always non-nil (newShard substitutes nopMarketCache{}); nopMarketCache.LastPrice
        // misses → MarkAvailable stays false. Readers key by (sess.binding.ExchangeID, instrument).
        if ltp, ok := ownerShard.mktCache.LastPrice(sess.binding.ExchangeID, instr); ok {
            gross, _ := realize.ComputeUnrealizedMark(pos.OpenLots, ltp, 0) // incurredEntryCosts=0 → take GROSS
            v.MarkAvailable, v.LTP, v.GrossUnrealized = true, ltp, gross
        }
        views = append(views, v)
    }
    if instrument == "" {
        for instr, pos := range acct.Positions { emit(instr, pos) }
    } else if pos, ok := acct.Positions[instrument]; ok {
        emit(instrument, pos)
    }
    return views
}

// readOpenOrders — join tracks (status) + reservation View (qty/side/price) (A11 body + A14).
func readOpenOrders(ownerShard *shard, accountID AccountID) []OpenOrderView {
    orders := make([]OpenOrderView, 0)
    for orderID, track := range ownerShard.tracks {
        if track.AccountID != accountID { continue }
        v := OpenOrderView{OrderID: orderID, Instrument: track.Instrument, Status: track.Status, CancelInFlight: track.CancelInFlight}
        if rec, ok := ownerShard.resv.View(orderID); ok { // NEW L5 accessor (X3)
            v.Side, v.IsMarket, v.LimitPrice = rec.Side, rec.IsMarket, rec.LimitPrice
            v.OrigQty, v.RemainingQty = rec.OrigQty, rec.RemainingQty
            v.ReservedCash, v.ReservedUnits = rec.ReservedCash, rec.ReservedUnits
        }
        orders = append(orders, v)
    }
    return orders
}

Naming note: these are read helpers (not "build") — they read live state into view structs. The handlers below are thin wrappers that add the liveness/Present check.

2. Point 1 — A8 Balance

Endpoint: GET /v1/account/balance?account_id={id} Built already: ReadBalance command, Broker.ReadBalance facade, dispatch case. New: fill BalanceSnapshot, fill handleReadBalance, add the HTTP handler. Cross-layer: none.

2.1 Persistence question

None — pure in-memory read.

2.2 Scope
Is: 4-bucket copy-out + endpoint.
Isn't: derived views (equity/buying-power); durable standing-balance for a logged-out account (a separate future endpoint).
2.3 Types (results.go — fill the empty struct)
go
type BalanceSnapshot struct {
    Present   bool  // false → no live session → 409
    Available int64 // SIGNED (debt may be negative); raw units, no currency
    Reserved  int64
    Settled   int64
    Bank      int64
}
2.4 Shard handler (shard_handlers.go — fill the stub)
go
func (ownerShard *shard) handleReadBalance(cmd ReadBalance) BalanceSnapshot {
    sess, live := ownerShard.sessions[cmd.AccountID]
    if !live { return BalanceSnapshot{Present: false} } // → 409
    return readBalance(sess)
}

sess.account is a lifecycle.LoadedAccount; cash path is sess.account.Account.Cash (verified — the real handleTransfer uses exactly this). No accessor fits all four buckets (account.ReadAvailableCash() returns only Available), so the helper copies the Cash value fields directly.

2.5 HTTP handler (reads/balance.go)
go
const PathBalance = "/v1/account/balance"

func (h *balanceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // method guard GET; recover→500 (shared preamble)
    aid, ok := parseAccountID(r); if !ok { writeError(w, 400, "account_id required"); return }
    ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ReadTimeout); defer cancel()
    snap, err := h.broker.ReadBalance(ctx, funnel.AccountID(aid))
    switch {
    case errors.Is(err, funnel.ErrBrokerTimeout): writeError(w, 504, "balance read timed out")
    case err != nil:                              writeError(w, 500, "internal error")
    case !snap.Present:                            writeError(w, 409, "no live session for account")
    default: writeJSON(w, 200, map[string]any{
        "available": snap.Available, "reserved": snap.Reserved, "settled": snap.Settled, "bank": snap.Bank})
    }
}
2.6 Status map

400 missing account_id · 504 ErrBrokerTimeout · 500 unexpected · 409 !Present · 200 buckets (no unit).

2.7 Files touched

funnel/results.go (BalanceSnapshot), funnel/shard_handlers.go (handleReadBalance), funnel/reads.go (readBalance helper), reads/balance.go (new), reads/ports.go (ReadBalance).

3. Point 2 — A9 Positions (gross-only marks)

Endpoint: GET /v1/account/positions?account_id={id}[&instrument={sym}] Built already: ReadPositions command, Broker.ReadPositions facade, dispatch case. New: fill PositionsSnapshot, extend the command + facade with an instrument filter, fill handleReadPositions, add the HTTP handler. Cross-layer: X4 (MarketPriceCache) for the mark — degrades until Phase 7. The net-unrealized gap (below) stays unfilled.

3.1 Persistence question

None.

3.2 Scope
Is: positions + weighted-avg entry + gross mark; one instrument or all.
Isn't: net unrealized (gap — §3.8); the MarketPriceCache itself (Phase 7); portfolio totals.
3.3 Command extension (commands.go) — Option A, shard-side filter
go
type ReadPositions struct {
    AccountID  AccountID
    Instrument InstrumentID // "" = all held; else filter to this one
    ReplyTo    chan PositionsSnapshot
}

Facade (broker.go) gains the param:

go
func (broker *Broker) ReadPositions(ctx context.Context, accountID AccountID, instrument InstrumentID) (PositionsSnapshot, error) {
    replyChannel := make(chan PositionsSnapshot, 1)
    broker.route(ReadPositions{AccountID: accountID, Instrument: instrument, ReplyTo: replyChannel})
    select {
    case reply := <-replyChannel: return reply, nil
    case <-ctx.Done():            return PositionsSnapshot{}, ErrBrokerTimeout
    }
}

(A14 calls it with "".)

3.4 Types (results.go)
go
type PositionsSnapshot struct {
    Present   bool           // false → 409
    Positions []PositionView // empty slice = live but FLAT → 200 {positions:[]}
}
type PositionView struct {
    Instrument       InstrumentID
    Qty              int64 // Available — free to sell
    ReservedQty      int64 // locked behind resting sells
    WeightedAvgEntry int64 // realize.ComputeWeightedAvgEntry
    MarkAvailable    bool  // false when mktCache cold/absent (Phase 7 not up)
    LTP              int64 // 0 when !MarkAvailable
    GrossUnrealized  int64 // 0 when !MarkAvailable
    // NO NetUnrealized field — see §3.8 (absent, not 0, so the payload stays honest)
}
3.5 Shard handler (shard_handlers.go)
go
func (ownerShard *shard) handleReadPositions(cmd ReadPositions) PositionsSnapshot {
    sess, live := ownerShard.sessions[cmd.AccountID]
    if !live { return PositionsSnapshot{Present: false} } // → 409
    return PositionsSnapshot{Present: true, Positions: readPositions(ownerShard, sess, cmd.Instrument)}
}

Source facts (verified): Account.Positions is map[contract.InstrumentID]*Position; Position{Available, Reserved int64, OpenLots []Lot}; Lot{EntryPrice, EntryAt, RemainingQty}; realize.ComputeWeightedAvgEntry(lots) (avg int64, hasOpen bool); realize.ComputeUnrealizedMark(lots, ltp, incurredEntryCosts) (gross, net int64) with net = gross - incurredEntryCosts, so passing 0 yields net==gross and we read gross (needs only lots + ltp → fully correct).

3.6 HTTP handler (reads/positions.go)

Same skeleton as A8; additionally reads optional instrument query param and passes it:

go
const PathPositions = "/v1/account/positions"
// ... aid required → 400; instrument := r.URL.Query().Get("instrument") // "" ok
snap, err := h.broker.ReadPositions(ctx, funnel.AccountID(aid), funnel.InstrumentID(instrument))
// 504 / 500 / 409 as A8; success:
writeJSON(w, 200, map[string]any{"positions": shapePositions(snap.Positions)})

shapePositions maps each PositionView → {instrument, qty, reserved_qty, weighted_avg_entry, mark_available, ltp, gross_unrealized} (no unit).

3.7 Status map

400 · 504 · 500 · 409 !Present · 200 {positions:[…]} (empty slice when flat; a specified instrument the account doesn't hold → {positions:[]}, not an error).

3.8 Cross-layer / gaps
X4 MarketPriceCache (writer deferred to Phase 7): the mark reads `ownerShard.mktCache.LastPrice(sess.binding.ExchangeID, instr)`. Until Phase 7 wires a real cache, bringup passes nil → newShard substitutes nopMarketCache{} → every LastPrice misses → MarkAvailable:false, position + avg still returned. A9 is not blocked. Phase 7 drops in a real cache (one ingestor goroutine per exchange, stamping its own ExchangeID) and marks light up with zero handler changes.
Net-unrealized gap (unfilled): ComputeUnrealizedMark needs incurredEntryCosts; nothing tracks per-position entry costs (Position/Lot carry no cost field — verified). Serving net would require a new L5 write-path change — a Position.IncurredEntryCosts accumulator wired into reserve.Convert + the snapshot. Out of scope for a read; deferred. (Resurfaces at A14.) A9 serves gross only.
3.9 Files touched

funnel/commands.go (+Instrument), funnel/broker.go (facade +param), funnel/results.go (PositionsSnapshot, PositionView), funnel/shard_handlers.go, funnel/reads.go (readPositions), funnel/mkt_cache.go + funnel/shard.go + funnel/broker.go + funnel/place.go (X4; mkt_cache.go extended in place, place.go call site updated to pass sess.binding.ExchangeID), reads/positions.go (new), reads/ports.go.

4. Point 3 — A11 Open/Working Orders

Endpoint: GET /v1/account/orders?account_id={id} Built already: nothing (fully net-new command/facade/dispatch/handler/snapshot). New: everything below + the L5 reserve edits (X1/X2/X3) — the one real gap in the tier.

4.1 Persistence question

None (live read).

4.2 Scope
Is: list the account's working orders with id, instrument, side, is-market, limit price, orig/remaining qty, reserved amounts, status, cancel-in-flight.
Isn't: order history (A10); placement time (dropped — see §4.7); mutation.
4.3 CROSS-LAYER: the L5 economy/reserve change (X1/X2/X3)

OrderTrack carries only exchange-seam facts (OrderID, AccountID, Instrument, Status, CancelInFlight — verified track.go). Money/qty/side truth lives in L5's ReservationRecord, which is read-only-inaccessible today (ReservationService exports only the 4 write methods + SetLogger). A11 needs a read accessor, and limit price must be retained.

X1 — economy/reserve/book.go: add to ReservationRecord (it already has OrderID, AccountID, SessionID, InstrumentID, Side, IsMarket, ReservedCash, ReservedUnits, RemainingQty, OrigQty, + cost flags):

go
    LimitPrice int64 // the user's set price (ticks); 0 when IsMarket. Read-only display fact.

Why store it: the Order struct carries LimitPrice, but it's transient (an arg to ReserveIfAvailable) and only the derived ReservedCash = LimitPrice*Qty is kept. Back-calculating LimitPrice = ReservedCash/RemainingQty is unreliable — partial fills decrement ReservedCash at the execution price (convert.go), sells reserve units not cash (ReservedCash==0), market buys fold in a slippage buffer. So it must be stored. It's deterministic (no clock/IO) → does not break L5's pure/replayable rule.

X2 — economy/reserve/reserve.go: set it in both record builds:

go
// reserveBuy's service.book.put(&ReservationRecord{ ... }) :
    LimitPrice: order.LimitPrice,
// reserveSell's service.book.put(&ReservationRecord{ ... }) :
    LimitPrice: order.LimitPrice,

X3 — economy/reserve/service.go: add the exported read accessor (the struct field is book *ReservationBook; book.get(orderID) *ReservationRecord exists):

go
// View returns a by-value copy of the reservation record for orderID, or (zero,false).
// Copy-out — never leaks the *record pointer (single-writer / no-escape). Also serves
// Phase-5's completion read (supersedes the P5-D22 RemainingQty/IsComplete assumption,
// which referenced methods that do not exist).
func (service *ReservationService) View(orderID OrderID) (ReservationRecord, bool) {
    rec := service.book.get(orderID)
    if rec == nil { return ReservationRecord{}, false }
    return *rec, true
}
4.4 Broker types (commands.go + results.go)
go
type ReadOpenOrders struct {
    AccountID AccountID
    ReplyTo   chan OpenOrdersSnapshot // buffered(1)
}
func (cmd ReadOpenOrders) Account() AccountID { return cmd.AccountID }

type OpenOrdersSnapshot struct {
    Present bool
    Orders  []OpenOrderView
}
type OpenOrderView struct {
    OrderID        OrderID
    Instrument     InstrumentID
    Side           contract.Side
    IsMarket       bool
    LimitPrice     int64 // 0 for market orders
    OrigQty        int64
    RemainingQty   int64
    ReservedCash   int64
    ReservedUnits  int64
    Status         OrderStatus
    CancelInFlight bool
}
4.5 Shard handler + facade + dispatch

Handler = thin wrapper over the readOpenOrders helper (§1.4):

go
func (ownerShard *shard) handleReadOpenOrders(cmd ReadOpenOrders) OpenOrdersSnapshot {
    if _, live := ownerShard.sessions[cmd.AccountID]; !live {
        return OpenOrdersSnapshot{Present: false} // → 409
    }
    return OpenOrdersSnapshot{Present: true, Orders: readOpenOrders(ownerShard, cmd.AccountID)}
}

Dispatch (shard_handlers.go): case ReadOpenOrders: typedCmd.ReplyTo <- ownerShard.handleReadOpenOrders(typedCmd). Facade (broker.go): ReadOpenOrders(ctx, aid) (OpenOrdersSnapshot, error) — identical shape to the other read facades.

tracks is the index (status + cancel-in-flight live only there, and terminal orders are already removed, so it is the open set); View supplies the money/qty/side/price truth.

4.6 HTTP handler (reads/orders.go)
go
const PathOrders = "/v1/account/orders"
// aid required → 400; ReadOpenOrders facade; 504/500/409 as A8; success:
writeJSON(w, 200, map[string]any{"orders": shapeOrders(snap.Orders)})

shapeOrders → {order_id, instrument, side, is_market, limit_price, orig_qty, remaining_qty, reserved_cash, reserved_units, status, cancel_in_flight} (no unit).

4.7 Decisions / notes
Placement time dropped (would need an OrderTrack.PlacedAt + a broker punch-time stamp; not worth it for the demo).
Dependency (X8): tracks + reservation records are populated by Tier-1 order intake (design-complete, code-stubbed — handlePlaceOrder returns PlaceResult{} today). A11 reads structures that light up when Tier-1 lands; the design is not blocked by the stub.
Minor: per-account filter is O(shard's open orders) — fine for the demo; a per-account track index is a later optimization.
4.8 Files touched

L5: economy/reserve/book.go, economy/reserve/reserve.go, economy/reserve/service.go. Broker: funnel/commands.go, funnel/results.go, funnel/shard_handlers.go, funnel/broker.go, funnel/reads.go (readOpenOrders), reads/orders.go (new), reads/ports.go.

5. Point 4 — A14 Agent Observation

Endpoint: GET /v1/account/observation?account_id={id} What it is: an agent's one-shot self-snapshot — balance + positions + open orders (live, one atomic shard visit) plus the account's own recent closed trades (durable), fanned out concurrently and merged. No market-wide / OHLCV data — those are a separate Phase-7 surface. (Store.RecentTrades is account-keyed: WHERE account_id=$1 — the agent's own closed trades, not market prints.) Built already: Store.RecentTrades. New: the composite command/facade/handler + the fan-out HTTP handler. Cross-layer: reuses A9's MarketPriceCache degrade; reuses A11's View.

5.1 Persistence question

None written (one durable read).

5.2 Scope
Is: atomic live trio (balance+positions+orders in one owner visit) + Store.RecentTrades(…,20), concurrent, merged, degrade-don't-fail.
Isn't: any market-wide/OHLCV/quote/depth data (Phase 7); P&L summary (A10); writes.
5.3 Why one atomic command (not three facade calls)

Three separate ReadBalance/ReadPositions/ReadOpenOrders round-trips could interleave with a fill, giving an inconsistent snapshot. A14 uses one command visiting the owner once, assembling all three via the shared helpers.

5.4 Types (commands.go + results.go)
go
type ReadObservation struct {
    AccountID AccountID
    ReplyTo   chan ObservationLive // buffered(1)
}
func (cmd ReadObservation) Account() AccountID { return cmd.AccountID }

// ObservationLive = the atomic LIVE half only. The durable recent-trades half is merged
// in by the HTTP handler (different source, concurrent).
type ObservationLive struct {
    Present    bool
    Balance    BalanceSnapshot
    Positions  []PositionView
    OpenOrders []OpenOrderView
}
5.5 Shard handler + facade + dispatch
go
func (ownerShard *shard) handleReadObservation(cmd ReadObservation) ObservationLive {
    sess, live := ownerShard.sessions[cmd.AccountID]
    if !live { return ObservationLive{Present: false} } // → 409
    return ObservationLive{
        Present:    true,
        Balance:    readBalance(sess),
        Positions:  readPositions(ownerShard, sess, ""), // all
        OpenOrders: readOpenOrders(ownerShard, cmd.AccountID),
    }
}

Dispatch: case ReadObservation: typedCmd.ReplyTo <- ownerShard.handleReadObservation(typedCmd). Facade (broker.go): ReadObservation(ctx, aid) (ObservationLive, error) — standard shape.

5.6 HTTP handler with fan-out + degrade (reads/observation.go)

Needs both ports. Fire live + durable concurrently on one ctx:

go
const PathObservation = "/v1/account/observation"

ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ReadTimeout); defer cancel()
var live funnel.ObservationLive; var liveErr error
var trades []store.TradeRow;     var tradesErr error
var wg sync.WaitGroup; wg.Add(2)
go func(){ defer wg.Done(); live, liveErr = h.broker.ReadObservation(ctx, funnel.AccountID(aid)) }()
go func(){ defer wg.Done(); trades, tradesErr = h.store.RecentTrades(ctx, store.AccountID(aid), 20) }()
wg.Wait()

switch {
case errors.Is(liveErr, funnel.ErrBrokerTimeout): writeError(w, 504, "observation read timed out"); return
case liveErr != nil:                              writeError(w, 500, "internal error"); return
case !live.Present:                               writeError(w, 409, "no live session for account"); return
}
// live ok — durable half degrades, never fails the response:
resp := map[string]any{
    "balance":    shapeBalance(live.Balance),
    "positions":  shapePositions(live.Positions),
    "orders":     shapeOrders(live.OpenOrders),
}
if tradesErr != nil { resp["recent_trades"] = []any{}; resp["trades_degraded"] = true } // + log
else                { resp["recent_trades"] = shapeTrades(trades); resp["trades_degraded"] = false }
writeJSON(w, 200, resp)
5.7 Decisions / notes
Only the live half drives 504/409/500; a durable-read failure → trades_degraded:true, recent_trades:[], still 200.
RecentTrades clamped at 20 (the store/types.go comment notes A14 clamps itself at 20).
Lag accepted + documented: durable trades (async-drained to Postgres) can trail the live half; a just-closed trade may not appear yet.
Positions in the observation inherit A9's mark degrade (Phase-7 cache).
5.8 Files touched

funnel/commands.go, funnel/results.go, funnel/shard_handlers.go, funnel/broker.go, reads/observation.go (new), reads/ports.go.

6. Point 5 — A10 Trade History + P&L Summary (durable)

Endpoints: GET /v1/account/trades?account_id={id}&limit={n}&before={rfc3339?} and GET /v1/account/pnl?account_id={id} Built already: Store.TradePage, Store.PnLSummary, TradeRow, PnLSummary — all of it. A10 = HTTP + shaping only. No funnel, no shard, no L5.

6.1 Persistence question

None written (two durable reads).

6.2 Why pure-durable (no 409/404)

Closed trades live in Postgres, readable regardless of live state. The handler holds only a storePort, calls store.TradePage/store.PnLSummary directly. A new/unknown account → zero rows (TradePage → []) / zero-filled summary (count(*) always yields a row) → 200, never 404/409.

6.3 Trades endpoint — keyset pagination (reads/trades.go)
go
const PathTrades = "/v1/account/trades"
// aid required → 400; limit := parseLimit(r, 50); before := parseBefore(r) (nil ok; malformed → 400)
rows, err := h.store.TradePage(ctx, store.AccountID(aid), limit, before)
switch {
case errors.Is(err, store.ErrLoadFailed): writeError(w, 503, "store unavailable")
case err != nil:                           writeError(w, 500, "internal error")
default:
    var nextBefore *time.Time
    if len(rows) == limit && len(rows) > 0 { t := rows[len(rows)-1].ExitAt; nextBefore = &t } // full page → more may exist
    writeJSON(w, 200, map[string]any{"trades": shapeTrades(rows), "next_before": nextBefore})  // short/empty page → next_before null = last page
}

shapeTrades maps each TradeRow → {trade_id, instrument, direction, quantity, entry_price, exit_price, entry_at, exit_at, gross_pnl, total_costs, net_pnl, pnl_pct, exit_reason} (no unit). Client walks pages by feeding next_before back as before.

6.4 P&L endpoint (reads/trades.go)
go
const PathPnL = "/v1/account/pnl"
s, err := h.store.PnLSummary(ctx, store.AccountID(aid))
// 503 ErrLoadFailed / 500 / 200:
writeJSON(w, 200, map[string]any{
    "trade_count": s.TradeCount, "gross_pnl": s.GrossPnl, "total_costs": s.TotalCosts,
    "net_pnl": s.NetPnl, "win_count": s.WinCount, "loss_count": s.LossCount})

(Breakevens, net_pnl==0, counted in neither win nor loss — matches the store.) Kept a separate endpoint so the account-wide aggregate isn't recomputed per trade page.

6.5 Status map

400 bad account_id/limit/before · 503 ErrLoadFailed (incl. ctx timeout — matches the transfer durable branch; no 504 on the pure-durable path) · 200 (incl. empty).

6.6 Files touched

reads/trades.go (new — both handlers), reads/ports.go (TradePage, PnLSummary). No funnel/L5.

7. Point 6 — A16 Cost/Tax View (country-parametric, durable)

Endpoints: GET /v1/account/costs?account_id={id}&country={c}&limit={n}&before={rfc3339?} and GET /v1/account/costs/summary?account_id={id}&country={c} Built already: the generic shapes (Line, TradeCostBreakdown, CostAggregate), the India reads (CostsPerTradeIN, CostsAggregateIN), and the column-knowledge quarantine (mapIndiaCostsToLines + the country/india package). A16 = the read-side country dispatch + endpoints + shaping. No store/L5/funnel change.

7.1 Persistence question

None written (two durable reads).

7.2 The read-side country dispatch (A16's core)

country is a required param. Mirror buildCountryStack's allowlist, but map to the Store read methods. The handler sees only generic []Line/Total:

go
// per-trade dispatch
switch country {
case "IN": breakdowns, err = h.store.CostsPerTradeIN(ctx, store.AccountID(aid), limit, before) // built
case "US": writeError(w, 501, "country not available"); return   // slot defined, not built (honest-501)
default:   writeError(w, 400, "unknown country"); return          // not in allowlist
}

Adding a country later = a case here + its CostsPerTrade<XX>/CostsAggregate<XX> store methods; the handler's shape never changes.

7.3 Endpoints (reads/costs.go)
go
const PathCosts        = "/v1/account/costs"
const PathCostsSummary = "/v1/account/costs/summary"

Per-trade ([]TradeCostBreakdown) → {trades:[{trade_id, lines:[{name,amount}], total}], next_before} — keyset cursor identical to A10 (paginates on pt.exit_at via the mandatory participant_trades join). Only non-zero lines appear (store drops zeros). Summary (CostAggregate) → {trade_count, lines:[{name,amount}], total}. (amount/total are plain integer units — no currency, per §0.6.)

7.4 Status map

400 missing/bad account_id, missing/unknown country, bad limit/before · 501 country=="US" · 503 ErrLoadFailed · 200 (incl. empty, zero-filled).

7.5 Notes
Handler is country-agnostic: never learns column/tax names (quarantined in the store + country/india).
Total is the stored trade_costs.total column, never recomputed.
US = honest 501, consistent with buildCountryStack.
7.6 Files touched

reads/costs.go (new — both handlers), reads/ports.go (CostsPerTradeIN, CostsAggregateIN). No store/L5/funnel change.

8. Decision log (Phase 6)
ID	Decision	Status
P6-D1	BalanceSnapshot = four raw signed buckets, no derived views	LOCKED
P6-D2	Direct Cash-field copy on the owner (ReadAvailableCash returns only Available)	LOCKED
P6-D3	Live reads: !Present→409, handler-owned ctx timeout→504 (via ErrBrokerTimeout). Cross-doc: supersedes the Tier-0 doc's generic 503 framing for reads	LOCKED
P6-D4	Shared GET-read skeleton established at A8, reused by A9/A11/A14	LOCKED
P6-D5	Reads persist nothing	LOCKED
P6-D6	money unit field — RETIRED (see P6-D33: no currency/unit field)	RETIRED
P6-D7	A9 compute-on-shard (pure realize funcs on the owner; lots don't escape)	LOCKED
P6-D8	Gross-only mark via ComputeUnrealizedMark(…,0); NetUnrealized field absent	LOCKED
P6-D9	Mark degrades, never fails: cold mktCache → MarkAvailable:false	LOCKED
P6-D10	ReadPositions extended with an optional Instrument filter, applied shard-side (Option A)	LOCKED
P6-D11	Flat ≠ no-session: empty positions → 200 {positions:[]}; only no-session → 409	LOCKED
P6-D12	Available vs Reserved surfaced separately	LOCKED
P6-D13	New L5 ReservationService.View(orderID) (ReservationRecord, bool) (copy-out); also serves Phase-5 completion	LOCKED
P6-D14	A11 indexes by tracks, joins reservation via View	LOCKED
P6-D15	A11 live-only; 409 on no session (history = A10)	LOCKED
P6-D16	A11 surfaces reserved cash/units	LOCKED
P6-D17	ReservationRecord.LimitPrice added (set in reserveBuy/sell); IsMarket reused; market rows carry LimitPrice:0+IsMarket:true	LOCKED
P6-D18	A11 placement time dropped	LOCKED
P6-D19	A14 live trio is ONE atomic shard command, reusing shared read* helpers — not three round-trips	LOCKED
P6-D20	A14 fan-out concurrent (live ∥ durable), merged in the HTTP handler on one ctx	LOCKED
P6-D21	A14 degrade-don't-fail: only the live half drives 504/409/500; durable failure → trades_degraded, still 200	LOCKED
P6-D22	A14 recent trades = the account's own closed trades, clamped at 20 (NOT market-wide)	LOCKED
P6-D23	A14 lag accepted + documented	LOCKED
P6-D24	A10 pure-durable, off-shard (storePort only); no 409/404; empty = 200	LOCKED
P6-D25	A10 two endpoints (/trades paginated, /pnl summary); summary not recomputed per page	LOCKED
P6-D26	A10 keyset pagination on exit_at, cursor next_before; short page = last page	LOCKED
P6-D27	Durable reads: ErrLoadFailed → 503 across the board (incl. ctx timeout); no 504	LOCKED
P6-D28	A16 country required; read-side allowlist dispatch (IN→India, US→501, else→400)	LOCKED
P6-D29	A16 handler country-agnostic — sees only []Line/Total; column knowledge quarantined	LOCKED
P6-D30	A16 two endpoints (per-trade paginated + aggregate), same keyset cursor as A10	LOCKED
P6-D31	A16 Total is the stored column, never recomputed; only non-zero lines surfaced	LOCKED
P6-D32	A16 US = honest 501, consistent with buildCountryStack	LOCKED
P6-D33	Money = bare integer units, no currency/unit field anywhere. Carried issue: no FX, 1 unit ≡ 1 INR ≡ 1 USD (see §0.6 / §10)	LOCKED
P6-D34	Read timeout = configurable reads.Config.ReadTimeout (bringup-supplied, Validate >0), mirroring transfer.Config.TransferTimeout	LOCKED
P6-D35	MarketPriceCache (built in Phase 5 with BestAsk only) extended in place with LastPrice; BOTH methods keyed by (ExchangeID, InstrumentID). ExchangeID is a broker-side identifier (SessionBinding carries it; the exchange wire does NOT); the Phase-7 ingestor stamps it on receipt — one ingestor per exchange, same pattern as the settlement consumer — so the cache is implicitly map[ExchangeID]map[InstrumentID]… with single-writer-per-(ExchangeID,InstrumentID) by construction. Readers pass sess.binding.ExchangeID. shard.mktCache is always non-nil post-construction (newShard substitutes nopMarketCache{}); mark branches guard on method ok=false, not nil-check. Real writer = Phase 7.	LOCKED
P6-D36	broker/reads is one package for all eight endpoints (vs transfer's one-per-concern), since they share the GET skeleton	LOCKED
9. Cross-doc corrections (to the authority docs)
BROKER_READ_SURFACES_tier4.md §0.3 ("unit":"paise" provisional) — superseded by P6-D33: no unit field, bare units. Every unit:"paise" in that doc's examples is dropped.
BROKER_READ_SURFACES_tier4.md §3.5 (A9 filter said shard-side) — upheld (P6-D10). (An earlier HTTP-side variant was considered and dropped.)
BROKER_PHASE5_IMPLEMENTATION_DESIGN.md P5-D22 (completion read via ReservationService.RemainingQty/IsComplete) — those methods do not exist in source; the new View accessor (P6-D13/X3) is the single read path for both A11 and Phase-5 completion. Phase 5 should read completion via View(orderID).RemainingQty.
A14 "recent trades" — the Tier-4 design's wording that could read as market-wide prints is clarified (P6-D22): it's the account's own closed trades (RecentTrades is WHERE account_id=$1). No market-data coupling.
10. Carried / deferred items
No currency/FX model (P6-D33) — KNOWN DELIBERATE ISSUE. Money is dimensionless units; 1 unit ≡ 1 INR ≡ 1 USD, no conversion. Multi-currency-within-an-account and FX-on-bank-transfers are out of scope. Revisit when currencies matter.
Net-unrealized P&L (A9/A14). Gross only. Net needs an L5 write-path change (Position.IncurredEntryCosts accumulator wired into reserve.Convert + the snapshot). Deferred.
MarketPriceCache real impl + wiring (X7, Phase 7). A9/A14 marks degrade (MarkAvailable:false) until the ingestor-backed cache lands and is passed to NewBroker.
A11 placement time. Dropped; would need OrderTrack.PlacedAt + a punch-time stamp.
A13 (positions optimization) — out of Phase 6 (deferred earlier).
Per-account track index — A11/A14 filter tracks O(shard's open orders); fine for the demo, optimize later.
Dependency, not deferred: Tier-1 order intake (X8) populates tracks + reservation records; A11/A14 show orders once Phase-5's handlePlaceOrder body lands.
11. Complete file touch-list

L5 — economy/reserve/ (cross-layer X1/X2/X3):

book.go — + LimitPrice int64 on ReservationRecord.
reserve.go — + LimitPrice: order.LimitPrice in reserveBuy and reserveSell record builds.
service.go — + View(orderID) (ReservationRecord, bool).

Broker — funnel/:

mkt_cache.go (extended in place — built in Phase 5 with BestAsk only) — add LastPrice + ExchangeID keying on both methods; extend nopMarketCache. Do NOT create funnel/marketdata.go.
shard.go — + mktCache MarketPriceCache field (post-construction always non-nil); newShard gains mktCache param, substitutes nopMarketCache{} on nil.
place.go — call site updated: `mktCache.BestAsk(order.Instrument)` → `mktCache.BestAsk(liveSession.binding.ExchangeID, order.Instrument)` (Phase-5 code edited in place; one line).
broker.go — NewBroker + mktCache param; facade ReadPositions + instrument param; new facades ReadOpenOrders, ReadObservation.
commands.go — ReadPositions + Instrument; new commands ReadOpenOrders, ReadObservation (+ Account()).
results.go — fill BalanceSnapshot, PositionsSnapshot; add PositionView, OpenOrdersSnapshot, OpenOrderView, ObservationLive.
shard_handlers.go — fill handleReadBalance, handleReadPositions; add handleReadOpenOrders, handleReadObservation; add dispatch cases for the two new commands.
reads.go (new) — shared helpers readBalance, readPositions, readOpenOrders.

Broker — reads/ (new package):

config.go, ports.go, common.go, balance.go, positions.go, orders.go, observation.go, trades.go, costs.go.

Broker — bringup/ (X6):

supply reads.Config{ReadTimeout}; register the eight routes on the HTTP mux (each handler's const Path); pass nil MarketPriceCache into NewBroker.

Unchanged (reused as-is): store/trades.go, store/costs_in.go, store/types.go, economy/account/*, economy/realize/compute.go, transfer/*.

12. Suggested implementation order
X1–X3 (L5 reserve): LimitPrice field + sets + View. Unblocks A11/A14 and Phase-5 completion. Small, isolated, testable.
X4 (MarketPriceCache + shard field + NewBroker param): interface + nil wiring. Unblocks A9/A14 marks (degrading).
Shared helpers + snapshot structs (funnel/reads.go, results.go).
A8 (fill handleReadBalance) → A9 (command/facade/handler) → A11 (new command/facade/handler) → A14 (new command/facade/handler). Live family, in dependency order.
reads package skeleton (config.go, ports.go, common.go) → then the live handlers (balance/positions/orders/observation).
A10 (trades.go) and A16 (costs.go) — pure-durable, independent of the funnel work; can be done in parallel.
bringup wiring (X6): Config, routes, nil cache.
Phase 7 (later): real MarketPriceCache; marks light up with no handler change.