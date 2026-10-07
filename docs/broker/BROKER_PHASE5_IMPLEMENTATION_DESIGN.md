Broker — Phase 5 Implementation Design (Order Core & Order Endpoints — Tier 1)

Scope: the order core — the first phase with a real external order surface. Four points: place (handlePlaceOrder), cancel (handleCancel), settle (handleApplyResult, the six-outcome adapter), and the HTTP endpoints (place / cancel / modify). Keyed by AccountID. It fills in the handler bodies at both ends of the Phase-4 exchange loop — the side that builds+punches orders, and the side that applies what comes back.

Status: Points 1–4 LOCKED (design + source-verified in this session). A handful of items are carried (§8): slippageBuffer sizing, the knownInstrument instrument-set injection, OrderTrack durability (B9), and exitAt session-time alignment.

Where it sits: after Phase 0 / Tier 0 (funnel — BROKER_TIER0_IMPLEMENTATION_DESIGN.md), Phase 1 (wiring + durable store — BROKER_PHASE1_IMPLEMENTATION_DESIGN.md), Phase 2 (session & account lifecycle — BROKER_PHASE2_IMPLEMENTATION_DESIGN.md), Phase 3 (bank transfer — BROKER_PHASE3_IMPLEMENTATION_DESIGN.md), and Phase 4 (exchange wire seam — BROKER_PHASE4_IMPLEMENTATION_DESIGN.md). Phase 5 is the first consumer of Phase 4's punch (Point 1) and settlement consumer (Point 2).

Authority:

BROKER_ORDER_LIFECYCLE_intake_punch_settle.md — the order core (intake §3, cancel §4, settle §6).
BROKER_READ_SURFACES_tier4.md — §0 (HTTP house style), §9 (the modify-order composer).
Exchange_Broker_Contract.docx (UTF-8 text) — the wire seam: §4.1 EngineResult, §4.2 MatchResult/Trade, §6–8 the three "doubts" (diverted, one-shot, maker fill).
Source of truth for types: the contract, economy/reserve, economy/realize, and exchange Go packages.

The four points:

handlePlaceOrder — validate → (market-buy: price) → mint OrderID → reserve (Gate 4) → create OrderTrack → punch.
handleCancel — async: validate → mark in-flight → punch; release happens later in settlement.
handleApplyResult — the six-outcome settlement adapter; sequences L5 Convert/Release/ReleaseExcess.
The HTTP endpoints — POST /v1/orders (place), /v1/orders/cancel, /v1/orders/modify (compose).

Read rule: every type / signature / field is grounded in real source and the locked docs; where the authority doc's pseudo-code diverges from source, the correction is logged (§6) and cross-noted (§9).

0. Orientation — the two-record discipline, and "accepted, not executed"

The division of labor that the whole phase rests on (BROKER_ORDER_LIFECYCLE_intake_punch_settle.md §1):

L5's ReservationRecord (in the ReservationBook, keyed by OrderID, per-shard) = the money truth: reserved cash/units, qty progress, brokerage state. The broker never duplicates it.
The broker's OrderTrack = thin exchange-seam state only: OrderID, AccountID, Instrument, Status, CancelInFlight, IsMarketBuy. No amounts, no qty progress — "how much is left / is it complete" is read from L5 (ReservationService.ViewReservation(oid).RemainingQty), never mirrored.

The unifying HTTP semantic (BROKER_READ_SURFACES_tier4.md §9.2): every order action is accepted, not executed. A punch is fire-and-forget; the real fill/cancel outcome returns asynchronously via settlement (Point 3) and surfaces in later reads. No endpoint ever claims "done."

The order loop across Phase 4: place/cancel build an InboundMessage and call s.punch (Phase 4 Point 1); the exchange's answer returns on Private, is carried by the settlement consumer (Phase 4 Point 2) as an ApplyResult, and lands in handleApplyResult (Point 3). Phase 4 is outcome-blind transport; Phase 5 is where money moves.

1. Point 1 — handlePlaceOrder (the place path)
1.1 Persistence question

Persists via L5 only. Gate 4 (ReserveIfAvailable) creates + marks the ReservationRecord dirty inside L5 — the broker persists no money state. The broker's OrderTrack lives in the in-memory s.tracks map; its snapshotting is tied to crash-recovery (B9, deferred, §8). Net: a place causes an L5 persist, adds no broker-side durable write yet.

1.2 Scope (decision B, from Phase 4)

Is: validate → price (market-buy only) → mint OrderID → reserve → create track → punch → reply. Isn't: the reservation math (L5's), and the punch mechanism (Phase 4's s.punch).

1.3 The handler
go
func (s *shard) handlePlaceOrder(c PlaceOrder) PlaceResult {
    sess, ok := s.sessions[c.AccountID]
    if !ok { return PlaceResult{Status: Rejected, Reason: ReasonNoSession} }
    o := c.Order

    if reason := s.validateSpec(sess, o); reason != ReasonOK {   // STEP 1
        return PlaceResult{Status: Rejected, Reason: reason}
    }

    var bestAsk, buffer int64                                     // STEP 2 — market-buy only
    if o.Side == Buy && o.Type == Market {
        ba, ok := s.mktCache.BestAsk(o.Instrument)               // Point 5 (deferred runtime); interface now
        if !ok { return PlaceResult{Status: Rejected, Reason: ReasonNoMarketPrice} }
        bestAsk, buffer = ba, slippageBuffer(ba, o.Qty)
    }

    oid := s.nextOrderID(c.AccountID)                             // STEP 3 — "<AccountID>:<seq>"

    if err := s.resv.ReserveIfAvailable(sess.participant,        // STEP 4 — Gate 4, error-only
                 buildReserveOrder(o, oid, c, bestAsk, buffer)); err != nil {
        return PlaceResult{Status: Rejected, Reason: reserveReason(err)}   // errors.Is on L5 sentinels
    }

    s.tracks[oid] = &OrderTrack{                                  // STEP 5 — thin track
        OrderID: oid, AccountID: c.AccountID, Instrument: o.Instrument,
        Status: Working, IsMarketBuy: o.Side == Buy && o.Type == Market,
    }

    s.punch(sess, contract.InboundMessage{                       // STEP 6 — Phase 4 Point 1
        Kind: contract.SubmitOrder, Instrument: o.Instrument,    // BrokerID stamped inside s.punch
        Submit: &contract.FundedOrder{
            OrderID: oid, Participant: sess.participant, Instrument: o.Instrument,
            Side: o.Side, Type: o.Type, TIF: o.TIF, Qty: o.Qty, LimitPrice: o.LimitPrice,
            MaxSpend: maxSpend(bestAsk, o.Qty, buffer), STP: CancelIncoming,
        },
    })
    return PlaceResult{Status: Accepted, OrderID: oid}
}
1.4 The two helpers (designed, not hand-waved)
go
// nextOrderID — shard-local monotonic counter (shard adds `seq uint64`). On the owner goroutine, so a
// plain increment: no atomic, no lock. "<AccountID>:<seq>" so the settlement consumer parses account back.
func (s *shard) nextOrderID(accountID AccountID) OrderID {
    s.seq++
    return accountID + ":" + strconv.FormatUint(s.seq, 10)      // OrderID = string
}

// validateSpec — structural, pre-pricing, BEFORE reserve (a bad spec never touches L5).
func (s *shard) validateSpec(sess *ownedSession, o OrderSpec) RejectReason {
    switch {
    case !sess.knownInstrument(o.Instrument):   return ReasonUnknownInstrument  // instrument-set: see P5-D9 / §8
    case o.Qty <= 0:                            return ReasonBadQty
    case o.Side != Buy && o.Side != Sell:       return ReasonBadSide
    case o.Type == Limit && o.LimitPrice <= 0:  return ReasonBadPrice
    case o.Type != Limit && o.Type != Market:   return ReasonBadType
    }
    return ReasonOK
}

nextOrderID uniqueness holds because hash(AccountID) % N (fixed N) keeps an account on one shard, so the account prefix disambiguates across shards. Resharding would need counter care (out of scope); crash-resume of the counter is B9 (§8).

1.5 Market-buy — the one cache-dependent branch (P5-D3)

Only market-buy needs a price (a market order carries none, and a buy must pre-hold cash before the fill price is known). The broker reads mktCache.BestAsk and sizes SlippageBuffer, and passes both into the reserve.Order — L5 computes requiredCash = BestAsk*Qty + SlippageBuffer internally (economy/reserve/types.go). The broker separately computes the equal maxSpend for FundedOrder.MaxSpend (the exchange's hard cap); reserved == MaxSpend by construction, so exchange staleness is harmless and ReleaseExcess refunds the unspent remainder at settlement. buildReserveOrder maps o.Type==Market → IsMarket; for limits/sells BestAsk/SlippageBuffer are 0 and L5 ignores them (reserves off LimitPrice×Qty or units). The helper is named by role (builds a reserve.Order), not by its downstream layer, per the "no layer or flag IDs in symbol names" rule. Market-buy's runtime is blocked until Point 5 (MarketDataCache) lands; the branch is designed against the BestAsk interface now (option a).

1.6 Ingress / egress

Ingress: PlaceOrder{AccountID, Order, ReplyTo} via route → shard inbox. Egress: the punch (FundedOrder → exchange Inbound) and a PlaceResult reply. Touch: L5 ReservationService, mktCache (market-buy only), the exchange seam, the tracks map. Runs on: owner goroutine (no locks).

2. Point 2 — handleCancel (the cancel path)
2.1 Persistence question

One durable touch: setting CancelInFlight calls s.bundle.Persist.MarkSnapshotDirty(sess.binding.SessionID, c.AccountID) so the flag survives a crash → no re-punch on recovery. OrderTrack-in-snapshot durability is the B9 cross-point (§8). No money moves here → no L5 persist. (Signature carries (sessionID, accountID), not aid-only — P5-D30; the sessionID comes from the live session resolved earlier in the handler via sess, _ := s.sessions[c.AccountID].)

2.2 Scope

Is: handleCancel only — validate, mark in-flight, punch. Isn't: any release (deferred to settlement), and any handleModify primitive (there is none — P5-D14).

2.3 The handler
go
func (s *shard) handleCancel(c CancelOrder) CancelResult {   // CancelOrder{AccountID, OrderID, ReplyTo} — Tier-0
    sess, ok := s.sessions[c.AccountID]
    if !ok { return CancelResult{Status: CancelFailed, Reason: ReasonNoSession} }
    tr, ok := s.tracks[c.OrderID]
    if !ok || tr.AccountID != c.AccountID {                  // ownership: no cross-account cancel
        return CancelResult{Status: CancelFailed, Reason: ReasonUnknownOrder}
    }
    if tr.Status != Working && tr.Status != PartiallyFilled {
        return CancelResult{Status: CancelFailed, Reason: ReasonNotCancellable}
    }
    if tr.CancelInFlight {
        return CancelResult{Status: CancelPending, OrderID: c.OrderID}   // idempotent — no re-punch
    }
    tr.CancelInFlight = true
    s.bundle.Persist.MarkSnapshotDirty(sess.binding.SessionID, c.AccountID)  // (sessionID, aid) — P5-D30
    s.punch(sess, contract.InboundMessage{
        Kind: contract.CancelOrder, Instrument: tr.Instrument,           // qualified: contract.CancelOrder
        Cancel: &contract.CancelRequest{OrderID: c.OrderID},             // whole-order only
    })
    return CancelResult{Status: CancelPending, OrderID: c.OrderID}
}
2.4 The async discipline (the heart of it)

No reservation is released here — by design. The broker's Status check is a best-effort pre-filter; the exchange is authoritative. Our "Working" may already be filled there (the race), so releasing now could double-spend. Release waits for the echoed ack: settlement sees OutcomeCancelled with the exchange's CancelledQty and releases exactly that (Point 3). Cancel is punch-now, settle-money-on-echo. CancelPending means "requested," never "cancelled."

2.5 Naming note (collision)

The broker command CancelOrder and the contract.InboundKind constant CancelOrder (contract/inbound.go: SubmitOrder/CancelOrder) share a name. Different packages — compiles as long as the Kind is always qualified (contract.SubmitOrder/contract.CancelOrder) and the bare name is the command type. Both names are locked upstream; always qualify the Kind.

3. Point 3 — handleApplyResult (the six-outcome settlement adapter)
3.1 Persistence question

L5 persists all money; the broker only marks the account dirty. Every Convert/Release/ ReleaseExcess manages its own reservation record inside L5 (BROKER_ORDER_LIFECYCLE_intake_punch_settle.md §9). The broker's only durable act is finalizeTrack → MarkSnapshotDirty(aid); OrderTrack-in-snapshot is the B9 cross-point (§8).

3.2 Scope

Is: route each EngineResult to L5 op(s), update OrderTrack.Status, clean up terminal tracks. Isn't: the money logic (L5's — §8 of the authority doc) or the transport (Phase 4 Point 2's consumer already drained Private, resolved the account, and routed the ApplyResult).

3.3 Dispatch + shared fill helper
go
func (s *shard) handleApplyResult(c ApplyResult) {   // push command, no reply
    r := c.Result
    switch r.Outcome {
    case contract.OutcomeMatched:        s.settleMatch(c.AccountID, r)
    case contract.OutcomeMakerFill:      s.settleMakerFill(c.AccountID, r)
    case contract.OutcomeDiverted:       s.markDiverted(r)
    case contract.OutcomeRejected:       s.settleRejected(c.AccountID, r)
    case contract.OutcomeCancelled:      s.settleCancelled(c.AccountID, r)
    case contract.OutcomeCancelRejected: s.settleCancelRejected(r)
    default: panic(fmt.Sprintf("apply: unknown outcome %v for order %s", r.Outcome, r.OrderID))
    }
}

// Convert is 5-arg in source (economy/reserve/convert.go), not the doc's 3.
//   exitAt  = time.Now() — the exchange result carries NO timestamp (contract never imports "time").
//   tradeID = "<OrderID>:<Trade.Seq>" — from the exchange's Trade.Seq (per-matcher, so OrderID-qualified).
func (s *shard) convertTrades(r contract.EngineResult) {
    for _, t := range r.Match.Trades {
        _ = s.resv.Convert(r.OrderID, t.Qty, t.Price, time.Now(), tradeID(r.OrderID, t.Seq))
    }
}
func tradeID(oid OrderID, seq contract.Seq) realize.TradeID {
    return realize.TradeID(oid + ":" + strconv.FormatUint(uint64(seq), 10))
}

t.Price is always the maker's resting price (contract/match.go) — exactly L5's makerPrice.

3.4 The six settle helpers
go
// 1. Matched — taker fill, one-shot: all prints batched in Match.Trades.
//    FIVE branches (P5-D29): zero-fill-cancel is split off from fills+leftover
//    because the two have different terminal statuses — a zero-fill cancel
//    never settled anything (Status=Cancelled), while fills+leftover did
//    (Status=Filled).
func (s *shard) settleMatch(aid AccountID, r contract.EngineResult) {
    m := r.Match; tr := s.tracks[r.OrderID]
    s.convertTrades(r)
    switch {
    case len(m.Trades) == 0 && m.Rested != nil:                                // pure rest (P5-D17)
        tr.Status = Working
    case m.Rested != nil:                                                      // partial + remainder rests
        tr.Status = PartiallyFilled
    case len(m.Trades) == 0 && m.CancelledQty > 0:                             // zero-fill cancel (P5-D29)
        _ = s.resv.Release(r.OrderID, m.CancelledQty); tr.Status = Cancelled
    case m.CancelledQty > 0:                                                   // fills + non-resting remainder
        _ = s.resv.Release(r.OrderID, m.CancelledQty); tr.Status = Filled
    default:                                                                   // clean full fill
        tr.Status = Filled
    }
    if tr.IsMarketBuy && tr.Status == Filled { _ = s.resv.ReleaseExcess(r.OrderID) }  // buffer true-up (see §3.5)
    s.finalizeTrack(aid, r.OrderID)
}

// 2. MakerFill — a RESTING order of ours got hit; incremental, many over the order's life.
//    Completion is an L5 fact read through ViewReservation (not a direct accessor —
//    Convert's shared tail may REMOVE the record at RemainingQty==0, so (zero, false)
//    is a legitimate "no live reservation" answer, equivalent to RemainingQty==0.
func (s *shard) settleMakerFill(aid AccountID, r contract.EngineResult) {
    tr := s.tracks[r.OrderID]
    s.convertTrades(r)
    view, ok := s.resv.ViewReservation(r.OrderID)                             // completion = L5 fact
    if !ok || view.RemainingQty == 0 {
        tr.Status = Filled; s.finalizeTrack(aid, r.OrderID)
    } else { tr.Status = PartiallyFilled }
}

// 3. Diverted — auction ACK: hold reservation, no L5 call. Second result settles at clear (settleMatch).
func (s *shard) markDiverted(r contract.EngineResult) { s.tracks[r.OrderID].Status = Working }

// 4. Rejected — nothing entered the book: release everything.
func (s *shard) settleRejected(aid AccountID, r contract.EngineResult) {
    _ = s.resv.Release(r.OrderID, 0)                                         // 0 = release all
    s.tracks[r.OrderID].Status = Rejected; s.finalizeTrack(aid, r.OrderID)
}

// 5. Cancelled — our cancel won: release EXACTLY the echoed qty (race discipline).
func (s *shard) settleCancelled(aid AccountID, r contract.EngineResult) {
    _ = s.resv.Release(r.OrderID, r.CancelledQty)                           // never the assumed remainder
    tr := s.tracks[r.OrderID]; tr.CancelInFlight = false; tr.Status = Cancelled
    s.finalizeTrack(aid, r.OrderID)
}

// 6. CancelRejected — order already gone: nothing to release, just clear the flag.
func (s *shard) settleCancelRejected(r contract.EngineResult) {
    if tr, ok := s.tracks[r.OrderID]; ok { tr.CancelInFlight = false }      // usually a miss (channel order)
}

// cleanup — delete terminal track, mark the RESOLVED account dirty (threads c.AccountID, P5-D20).
// MarkSnapshotDirty's real signature is (sessionID, accountID), not aid-only (P5-D30); resolve
// the live session via s.sessions[aid]. A !live account at this point means the session was
// torn down between place-time and this settlement (user disconnect, lifecycle teardown); the
// money-side mark-dirty already fired inside Convert/Release/ReleaseExcess with the record's
// own SessionID, so this broker-side mark is defensive and safely skipped. OrderTrack-in-
// snapshot durability is still B9 carried.
func (s *shard) finalizeTrack(aid AccountID, id OrderID) {
    if tr, ok := s.tracks[id]; ok && isTerminal(tr.Status) { delete(s.tracks, id) }
    sess, live := s.sessions[aid]
    if !live { return }
    s.bundle.Persist.MarkSnapshotDirty(sess.binding.SessionID, aid)
}
3.5 The load-bearing disciplines (the why)
Race discipline (settleCancelled): release r.CancelledQty (echoed) — a fill may have beaten the cancel (Exchange_Broker_Contract.docx line 298: "release CancelledQty as echoed, never the qty you assumed").
One-shot vs incremental: a taker gets one batched MatchResult; a resting order gets separate OutcomeMakerFill events over time (Exchange_Broker_Contract.docx §7–§8). Completion read from L5.
Diverted two-phase: markDiverted only ACKs and holds the reservation across an unbounded gap (Exchange_Broker_Contract.docx line 358); the auction-clear result lands on the same OrderID and runs settleMatch, releasing any ReasonAuctionUnfilled remainder.
settleCancelRejected is a no-op guard: channel order + in-order Private drain mean the terminating result already finalized the track; there's no live state, so no mark-dirty (not a manufactured persist).
Release completeness (B4 is a property, not code): every non-filled unit is freed by one branch (BROKER_ORDER_LIFECYCLE_intake_punch_settle.md §10).
Ignored errors (_ =) match the doc; Convert panics on the real invariant (no reservation record); softer failures get logged (not an error-handling policy — not this point's job).
ReleaseExcess no-op in case 4: when settleMatch's "fills + CancelledQty" branch runs, fills + CancelledQty = origQty by contract, so Release(CancelledQty) is always an exact-tail release (releaseQty == RemainingQty) — economy/reserve full-drains ReservedCash to zero and REMOVES the record. The subsequent IsMarketBuy && Filled → ReleaseExcess call then finds record == nil and silently returns, emitting no ReleaseEvent. The call is kept for uniformity with case 5 (clean full fill, where residue from price improvement + slippage buffer DOES get returned via ReleaseExcess), but observers of the event stream will only see a ReleaseExcess emission on case 5. Pinned by Golden 1 (positive residue) and Golden 4 (exact-tail no-op).
4. Point 4 — the order HTTP endpoints
4.1 Persistence question

Nothing. HTTP request goroutines: parse, route, shape. All state lives in the Point 1–3 handlers.

4.2 The pinned OrderSpec (the parked Point-1 gap, P5-D23)
POST /v1/orders body → internal OrderSpec{Instrument, Side, Type, TIF, Qty, LimitPrice}:
  { account_id, instrument, side:"buy"|"sell", type:"limit"|"market",
    tif:"gtc"|"ioc"|"fok", qty:int (minor units, §0.4), limit_price?:int (required iff type=="limit") }

Surface nit (flagged, not overridden): modify's body (BROKER_READ_SURFACES_tier4.md §9.1) uses is_market/new_price rather than type/limit_price — a locked doc choice; both build the same internal OrderSpec, left as written.

4.3 The entry-points (reused Tier-0 request/reply)

Broker.PlaceOrder/Broker.Cancel mirror Broker.Transfer (BROKER_PHASE3_IMPLEMENTATION_DESIGN.md §1.4; Tier-0 Point 7): build a buffered(1) ReplyTo, b.route(cmd), select on reply vs ctx.Done() (→ ErrBrokerTimeout → 504). PlaceOrder.SessionID (Tier-0) is vestigial — the handler resolves the session via s.sessions[AccountID] — so the entry-point leaves it zero (flagged).

4.4 The three endpoints
POST /v1/orders (place) = BROKER_READ_SURFACES_tier4.md §9.2 STEP B, standalone: parse → timeout → broker.PlaceOrder → shape.
POST /v1/orders/cancel = §9.2 STEP A, standalone: {account_id, order_id} → broker.Cancel → shape. (POST-with-body, not DELETE /{id}, to keep the no-auth body convention, §0.1.)
POST /v1/orders/modify — adopted from §9 unchanged: Option-1 gate-on-local-acceptance (punch cancel → if CancelFailed return 409, don't place, original untouched; if CancelPending → punch place; compose from both local results). Not idempotent (fresh OrderID). Partial failure (cancel pending + place rejected at Gate 4) → reported, never rolled back (§9.3): 200 {modify_status:"partial", warning:…}. No pre-cancel funds check (TOCTOU-racy, §9.3).
4.5 The status contract (P5-D26/D27/D28)

Accept = 202 (accepted-not-executed); real outcomes async via Point 3.

Result	Status	Body
Place Accepted / Cancel CancelPending	202	{order_id}
Malformed/missing body field	400	{error} (pre-funnel)
Non-POST method	405	{error} "method not allowed"; Allow: POST header set
ReasonNoSession (place or cancel)	409	{reason:"no_session"} (§0.5 mandate)
Cancel CancelFailed (any reason)	409	{reason} (§9.4 pins cancel-fail → 409, uniform)
Place business reject (validation / ReasonInsufficientFunds / ReasonNoMarketPrice)	422	{reason}
Timeout (ErrBrokerTimeout)	504	{error}
Unexpected broker error / panic recover (defensive)	500	{error} "internal error"
Modify (composed)	200	§9.4 body (modify_status: ok|partial|rejected)

No per-reason status codes (call B): business rejections all → 422 with the exact reason string in the body (the reasons are returned, they just don't each earn a code). Carve-out: ReasonNoSession → 409 because §0.5 mandates 409 for no-live-session. 422 is a flagged extension of §0.5's list (which had no business-rejection code). ReasonInsufficientFunds is not split funds-vs-units (call C). Modify's 200 vs standalone's 202/4xx is intentional — modify is two actions that can partially succeed (a punched cancel can't become a hard error); standalone are single actions.

405 and 500 are standard HTTP hygiene additions on top of P5-D27's business table: a non-POST method hits the Allow-header method guard before any body parse, and every ServeHTTP installs a defer recoverPanic that catches a bubbled broker panic (reserve/realize hands can panic on invariant violations — root CLAUDE.md policy) and writes 500 instead of tearing the connection. Both land in {error} shape, not {reason}.

5. The order loop, consolidated (end to end)
POST /v1/orders → Broker.PlaceOrder → handlePlaceOrder (P5.1)
   validate → [market-buy: BestAsk] → nextOrderID → ReserveIfAvailable(Gate 4) → OrderTrack → s.punch
        └─ FundedOrder ─────────────► exchange Inbound      [PHASE 4 P1]
                                          │  matches/rejects/cancels → EngineResult
                                          ▼
                              exchange Private ──► runSettlementConsumer  [PHASE 4 P2]
                                   accountOf(OrderID) → route(ApplyResult{aid, r})
                                          │
                                          ▼
   handleApplyResult (P5.3): switch Outcome → Convert/Release/ReleaseExcess + Status + finalizeTrack

POST /v1/orders/cancel → Broker.Cancel → handleCancel (P5.2): mark in-flight → s.punch(cancel)
POST /v1/orders/modify → compose cancel + place (P5.4, §9)

Out on Inbound, back on Private; correlated by OrderID+BrokerID; no synchronous reply.

6. Decisions made (Phase 5 decision log)
P5-D1 — Two-record division. L5 ReservationRecord = money truth (never duplicated); broker OrderTrack = thin exchange-seam (6 fields). Completion read from L5, never mirrored. LOCKED.
P5-D2 — ReserveIfAvailable returns error only (correction to the doc's res, _ := / res.(Rejected)). Reject mapping via errors.Is on L5's typed reserve sentinels. LOCKED.
P5-D3 — Market-buy reservation via two inputs, not a pre-computed cap. Broker reads mktCache.BestAsk
sizes SlippageBuffer, passes both into reserve.Order (L5 computes requiredCash); broker computes the equal maxSpend for FundedOrder.MaxSpend. Designed against the BestAsk interface; runtime blocked on Point 5. LOCKED.
P5-D4 — nextOrderID(accountID) (doc's no-arg pseudo corrected): single shard-local uint64, no lock, "<AccountID>:<seq>". Crash-resume deferred to B9; resharding caveat noted. LOCKED.
P5-D5 — OrderTrack shape pinned (roadmap gap). IsMarketBuy = Buy && Market set at creation, gates ReleaseExcess; CancelInFlight set by Point 2. LOCKED.
P5-D6 — Reserve-before-punch; track created before punch. Any pre-punch failure punches nothing. STP: CancelIncoming stamped on every submit. LOCKED.
P5-D7 — Persistence split. L5 persists the reservation; OrderTrack is in-memory, snapshotting deferred to B9. LOCKED.
P5-D8 — slippageBuffer sizing = config-driven parameter, pinned when the market-buy path goes live with Point 5 (not a silent constant). OPEN (parameter).
P5-D9 — validateSpec instrument check → option (i): the session carries its instrument set, injected at session-install (cross-point: L7 session-install must fill it — §8). Rest of the switch (qty/side/price/type) LOCKED; the instrument-set source is carried.
P5-D10 — handleCancel is async: validate → mark in-flight → punch, no release. Release deferred to settlement on echoed CancelledQty. Via s.punch. LOCKED.
P5-D11 — Idempotent on CancelInFlight: a re-cancel returns CancelPending, punches nothing again. LOCKED.
P5-D12 — Status check is a best-effort pre-filter (Working/PartiallyFilled); ownership guard (tr.AccountID == c.AccountID) blocks cross-account cancels. LOCKED.
P5-D13 — CancelInFlight marked snapshot-dirty for crash-survival (no re-punch on recovery); OrderTrack-in-snapshot durability carried to B9 + session_save. LOCKED (mark); durability carried.
P5-D14 — No handleModify primitive. Modify = cancel+resubmit punched in sequence (exchange preserves same-shard channel order), composed at the interface tier (Point 4). LOCKED.
P5-D15 — handleApplyResult = six-outcome push adapter fed by the Phase-4 consumer: route → L5 op(s) → status → cleanup. No money logic (L5 owns it). LOCKED.
P5-D16 — Convert called 5-arg: makerPrice = t.Price; exitAt = time.Now() (no exchange timestamp exists — contract never imports "time"); tradeID = "<OrderID>:<Trade.Seq>" (exchange- correlated via Trade.Seq, OrderID-qualified because Trade.Seq is per-matcher). exitAt session- time alignment flagged for a future exchange fill timestamp (§8). LOCKED.
P5-D17 — settleMatch pure-rest guard: len(Trades)==0 && Rested != nil → Working; else with Rested → PartiallyFilled. Fixes the doc's code-vs-note contradiction (§6.1 line 264 vs 279). LOCKED.
P5-D18 — Race discipline: settleCancelled releases exactly r.CancelledQty (echoed). LOCKED.
P5-D19 — Diverted two-phase: markDiverted holds (no L5 call), Working; auction-clear settles via settleMatch on the same OrderID. LOCKED.
P5-D20 — finalizeTrack(aid, id) uses c.AccountID threaded from the ApplyResult command (resolved by Phase-4's accountOf). Fixes the doc's /* accountID */ placeholder — no capture-before- delete, no re-parse. LOCKED.
P5-D21 — settleCancelRejected clears the flag only, no finalize/mark — channel order guarantees the terminating result already finalized the track; defensive no-op. LOCKED.
P5-D22 — Completion read from L5 via ReservationService.ViewReservation (returns the RemainingQty field on a value-copy view; (zero, false) when the record is already gone is a legitimate completion signal), never mirrored on OrderTrack; release completeness (B4) is a property of B3's branches, no separate code. LOCKED.
P5-D23 — Three POST endpoints, explicit account_id in body (no auth, §0.1): /v1/orders, /v1/orders/cancel, /v1/orders/modify. OrderSpec pinned. LOCKED.
P5-D24 — Place = §9.2 STEP B, cancel = §9.2 STEP A, extracted standalone (five-layer flow §0.2; handler owns timeout → 504). Bodies are Points 1–2. LOCKED.
P5-D25 — Modify adopted from §9 unchanged: Option-1 gate-on-local-acceptance; partial-failure reported not rolled back; 200+modify_status. LOCKED.
P5-D26 — Accept code 202 for standalone place/cancel (accepted-not-executed); modify keeps 200 (composed body). LOCKED.
P5-D27 — Status buckets, not per-reason codes (B): 202 accept, 400 malformed body, 409 no-session (§0.5) + all cancel-fails (§9.4), 422 place business-rejections, 504 timeout — exact reason string in the body on every reject. 422 is a flagged extension of §0.5. LOCKED.
P5-D28 — ReasonInsufficientFunds not split funds-vs-units (C): one reason, one 422. LOCKED.
P5-D29 — settleMatch is a FIVE-branch switch, not four (deviation from §3.4's earlier draft which lumped zero-fill-cancel with fills+leftover). The split: zero-fill cancel (self-match, no-liquidity, IOC-leftover, circuit-clamp) terminates as Status=Cancelled because the order never settled anything; fills + non-resting leftover (funded-cap, auction-unfilled) terminates as Status=Filled because the filled portion did settle. Both branches Release(CancelledQty); the Status discriminates the terminal nature. Pinned by goldens 5-8 (zero-fill → Cancelled) and golden 4 (fills + funded-cap → Filled). LOCKED.
P5-D30 — MarkSnapshotDirty's real signature is (sessionID, accountID), not aid-only — the one-arg pseudo in §2.3 / §3.4 drafts was a doc shortcut, not the real seam. Call site: s.bundle.Persist.MarkSnapshotDirty(sess.binding.SessionID, aid), with sess resolved via s.sessions[aid]. Field path is s.bundle.Persist (per-shard capstone bundle) not a shard-level persist field. finalizeTrack and handleCancel both honor this; a !live session at finalize-time safely skips the broker-side mark (money-side mark already fired inside Convert/Release/ReleaseExcess with the record's own SessionID). LOCKED.
7. Constraints / invariants honored
Single-writer per account — every handler runs on the owner goroutine (place/cancel/settle are routed there; the HTTP layer only routes commands in). No shard map is touched off-goroutine.
L5 owns money, broker owns exchange-seam — the two-record line (P5-D1) holds everywhere: reservations/positions/cash/realization in L5; Status/CancelInFlight/IsMarketBuy on OrderTrack.
Reserve-before-punch / accepted-not-executed — nothing is punched before funds are held; nothing claims execution before settlement.
Race resolved not prevented — cancel is async; release only on the exchange's echoed qty.
Persistence-with-every-functionality — each point stated its persistence question: place → L5 reservation; cancel → in-flight flag mark; settle → L5 money + track-dirty; endpoints → none.
Correlation — OrderID ("<AccountID>:<seq>") + BrokerID carry account identity out and back with no shared index.
8. Deferred / carried items
P5-D8 — slippageBuffer sizing — a config-driven parameter (bps of notional), pinned when market-buy goes live with Point 5. OPEN.
P5-D9 — the session instrument set for knownInstrument — must be injected at L7 session-install alongside the binding; not defined in any doc today. Carried (cross-point).
OrderTrack durability (B9) — Status/CancelInFlight/IsMarketBuy in the account crash snapshot, and crash-resume of the shard OrderID counter (resume past the highest minted seq). Ties into Phase-2 session_save. Carried.
exitAt session-time alignment — time.Now() is wall-clock and won't match the exchange's session-local candle time under a simulated clock; a proper fix needs the exchange to expose a fill timestamp on EngineResult/Trade (an exchange-contract change). Carried.
Market-buy runtime — blocked on Point 5 (MarketDataCache.BestAsk); the branch is designed against the interface. Carried (depends on the deferred market-data component).
B13 rejection notification — a participant-scoped async surface for OutcomeRejected (BROKER_ORDER_LIFECYCLE_intake_punch_settle.md §6.4). Out of scope here.
9. Cross-doc notes (corrections to the authority docs)

Annotate BROKER_ORDER_LIFECYCLE_intake_punch_settle.md where source diverges from its pseudo-code:

§3 line 122 nextOrderID() → takes accountID (P5-D4).
§3 lines 127–128 res, _ := ...; res.(Rejected) → ReserveIfAvailable returns error only; map via errors.Is (P5-D2).
§6.1 line 258 Convert(r.OrderID, t.Qty, t.Price) → 5-arg Convert(orderID, fillQty, makerPrice, exitAt, tradeID) (P5-D16).
§6.1 lines 262–280 add the pure-rest guard (len(Trades)==0 && Rested → Working) — resolves the code-vs-note contradiction (P5-D17).
§6.6 line 356 MarkSnapshotDirty(/* accountID */) → thread c.AccountID into finalizeTrack(aid, id) (P5-D20).
§3/§4 Kind constants — always qualify contract.SubmitOrder/contract.CancelOrder (collision with the broker CancelOrder command).
Inline punch (§3/§4) already superseded by s.punch in BROKER_PHASE4_IMPLEMENTATION_DESIGN.md (P4-D1) — cross-ref.

Annotate BROKER_READ_SURFACES_tier4.md: §9 modify is adopted unchanged; the 422 business-reject code (P5-D27) extends the §0.5 status list for the standalone place endpoint.