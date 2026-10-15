Broker — Order Lifecycle: Intake → Punch → Settle

What this doc covers. The full life of an order through the broker: a consumer places it (intake), the broker reserves funds and sends it to the exchange (punch), the exchange reports back, and the broker settles the outcome (settle). Includes place, cancel, modify, and the six-outcome settlement adapter. This is the Tier-1 order core.

Companion docs.

BROKER_CONCURRENCY_AND_ROUTING.md — the shard/funnel machine these handlers run inside.
BROKER_RESPONSIBILITIES_AND_PLAN.md — the responsibility list + tier order.
BROKER_DESIGN_TRACKER.md — live status of every component.
Exchange_Broker_Contract.docx (project) — the authoritative exchange↔broker wire contract.
L5 docs (LAYER_5_2_RESERVATION_FLOWS.md, LAYER_5_3_COST_ENGINE.md, LAYER_5_4_REALIZATION.md) — the reservation/cost/realization methods this driver calls.

The one load-bearing principle. L5 is part of the broker. All reservation money state (reserved cash/units, RemainingQty, OrigQty, EverFilled, BrokerageCharged) lives in the L5 ReservationBook and is never duplicated. The broker adds only OrderTrack — thin exchange-seam state (status, cancel-in-flight, is-market-buy) that L5 has no reason to know. The broker is the "engine/driver" the L5 docs refer to: it calls ReserveIfAvailable / Convert / Release / ReleaseExcess and never re-implements what they do.

1. The two records
1.1 L5's ReservationRecord (unchanged — the reservation money truth)

Held in the L5 ReservationBook, keyed by OrderID, per-shard. Carries the reservation money state: ReservedCash/ReservedUnits, RemainingQty, OrigQty, EverFilled, BrokerageCharged, Side, IsMarket, InstrumentID. The broker never copies these — it reads or drives them through the L5 methods.

1.2 The broker's OrderTrack (thin, exchange-seam only)
go
// OrderTrack — the broker's per-order exchange-seam record. Holds ONLY what the exchange connection
// needs and L5 does not carry. No reserved amounts, no qty progress — those are the L5
// ReservationRecord's, read via the ReservationService.
type OrderTrack struct {
    OrderID        OrderID
    AccountID      AccountID
    Instrument     InstrumentID // for routing a cancel
    Status         OrderStatus  // Working | PartiallyFilled | Filled | Cancelled | Rejected
    CancelInFlight bool         // a cancel was punched, awaiting the exchange's ack
    IsMarketBuy    bool         // set at punch; drives the ReleaseExcess buffer true-up decision
}

Completion ("is this order fully filled?") is a reservation fact, so the broker asks L5 via a read accessor (ReservationService.RemainingQty(orderID) / IsComplete(orderID)) rather than mirroring qty on the track. Exchange-seam facts live on OrderTrack; reservation facts are read from L5. That line is the whole discipline.

1.3 The shard's holdings (recap from the concurrency doc)
go
type shard struct {
    // ... id, brokerID, inbox, sessions ...
    resv   *ReservationService        // L5 5.2 — owns the ReservationBook (all reservation state)
    tracks map[OrderID]*OrderTrack     // broker exchange-seam tracking only
    costs  CostModule                  // 5.3 (used by ReservationService)
    realiz Realizer                    // 5.4 (used by ReservationService)
    persist Persistence                // async snapshot marks
}
2. The exchange wire contract (the shapes this doc punches / consumes)

From Exchange_Broker_Contract.docx — verified, not inferred.

Punch (broker → exchange): InboundMessage — a wrapper.

Kind (SubmitOrder | CancelOrder), BrokerID, Instrument (routing key, required on both), Submit *FundedOrder, Cancel *CancelRequest.
FundedOrder: OrderID, Participant, Instrument, Side, Type, TIF, Qty, LimitPrice, MaxSpend, STP.
Participant = self-match id, distinct from BrokerID.
TIF: Limit→GTC, Market→IOC; FOK deferred.
STP: default CancelIncoming.
MaxSpend: market-buy cap (best_ask×qty + buffer); 0 otherwise.
CancelRequest: {OrderID} — whole-order only.

Consume (exchange → broker): EngineResult — six outcomes.

Outcome, OrderID (echoed on all), BrokerID (echoed for routing), Match *MatchResult, Rejection *Rejection, CancelledQty.
OutcomeMatched / OutcomeMakerFill / OutcomeDiverted / OutcomeRejected / OutcomeCancelled / OutcomeCancelRejected.
MatchResult: Trades []Trade, Rested *RestingOrder, CancelledQty, CancelReason, LastPrice, LastPriceMoved. Trade.Price = maker's resting price, always.
3. Intake & punch — handlePlaceOrder (B2)

Runs on the owning shard (single-writer). Rejections are normal return values, not errors.

go
func (s *shard) handlePlaceOrder(c PlaceOrder) PlaceResult {
    sess, ok := s.sessions[c.AccountID]
    if !ok {
        return PlaceResult{Status: Rejected, Reason: ReasonNoSession}
    }
    o := c.Order

    // STEP 1 — structural validation (cheap, local). Price-band/market-state is the exchange's job.
    if reason := s.validateSpec(sess, o); reason != ReasonOK {
        return PlaceResult{Status: Rejected, Reason: reason}
    }

    // STEP 2 — compute reservation inputs. Market buy needs best_ask from the (tape-fed) cache.
    var maxSpend int64
    if o.Side == Buy && o.Type == Market {
        bestAsk, ok := s.mktCache.BestAsk(o.Instrument)
        if !ok {
            return PlaceResult{Status: Rejected, Reason: ReasonNoMarketPrice}
        }
        maxSpend = bestAsk*o.Qty + slippageBuffer(bestAsk, o.Qty) // reserved == MaxSpend (D1 blend)
    }

    // STEP 3 — assign OrderID (globally unique, shard-local generation, no cross-shard coord).
    oid := s.nextOrderID()

    // STEP 4 — Gate 4: ReserveIfAvailable (L5). Atomic check-and-reserve. Fail → reject, nothing punched.
    //          L5 writes the ReservationRecord (all reservation money state) itself.
    res, _ := s.resv.ReserveIfAvailable(sess.participant, toL5Order(o, oid, c.AccountID, c.SessionID, maxSpend))
    if _, rejected := res.(Rejected); rejected {
        return PlaceResult{Status: Rejected, Reason: ReasonInsufficientFunds}
    }

    // STEP 5 — create the thin OrderTrack (exchange-seam only). No reservation fields here.
    s.tracks[oid] = &OrderTrack{
        OrderID: oid, AccountID: c.AccountID, Instrument: o.Instrument,
        Status: Working, IsMarketBuy: o.Side == Buy && o.Type == Market,
    }

    // STEP 6 — punch the real wrapper. Mode-blind: whichever handle L7 bound at session load.
    sess.binding.ExchangeInbound <- InboundMessage{
        Kind: SubmitOrder, BrokerID: s.brokerID, Instrument: o.Instrument,
        Submit: &FundedOrder{
            OrderID: oid, Participant: sess.participant, Instrument: o.Instrument,
            Side: o.Side, Type: o.Type, TIF: tifFor(o.Type),
            Qty: o.Qty, LimitPrice: o.LimitPrice, MaxSpend: maxSpend, STP: CancelIncoming,
        },
    }

    // (L5 already marked the snapshot dirty for the reservation. Mark OrderTrack dirty if snapshotted.)
    return PlaceResult{Status: Accepted, OrderID: oid}
}

func tifFor(t OrdType) TIF { if t == Limit { return GTC }; return IOC }

func (s *shard) validateSpec(sess *ownedSession, o OrderSpec) RejectReason {
    switch {
    case !sess.knownInstrument(o.Instrument): return ReasonUnknownInstrument
    case o.Qty <= 0:                          return ReasonBadQty
    case o.Side != Buy && o.Side != Sell:     return ReasonBadSide
    case o.Type == Limit && o.LimitPrice <= 0: return ReasonBadPrice
    case o.Type != Limit && o.Type != Market: return ReasonBadType
    }
    return ReasonOK
}

Notes:

Reservation amount: for limits and sells, ReserveIfAvailable computes it internally (LimitPrice×Qty, or units). The broker only computes MaxSpend for the market-buy case (the tape cache is the only extra input). The reserved figure is the MaxSpend punched — same number by construction, so exchange staleness is harmless (the exchange caps against MaxSpend).
sess.participant — the self-match id, injected at session load, distinct from BrokerID.
All reservation state (record creation, reserved amounts, RemainingQty) is L5's, inside ReserveIfAvailable. The broker stores none of it.
4. Cancel — handleCancel (A2)

Thin: validate, mark in-flight, punch. No reservation released here — release happens in settlement on the echoed ack, because the exchange is the authority on whether the order can still be pulled.

go
func (s *shard) handleCancel(c CancelOrder) CancelResult {
    sess, ok := s.sessions[c.AccountID]
    if !ok {
        return CancelResult{Status: CancelFailed, Reason: ReasonNoSession}
    }
    tr, ok := s.tracks[c.OrderID]
    if !ok || tr.AccountID != c.AccountID {
        return CancelResult{Status: CancelFailed, Reason: ReasonUnknownOrder}
    }
    if tr.Status != Working && tr.Status != PartiallyFilled {
        return CancelResult{Status: CancelFailed, Reason: ReasonNotCancellable}
    }
    if tr.CancelInFlight {
        return CancelResult{Status: CancelPending, OrderID: c.OrderID} // idempotent
    }

    tr.CancelInFlight = true
    s.persist.MarkSnapshotDirty(c.AccountID) // in-flight flag survives crash → no re-punch
    sess.binding.ExchangeInbound <- InboundMessage{
        Kind: CancelOrder, BrokerID: s.brokerID, Instrument: tr.Instrument,
        Cancel: &CancelRequest{OrderID: c.OrderID},
    }
    return CancelResult{Status: CancelPending, OrderID: c.OrderID}
}

The broker's Status check is a cheap pre-filter only; the exchange is authoritative (our "Working" may already be filled there — the race). That is exactly why release waits for the echoed ack.

5. Modify (A3)

No handleModify primitive. Modify = cancel + resubmit, punched in sequence, no waiting — the exchange preserves channel order on the same shard, so the cancel is processed before the new submit. The interface layer composes it: issue CancelOrder, then immediately PlaceOrder with new params. Internally it is just handleCancel then handlePlaceOrder, unchanged.

(The consumer-facing "modify" endpoint that composes the two into one response is an interface-tier item — see the tracker.)

6. Settle — handleApplyResult (B3): the six-outcome adapter

Push command, no reply. Translates one EngineResult into calls on the L5 ReservationService. Implements no money logic — Convert/Release/ReleaseExcess do all of it (principal moves, cost 5.3, realization 5.4, brokerage-once, snapshot marks). B3 only sequences those calls and tracks exchange status.

go
func (s *shard) handleApplyResult(c ApplyResult) {
    r := c.Result
    switch r.Outcome {
    case OutcomeMatched:        s.settleMatch(r)
    case OutcomeMakerFill:      s.settleMakerFill(r)
    case OutcomeDiverted:       s.markDiverted(r)
    case OutcomeRejected:       s.settleRejected(r)
    case OutcomeCancelled:      s.settleCancelled(r)
    case OutcomeCancelRejected: s.settleCancelRejected(r)
    default:
        panic(fmt.Sprintf("apply: unknown outcome %v for order %s", r.Outcome, r.OrderID))
    }
}
6.1 Matched (taker fill — one-shot)
go
// One EngineResult, all prints in Match.Trades. Convert per trade at its maker price; then reconcile
// the remainder (rest / cancel); then market-buy buffer true-up.
func (s *shard) settleMatch(r EngineResult) {
    m := r.Match
    tr := s.tracks[r.OrderID]

    for _, t := range m.Trades {
        _ = s.resv.Convert(r.OrderID, t.Qty, t.Price) // L5: shrink reservation, move principal,
                                                       // apply cost, brokerage@0, sell→5.4 — all internal
    }

    switch {
    case m.Rested != nil:
        tr.Status = PartiallyFilled // remainder now resting as a maker; reservation STAYS HELD in L5
    case m.CancelledQty > 0:
        _ = s.resv.Release(r.OrderID, m.CancelledQty) // remainder that didn't fill/rest — release it
        tr.Status = Filled
    default:
        tr.Status = Filled // fully filled
    }

    if tr.IsMarketBuy && tr.Status == Filled {
        _ = s.resv.ReleaseExcess(r.OrderID) // return unused MaxSpend buffer
    }
    s.finalizeTrack(r.OrderID)
}

(Empty Trades + Rested = a limit that rested outright: Status = Working, reservation fully held.)

6.2 Maker fill (incremental — the normal resting-order case)
go
// A RESTING order of ours was hit. Incremental: many takers → many of these over the order's life.
// Convert the fill; the remainder stays working. Completion is a reservation fact — ask L5.
func (s *shard) settleMakerFill(r EngineResult) {
    tr := s.tracks[r.OrderID]
    for _, t := range r.Match.Trades {
        _ = s.resv.Convert(r.OrderID, t.Qty, t.Price) // maker price = our resting price
    }
    if s.resv.RemainingQty(r.OrderID) == 0 { // read accessor on L5 — reservation fact, not mirrored
        tr.Status = Filled
        s.finalizeTrack(r.OrderID)
    } else {
        tr.Status = PartiallyFilled
    }
}
6.3 Diverted (auction ACK — hold; second result settles later)
go
// ACK only: order sits in the closing-auction batch. HOLD the reservation (no L5 call). A SECOND
// result arrives at auction clear, same OrderID, EngineResult-shaped, fills at P* — and is handled
// by settleMatch exactly like a continuous fill (unfilled remainder carries ReasonAuctionUnfilled).
func (s *shard) markDiverted(r EngineResult) {
    s.tracks[r.OrderID].Status = Working
}
6.4 Rejected
go
// Admission failed; nothing entered the book. Release the WHOLE reservation.
func (s *shard) settleRejected(r EngineResult) {
    _ = s.resv.Release(r.OrderID, 0) // 0 = release all
    s.tracks[r.OrderID].Status = Rejected
    s.finalizeTrack(r.OrderID)
    // participant-scoped rejection notification to the consumer = B13 / async surface
}
6.5 Cancelled / CancelRejected (the race discipline)
go
// Our explicit cancel succeeded. Release EXACTLY the echoed CancelledQty — never the assumed
// remainder (a fill may have beaten the cancel, making this smaller). L5 Release frees that slice
// and charges partial-then-cancel brokerage if the order ever filled.
func (s *shard) settleCancelled(r EngineResult) {
    _ = s.resv.Release(r.OrderID, r.CancelledQty)
    tr := s.tracks[r.OrderID]
    tr.CancelInFlight = false
    tr.Status = Cancelled
    s.finalizeTrack(r.OrderID)
}

// The order was already gone (filled / already cancelled) before the cancel landed. No release —
// any fill settled / will settle via its own result. Just clear the in-flight flag. Normal outcome.
func (s *shard) settleCancelRejected(r EngineResult) {
    if tr, ok := s.tracks[r.OrderID]; ok {
        tr.CancelInFlight = false
    }
}
6.6 The cleanup helper
go
// finalizeTrack — remove the OrderTrack when terminal, mark snapshot dirty. The L5 reservation
// record self-deletes inside Convert/Release/ReleaseExcess; this cleans up only the broker's track.
func (s *shard) finalizeTrack(id OrderID) {
    if tr, ok := s.tracks[id]; ok && isTerminal(tr.Status) {
        delete(s.tracks, id)
    }
    s.persist.MarkSnapshotDirty(/* accountID */)
}
7. The one L5 read accessor this driver needs

ReservationService.RemainingQty(orderID) int64 (or IsComplete(orderID) bool) — a read-only accessor so the broker can tell whether a maker fill finished the order. This is L5 answering a question about its own state, not logic duplication and not the broker mirroring reservation fields. It is the correct side of the line: exchange-seam facts on OrderTrack; reservation facts read from L5.

8. What B3 does NOT do (because L5 already does)

Principal cash/position moves; cost computation + debit (5.3); FIFO realization + trade-row assembly (5.4); brokerage-once; reservation-record lifecycle; all MarkSnapshotDirty for money state. B3 is purely: route outcome → call the right L5 method(s) → update exchange status.

9. Persistence summary
Reservation money state — persisted by L5 (records already in the L5 crash snapshot, l5:snap:...:resv:{order_id}). Broker re-persists nothing here.
OrderTrack (Status / CancelInFlight / IsMarketBuy) — the only broker-added per-order state; in the snapshot if it must survive a crash (small addition).
Closed trades — L5 5.4 enqueues to the write-behind buffer on a round-trip close (session-type gated). Broker drives the sell fill that triggers it; the write path is L5's.
No new persistence mechanism anywhere — the broker inherits L5's.
10. Release completeness (B4) — satisfied by B3's branches

Every non-filled unit is released by one of B3's paths: rejection → full Release; matched remainder that didn't rest → Release(CancelledQty); market-buy buffer → ReleaseExcess; explicit cancel → Release(echoed CancelledQty). The CancelReason enum (NoLiquidity / FundedCap / CircuitClamp / IOCLeftover / SelfMatch / AuctionUnfilled) rides in MatchResult.CancelReason — diagnostic; the release qty is what the broker acts on. No separate B4 code — it is a property of B3.