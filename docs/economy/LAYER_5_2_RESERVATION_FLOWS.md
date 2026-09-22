Layer 5.2 — Reservation Service (Implementation Flows)

Scope: the four methods of the 5.2 reservation "brain" at implementation depth — items 24–31 on the Layer-5 action list — plus the reservation book (item 28), money-event emission (item 29), the 5.4 realization handoff (item 30), and the funded-cap guarantee (item 31). Design flow and the Go structs / method shapes for each, wired to the closed Tier-1 hands, the 5.3 cost engine, and the 5.4 realizer.

Relationship to LAYER_5_2_METHOD_FLOWS.md: that earlier doc captured the four methods at design-and-behaviour depth, written against 5.1's intent while storage was open. This document supersedes it for implementation: it binds the flows to the locked Tier-1 hand signatures (PascalCase), resolves the read-compare-vs-hand-guard relationship, pins persistence to in-memory + async Redis mirror, and folds in the brokerage-at-termination wiring that the closed 5.3 design forces. The behaviour is unchanged; only the wiring is made concrete.

What this document is NOT: it is not the 5.1 hands (LAYER_5_1_OPERATIONS.md, LAYER_5_1_READ_OPERATIONS.md), not the 5.3 cost engine (LAYER_5_3_COST_ENGINE.md), not the 5.4 realizer (LAYER_5_4_REALIZATION.md) — all referenced by their locked names. It does not design the money-event system internals (a deferred pass, §9) or the Tier-2 persistence write path.

Status: items 24–31 LOCKED at implementation-flow depth. The reservation book is Option 2 (5.2-held, separate from Account, whole-economy scope). Item 32 (per-archetype Gate-4 branch) is deferred to Layer-2 agent design. Item 29 (event emission) is wired at every call site but its event-system design is deferred (§9). Open/deferred in §10.

Naming. Doc labels use snake_case for readability; Go identifiers are PascalCase for all exported names; unexported helpers are camelCase.

0. Orientation — brain vs hands, and the one external consult

5.2 is the reservation brain: it decides what moves and why, and directs the Tier-1 hands (5.1) to perform the mutation. 5.2 holds no balances — every "move X" is an in-process function call into 5.1 (same session goroutine, no IPC, no network, no locks; race-freedom is inherited from the engine's single-goroutine-per-session serialization). 5.1 is the sole writer; 5.2 never mutates an account field.

5.2 is the engine's one external consult: the engine's Admission Gate 4 calls ReserveIfAvailable synchronously per order and blocks on the return. The other three methods (Convert, Release, ReleaseExcess) are lifecycle bookkeeping the engine drives afterward.

engine Gate 4 ──sync, blocking──► ReserveIfAvailable   (decision)
engine (each fill) ─────────────► Convert              (lifecycle bookkeeping)
engine (non-fill terminal) ─────► Release              (lifecycle bookkeeping)
engine (market-buy complete) ───► ReleaseExcess        (buffer true-up)
        │
        ▼  in-process calls to
TIER 1 hands (5.1): MoveCash, MovePosition, CreditPosition, RemovePosition, CreditCash, DebitCash,
                    ReadAvailableCash, ReadAvailablePosition
        │
        ▼ money truth
   in-memory Account  ──async mirror──► Redis snapshot (crash recovery)
0.1 The core invariant (held at every instant)

The reservation covers exactly the still-unfilled quantity; debits/credits cover exactly the filled quantity; the two never overlap.

Every method preserves this. It is what makes a partially-filled-then-cancelled order correct: the filled part was converted, the unfilled remainder is released, no overlap.

0.2 The money events (5.2 emits; the engine does not)

reserve, convert, release — the economy domain's events, kept out of the engine to preserve the domain split. Wired at every call site here; the event-system design is deferred (§9).

1. Shared types and structures
1.1 The reservation book — Option 2 (5.2-held, separate from Account)

The reservation record is 5.2's own state — bookkeeping about pending orders, not money. Cash and positions are the ledger (5.1's); the reservation record is the memory linking an admission to its lifecycle (5.2's brain state). It therefore lives in a 5.2-held structure, separate from the 5.1 Account — never mixed into Account. Records are keyed by order_id and carry the AccountID/SessionID they bind to (proper keys linking a reservation to an account for a session), so the book can be filtered by account for snapshotting without Account knowing it exists.

Scope: one book for the whole economy (records carry their own SessionID/AccountID). Per-session scoping is a viable later switch if needed; whole-economy is the locked choice for now.

go
type OrderID      = string
type AccountID    = string
type SessionID    = string
type InstrumentID = string

type Side int
const (
    Buy Side = iota
    Sell
)

// ReservationRecord — 5.2's per-order state. Links an admission to its later convert/release.
// Held in the book (Option 2), NOT inside the 5.1 Account. AccountID/SessionID link it back to
// the account+session (NOT a pointer into the Account).
type ReservationRecord struct {
    OrderID      OrderID
    AccountID    AccountID    // link back to the 5.1 account (NOT a pointer into it)
    SessionID    SessionID    // link to the session (snapshot key scope)
    InstrumentID InstrumentID // needed for sell-side position moves
    Side         Side
    IsMarket     bool         // affects buffer true-up (ReleaseExcess)

    ReservedCash  int64 // buy: cash still locked for the unfilled qty. 0 for sells.
    ReservedUnits int64 // sell: position units still locked for the unfilled qty. 0 for buys.
    RemainingQty  int64 // qty not yet filled

    // cost-lifecycle bookkeeping (forced by the locked 5.3 brokerage rule — see §5):
    OrigQty          int64 // original order qty, for OrderCostInput.TotalQty
    EverFilled       bool  // set true on the first Convert; drives ComputeOrderCost
    BrokerageCharged bool  // ensures the per-executed-order charge fires exactly once
}

// ReservationBook — 5.2-held, whole-economy scope. Keyed by order_id. AccountID/SessionID live
// inside each record, so the book can be filtered by account for snapshotting without the Account
// struct knowing it exists.
type ReservationBook struct {
    records map[OrderID]*ReservationRecord
}
1.2 The service and its dependencies
go
// ReservationService — 5.2. Calls DOWN into the Tier-1 hands (5.1), the 5.3 cost engine, the 5.4
// realizer, the event sink, and the async persistence layer. Holds only the reservation book.
type ReservationService struct {
    book    *ReservationBook
    ledger  *AccountRegistry // resolves AccountID -> *Account (in-memory working copies)
    costs   CostModule       // 5.3 (market-agnostic interface)
    realiz  Realizer         // 5.4
    events  EventSink        // emits reserve/convert/release (event system deferred)
    persist Persistence      // async snapshot mark; write-behind enqueue (via 5.4)
}
1.3 The persistence seam — async only, never on the hot path

Per the locked Tier-1 rule, no 5.2 method writes Redis or Postgres synchronously on the hot path. The methods mutate in-memory (the Account via hands, the ReservationBook directly); the async snapshotter mirrors on cadence. So the persistence calls wired here are mark-dirty / enqueue style, not blocking store writes.

go
type Persistence interface {
    // Async. Marks the account's live state dirty so the background snapshotter mirrors
    // {cash, positions, reservations} to Redis on its cadence. NON-BLOCKING: no Redis hit on the
    // hot path.
    MarkSnapshotDirty(sessionID SessionID, accountID AccountID)

    // Async. On a close, push the closed-trade row (+cost row) to the Redis buffer. Off the hot
    // path; the drainer moves it to Postgres later. Called via 5.4 on realization (shown for wiring).
    EnqueueClosedTrade(t ClosedTradeRow)
}
2. Item 24 — ReserveIfAvailable(participant, order) → reserved | reason
2.1 Purpose

The engine's one external consult, at Gate 4. The only decision method. Atomic check-and-reserve in one indivisible operation: a separate check-then-reserve would let two orders from one participant both pass the check before either reserves (over-commitment — the exact bug reservation exists to prevent).

2.2 When the engine calls it

Synchronously, inline in the session goroutine, as the last admission gate, for every order — human or agent (same gate, no bypass; the engine is blind to who placed it). The gate blocks on the return. A fired stop that re-enters admission hits this exactly like a fresh order (5.2 has no stop-specific logic).

2.3 Inputs
participant — identity + type. (Gate 4 is parameterized by participant type — a human hits the three-pool account; an agent archetype may be budget-constrained or pass-through. Structure identical for all; only what the check does varies. The per-archetype rule is item 32, deferred to Layer-2.)
order — side, instrument, qty, type (limit/market), limit_price (if limit).
Also read from the engine/rulebook (not the caller): best_ask (market-buy estimate), slippage_buffer (rulebook param).
2.4 The read-compare vs hand-guard relationship (resolved)

The locked MoveCash already enforces sufficiency internally (it rejects with ErrInsufficient, no mutation). So there are two guards for the same condition, and the flow resolves their relationship:

The explicit ReadAvailableCash() compare decides the return — ReserveIfAvailable must return a typed reason (insufficient funds / insufficient position) to Gate 4 without having mutated anything. The gate wants the decision before any move.
The hand's sufficiency guard is a never-should-fire backstop. If the compare said yes but the hand returns ErrInsufficient, the read and the move disagreed within one goroutine — impossible under single-threading = a programming error, handled by panic (the same discipline as the hands' own invariant panics), not a swallowed error.

One consequence of Option B (signed Available): ReadAvailableCash() may return negative, and the compare available >= required (with required > 0) correctly fails on a negative balance — exactly the "no buying power while underwater" behaviour. Falls out for free.

2.5 Flow
go
// ReserveIfAvailable — Gate 4. The ONE decision method. Atomic check-and-reserve, in-process, in
// the session goroutine. Returns a typed reason to the engine WITHOUT mutating on failure.
func (s *ReservationService) ReserveIfAvailable(
    participant Participant, order Order,
) (ReserveResult, error) {

    // 0. Resolve the in-memory account (loaded once at session start; no store hop).
    a := s.ledger.MustGet(order.AccountID) // panics if not primed — ordering bug, hot-path invariant

    switch order.Side {

    // ───────────────────────────── BUY ─────────────────────────────
    case Buy:
        // 1. Compute required cash. 5.2 READS the buffer; it does not set policy.
        var requiredCash int64
        if order.IsMarket {
            requiredCash = order.BestAsk*order.Qty + order.SlippageBuffer
        } else {
            requiredCash = order.LimitPrice * order.Qty
        }

        // 2. DECISION read (hot-path, in-memory). May be negative (Option B) — that correctly
        //    fails any positive requirement ("no buying power underwater").
        if a.ReadAvailableCash() < requiredCash {
            return Rejected{Reason: InsufficientFunds}, nil // NO state change
        }

        // 3. RESERVE via the hand. The read proved sufficiency, so this MUST succeed; an
        //    ErrInsufficient here = read/move disagreed within one goroutine = programming error.
        if err := a.MoveCash(Available, Reserved, requiredCash); err != nil {
            panic(fmt.Sprintf("reserve: MoveCash failed after positive check: %v", err))
        }

        // 4. Write the reservation record (in the BOOK, not the Account).
        s.book.records[order.OrderID] = &ReservationRecord{
            OrderID: order.OrderID, AccountID: order.AccountID, SessionID: order.SessionID,
            InstrumentID: order.InstrumentID, Side: Buy, IsMarket: order.IsMarket,
            ReservedCash: requiredCash, RemainingQty: order.Qty, OrigQty: order.Qty,
        }

        // 5. Emit money-event (in-memory event stream; not a store write).
        s.events.Emit(ReserveEvent{OrderID: order.OrderID, AccountID: order.AccountID,
            Kind: "reserve", Amount: requiredCash})

        // 6. Persistence: mark live state dirty so the async snapshotter mirrors {cash split + this
        //    new reservation} to Redis. NON-BLOCKING.
        s.persist.MarkSnapshotDirty(order.SessionID, order.AccountID)

        return Reserved{}, nil

    // ───────────────────────────── SELL ────────────────────────────
    case Sell:
        // 1. Required is a POSITION, not cash.
        // 2. DECISION read (hot-path, in-memory). Missing instrument -> 0 -> refuses correctly.
        if a.ReadAvailablePosition(order.InstrumentID) < order.Qty {
            return Rejected{Reason: InsufficientPosition}, nil // NO state change
        }

        // 3. RESERVE units via the hand. Must succeed (read proved it) — else programming error.
        if err := a.MovePosition(order.InstrumentID, Available, Reserved, order.Qty); err != nil {
            panic(fmt.Sprintf("reserve: MovePosition failed after positive check: %v", err))
        }

        // 4. Reservation record (BOOK).
        s.book.records[order.OrderID] = &ReservationRecord{
            OrderID: order.OrderID, AccountID: order.AccountID, SessionID: order.SessionID,
            InstrumentID: order.InstrumentID, Side: Sell, IsMarket: order.IsMarket,
            ReservedUnits: order.Qty, RemainingQty: order.Qty, OrigQty: order.Qty,
        }

        // 5. Emit position-lock reserve event.
        s.events.Emit(ReserveEvent{OrderID: order.OrderID, AccountID: order.AccountID,
            Kind: "reserve", Units: order.Qty})

        // 6. Async snapshot mark.
        s.persist.MarkSnapshotDirty(order.SessionID, order.AccountID)

        return Reserved{}, nil
    }
    panic("unreachable: unknown side")
}
2.6 Output

reserved (engine passes the order to the matcher) | insufficient funds | insufficient position (engine rejects with that typed reason).

2.7 The funded-cap (item 31) — where it lives

5.2 reserves best_ask × qty + buffer for market buys. If the walk fills worse than the buffer covers and the participant has nothing left, the engine caps the fill at the affordable quantity at walk-time and cancels the remainder (mechanically the same as the walk stopping at a circuit, just triggered by funds). So:

5.2's structural guarantee: you can only ever Convert what you reserved → no negative balance is possible by construction (the MoveCash(Reserved, Settled, ...) in Convert is sufficiency-guarded against Reserved).
The engine's behavioural guarantee: the walk-time cap stops the fill before it exceeds affordability.

Together: no debt, no leverage from reservation.

2.8 Atomicity

Free. This runs inside the session's single goroutine (single-active-session rule → one active session per user per market), so no two orders from one participant race here. No lock needed.

2.9 5.1 hands used

ReadAvailableCash / ReadAvailablePosition; MoveCash(Available, Reserved, amount) / MovePosition(instrument, Available, Reserved, units).

2.10 Flow diagram
                 engine Gate 4
                      │
          ReserveIfAvailable(participant, order)
                      │
              ┌───────┴───────┐
            BUY              SELL
              │                │
   required_cash =     required = qty units
   limit: price×qty          │
   market: best_ask×qty      │
           + buffer          │
              │                │
   ReadAvailableCash ≥   ReadAvailablePosition
   required_cash?         ≥ qty?
        ┌──┴──┐            ┌──┴──┐
       yes    no          yes    no
        │      │            │      │
  MoveCash   return    MovePosition return
  avail→rsv  "insuff.  avail→rsv    "insuff.
        │     funds"        │      position"
  write record        write record
  emit reserve        emit reserve
  MarkSnapshotDirty   MarkSnapshotDirty
  return reserved     return reserved
3. Item 25 — Convert(order_id, fill_qty, maker_price)
3.1 Purpose

Turn reservation into actual settlement, incrementally, on every fill (every partial). Fills happen in pieces at possibly different maker prices, so this is called repeatedly per order. It maintains the core invariant: reservation always covers exactly the unfilled qty; debits/credits cover exactly the filled qty.

3.2 When the engine calls it

On each fill. Fill-at-maker-price is a universal core invariant — every fill executes at the resting/maker order's price, so maker_price is authoritative. A resting limit maker trues its reservation to exactly zero on full fill; a market order fills at worsening maker prices and its leftover buffer is released later by ReleaseExcess; an auction fill converts at the clearing price (same true-up shape).

3.3 Cost-application ordering (locked)

Principal move + position delivery first, then the 5.3 cost debit after, consistent across buy and sell. The trade's principal settles first; friction rides on top.

3.4 Brokerage at RemainingQty == 0 (locked; the 5.3 wiring)

When RemainingQty hits 0 inside Convert (order fully executed), the per-executed-order brokerage is charged once, guarded by BrokerageCharged. EverFilled is set true on any Convert (a Convert means a fill happened). See §5 for the full cost wiring and the partial-cancel case in Release.

3.5 Flow

Signature history — augmented during stage-3 implementation. The behavioural spec above (§3.1–§3.4) is unchanged; the Go signature simply carries two extra values the `realize.RealizationInput` needs and this package cannot fabricate:

- `exitAt time.Time` — the fill's timestamp. `CreditPosition` needs it for the new lot's `EntryAt`; 5.4's accumulator tracks the max `ExitAt` across a round-trip's folded fills. Passed in from the fill event so this package holds no clock (replay-deterministic).
- `tradeID realize.TradeID` — the upstream fill event's trade id, may be empty. 5.4's accumulator keeps the first non-empty across the round-trip and generates one at flush if all fills were empty.

`Direction` on the `RealizationInput` is **hardcoded to `"long"`** inside the sell branch, per the long-only prototype scope (handover note 12: when shorts land, only THIS caller changes — nothing in realize's compute knows about direction). Reserve does NOT read Direction from any input.

go
// Convert — called on EVERY fill (every partial). Side-aware. Maintains the core invariant.
func (s *ReservationService) Convert(orderID OrderID, fillQty int64, makerPrice int64, exitAt time.Time, tradeID realize.TradeID) error {
    rec := s.book.records[orderID]
    if rec == nil {
        panic("convert: no reservation record for order (invariant violation)")
    }
    a := s.ledger.MustGet(rec.AccountID)

    switch rec.Side {

    // ───────────────────────────── BUY convert ─────────────────────────────
    case Buy:
        actualCost := makerPrice * fillQty

        // 1. Principal debit: reserved -> settled (the real spend, at MAKER price).
        if err := a.MoveCash(Reserved, Settled, actualCost); err != nil {
            panic(fmt.Sprintf("convert buy: reserved underflow: %v", err)) // invariant broken
        }
        // 2. Deliver the units bought (entry price for the lot = maker price; entry time = now).
        a.CreditPosition(rec.InstrumentID, fillQty, makerPrice, now())

        // 3. Shrink the reservation to cover only still-unfilled qty.
        rec.ReservedCash -= actualCost
        rec.RemainingQty -= fillQty

        // 4. COST STACK (5.3): per-fill turnover charges (buy-side), debit via hand.
        //    ORDERING (locked): principal move + position credit FIRST, cost debit AFTER.
        cost := s.costs.ComputeFillCost(FillCostInput{
            Side: Buy, FillQty: fillQty, MakerPrice: makerPrice, IsAutosquare: false,
        })
        if cost.Total > 0 {
            a.DebitCash(Available, cost.Total) // Option B: may drive Available negative; no gate.
        }

        // 5. Emit convert money-event (cash debited + position credited).
        s.events.Emit(ConvertEvent{OrderID: orderID, AccountID: rec.AccountID,
            Kind: "convert", CashDebited: actualCost, UnitsCredited: fillQty, Cost: cost})

        // 6. Async snapshot mark (cash split + position lots + shrunk reservation all changed).
        s.persist.MarkSnapshotDirty(rec.SessionID, rec.AccountID)

    // ───────────────────────────── SELL convert ────────────────────────────
    case Sell:
        proceeds := makerPrice * fillQty

        // 1. Deliver units OUT of reserved (gone). Returns matched lots for 5.4 to price.
        matched := a.RemovePosition(rec.InstrumentID, fillQty)

        // 2. Credit proceeds to AVAILABLE, immediately re-tradeable (STL-2, no T+1).
        a.CreditCash(Available, proceeds)

        // 3. Shrink the reservation.
        rec.ReservedUnits -= fillQty
        rec.RemainingQty  -= fillQty

        // 4. COST STACK (5.3): sell-side charges (STT, etc.), debit via hand — AFTER proceeds land.
        cost := s.costs.ComputeFillCost(FillCostInput{
            Side: Sell, FillQty: fillQty, MakerPrice: makerPrice, IsAutosquare: false,
        })
        if cost.Total > 0 {
            a.DebitCash(Available, cost.Total)
        }

        // 5. Emit convert (position delivered + cash credited).
        s.events.Emit(ConvertEvent{OrderID: orderID, AccountID: rec.AccountID,
            Kind: "convert", UnitsDelivered: fillQty, CashCredited: proceeds, Cost: cost})

        // 6. HANDOFF to 5.4 (item 30): a sell is a realization. 5.4 runs FIFO-matched realized P&L
        //    on `matched`, folds into its round-trip accumulator, and on a position CLOSE enqueues
        //    the closed-trade row to the write-behind buffer itself. (5.2 moves money; 5.4 computes
        //    the P&L narrative and owns the trade-record write.) PositionNowFlat is read from the
        //    post-RemovePosition state and passed so 5.4 knows whether this fill closed the position.
        s.realiz.OnSellFill(RealizationInput{
            AccountID: rec.AccountID, SessionID: rec.SessionID, InstrumentID: rec.InstrumentID,
            FillQty: fillQty, MakerPrice: makerPrice, MatchedLots: matched, Cost: cost,
            PositionNowFlat: a.PositionFlat(rec.InstrumentID),
        })

        // 7. Async snapshot mark.
        s.persist.MarkSnapshotDirty(rec.SessionID, rec.AccountID)
    }

    // ── Per-executed-order brokerage at full execution (locked 5.3 wiring, §5) ──
    rec.EverFilled = true // any Convert means at least one fill happened
    if rec.RemainingQty == 0 && !rec.BrokerageCharged {
        oc := s.costs.ComputeOrderCost(OrderCostInput{
            EverFilled: rec.EverFilled, TotalQty: rec.OrigQty, Side: rec.Side,
        })
        if oc.Total > 0 {
            a.DebitCash(Available, oc.Total)
            s.events.Emit(ConvertEvent{OrderID: orderID, AccountID: rec.AccountID,
                Kind: "convert", Cost: oc})
        }
        rec.BrokerageCharged = true
    }

    // Clean up a fully-filled record (both sides): nothing reserved, nothing remaining.
    if rec.RemainingQty == 0 && rec.ReservedCash == 0 && rec.ReservedUnits == 0 {
        delete(s.book.records, orderID)
    }
    return nil
}
3.6 What is abstracted to other components
The closed-trade persistence (write-behind enqueue + durable rows) is not done in Convert. It is handed to 5.4 (OnSellFill), which computes the realized P&L that fills those rows and writes only on an actual position close (not every partial). Convert moves money and hands off.
ComputeFillCost / DebitCash interleave — 5.3 computes; the hand debits. Convert orchestrates the order but owns neither.
3.7 5.1 hands used
buy → MoveCash(Reserved, Settled, actualCost) + CreditPosition(instrument, fillQty, makerPrice, now)
sell → RemovePosition(instrument, fillQty) + CreditCash(Available, proceeds)
both (terminal) → DebitCash(Available, cost.Total) per fill; DebitCash(Available, oc.Total) for brokerage at RemainingQty == 0.
3.8 Flow diagram
             engine (on each fill)
                     │
        Convert(order_id, fill_qty, maker_price)
                     │
              look up record
                     │
             ┌───────┴───────┐
           BUY              SELL
             │                │
  actual_cost =        proceeds =
  maker×fill_qty        maker×fill_qty
             │                │
  MoveCash             RemovePosition
  reserved→settled     reserved (−fill_qty) → matched
  (−actual_cost)             │
             │         CreditCash
  CreditPosition       available (+proceeds)
  (+fill_qty)               │
             │         shrink record
  shrink record        (reserved_units, remaining)
  (reserved_cash,           │
   remaining)         ComputeFillCost(Sell)+DebitCash
             │              │
  ComputeFillCost(Buy) emit convert
  + DebitCash          OnSellFill → 5.4 (realization)
             │              │
  emit convert         MarkSnapshotDirty
  MarkSnapshotDirty
             └───────┬───────┘
                     │
         RemainingQty == 0 && !BrokerageCharged?
                     │ yes
         ComputeOrderCost + DebitCash (once)
                     │
         clear record if fully done
4. Items 26 & 27 — Release and ReleaseExcess
4.1 Release(order_id, release_qty = all) (item 26)

Purpose. Free reservation that will not become a fill. Every terminal non-fill state must release with no leak: cancel → release; expire-at-close → release; IOC/market remainder → release; FOK reject → release; auction lapse → release; quantity-decrease modify → release only the freed portion.

Partial case. A quantity-decrease modify keeps the order resting (and its time priority) but frees the reduced portion, so release_qty defaults to all remaining but can be partial. (A price change or quantity increase is a cancel-and-replace: full Release of the old reservation, then a fresh ReserveIfAvailable — losing time priority.)

Key property. Release only ever touches what is still reserved — already-Converted (filled) portions are untouched. This is what makes a partially-filled-then-cancelled order correct: filled part converted, unfilled remainder released, no overlap.

Brokerage on partial-fill-then-cancel (locked 5.3 wiring, §5). If the order ever filled but brokerage was not yet charged (it terminated via cancel, not via RemainingQty == 0 in Convert), Release charges the per-executed-order cost once, guarded by BrokerageCharged.

go
// Release — frees reservation that will NOT fill. Partial release supported (qty-decrease modify).
func (s *ReservationService) Release(orderID OrderID, releaseQty int64 /* 0 = all */) error {
    rec := s.book.records[orderID]
    if rec == nil {
        panic("release: no reservation record for order (invariant violation)")
    }
    a := s.ledger.MustGet(rec.AccountID)

    switch rec.Side {
    case Buy:
        amount := rec.ReservedCash
        if releaseQty > 0 && releaseQty < rec.RemainingQty {
            amount = perQtyCashShare(rec, releaseQty) // proportional slice of ReservedCash
        }
        if err := a.MoveCash(Reserved, Available, amount); err != nil {
            panic(fmt.Sprintf("release buy: reserved underflow: %v", err))
        }
        rec.ReservedCash -= amount

    case Sell:
        units := rec.ReservedUnits
        if releaseQty > 0 && releaseQty < rec.RemainingQty {
            units = releaseQty
        }
        if err := a.MovePosition(rec.InstrumentID, Reserved, Available, units); err != nil {
            panic(fmt.Sprintf("release sell: reserved underflow: %v", err))
        }
        rec.ReservedUnits -= units
    }

    // ── Per-executed-order brokerage for the partial-fill-then-cancel case (locked, §5) ──
    if rec.EverFilled && !rec.BrokerageCharged {
        oc := s.costs.ComputeOrderCost(OrderCostInput{
            EverFilled: true, TotalQty: rec.OrigQty, Side: rec.Side,
        })
        if oc.Total > 0 {
            a.DebitCash(Available, oc.Total)
            s.events.Emit(ReleaseEvent{OrderID: orderID, AccountID: rec.AccountID, Kind: "release"})
        }
        rec.BrokerageCharged = true
    }

    // Reduce/clear the record.
    if releaseQty > 0 && releaseQty < rec.RemainingQty {
        rec.RemainingQty -= releaseQty // partial: order keeps resting
    } else {
        delete(s.book.records, orderID) // full: order terminal
    }

    s.events.Emit(ReleaseEvent{OrderID: orderID, AccountID: rec.AccountID, Kind: "release"})
    s.persist.MarkSnapshotDirty(rec.SessionID, rec.AccountID) // async
    return nil
}

5.1 hands used. MoveCash(Reserved, Available, amount) or MovePosition(instrument, Reserved, Available, units); DebitCash(Available, oc.Total) for the partial-cancel brokerage.

4.2 ReleaseExcess(order_id) (item 27)

Purpose. The market-buy true-up on completion: after a market buy is fully filled, the reservation may still hold a positive residue — the unused slippage buffer plus savings from fills cheaper than best_ask. That residue must be returned so nothing leaks.

When the engine calls it. On completion of a market buy (after all Converts). A resting limit order needs no ReleaseExcess — it trues to zero via Convert (fills at its own price). This method is specifically the market-order buffer return.

No brokerage block here. A fully-filled market buy already charged brokerage in Convert when RemainingQty hit 0. An optional BrokerageCharged == true assertion is a reasonable invariant check.

go
// ReleaseExcess — market-buy buffer true-up on COMPLETION. Order fully filled (remaining == 0) but
// reserved_cash may be > 0 (unused slippage buffer + better-than-estimate savings).
func (s *ReservationService) ReleaseExcess(orderID OrderID) error {
    rec := s.book.records[orderID]
    if rec == nil {
        // May already be gone if Convert cleared it at a clean zero — nothing to true up.
        return nil
    }
    if rec.RemainingQty != 0 {
        panic("release_excess: called on an incomplete order")
    }
    a := s.ledger.MustGet(rec.AccountID)

    residue := rec.ReservedCash
    if residue > 0 {
        if err := a.MoveCash(Reserved, Available, residue); err != nil {
            panic(fmt.Sprintf("release_excess: reserved underflow: %v", err))
        }
        rec.ReservedCash = 0
    }

    delete(s.book.records, orderID) // order fully done: nothing reserved, nothing leaked

    s.events.Emit(ReleaseEvent{OrderID: orderID, AccountID: rec.AccountID, Kind: "release"})
    s.persist.MarkSnapshotDirty(rec.SessionID, rec.AccountID) // async
    return nil
}

5.1 hands used. MoveCash(Reserved, Available, residue).

Auditable market-buy lifecycle. Kept explicit rather than folded into Convert, matching the engine's mental model: reserve → convert × N → release_excess → record empty.

Flow diagram.

        engine (market buy completed)
                     │
           ReleaseExcess(order_id)
                     │
              look up record
          (remaining_qty == 0,
           reserved_cash may be > 0)
                     │
          MoveCash reserved→available
          (entire residue: buffer +
           better-than-estimate savings)
                     │
              clear record
                     │
              emit release
              MarkSnapshotDirty
5. The cost wiring (items forced by the closed 5.3 design)

The brokerage rule from 5.3 is the one real change to the earlier 5.2 flows. Recorded here as the single source for how 5.2 charges costs:

Per-fill turnover cost — Convert calls ComputeFillCost per fill (side-aware, IsAutosquare: false for voluntary trades) and debits cost.Total via DebitCash after the principal move. Ordering locked (§3.3).
Per-executed-order brokerage — ComputeOrderCost is charged once when an order that filled reaches its terminal state:
the common path: inside Convert when RemainingQty == 0 (§3.4);
the partial-fill-then-cancel path: inside Release (§4.1);
guarded by BrokerageCharged on the record so it fires exactly once regardless of which method sees the terminal state;
EverFilled (set true on any Convert) is the "≥1 fill happened" signal; a never-filled order goes through Release with EverFilled == false and is charged zero.
Order-level, not per-trade — brokerage hits the account (via DebitCash) at termination but is not attributed into any trade's trade_costs row or net_pnl (5.4 excludes it), matching how a real broker presents it. Its persistence keying relative to the trade record is a Tier-2 write-path detail (deferred).
6. Item 28 — the reservation record maintenance (recap)

The record is written in ReserveIfAvailable, shrunk per fill in Convert, and cleared in Convert/Release/ReleaseExcess. Physical home: the 5.2-held book (Option 2), separate from Account, keyed by order_id, each record carrying AccountID/SessionID. The book is whole-economy scope; snapshotting filters by the in-record keys. This is 5.2's core state — the memory linking an admission to its later lifecycle.

7. Item 30 — the 5.4 realization handoff (recap)

On a sell Convert, 5.2 notifies 5.4 with {AccountID, SessionID, InstrumentID, FillQty, MakerPrice, MatchedLots, Cost, PositionNowFlat} so 5.4 runs FIFO-matched realized P&L, folds into its round-trip accumulator, and (on a position close) writes the closed-trade record. 5.2 moves the money; 5.4 computes the P&L narrative and owns the trade-record write. This is the one seam out of 5.2 besides the engine and 5.1.

8. Stops — no 5.2 involvement while dormant (recap)
Dormant/armed stops reserve nothing (no funds, no position) — they skip Gate 4, so touch neither 5.2 nor 5.1.
A fired stop converts into a normal order and re-enters the full admission gate, including Gate 4 — indistinguishable from any other order through the four methods. 5.2 has no stop-specific logic.
A fired stop that fails Gate 4 (e.g. position already sold) → the engine routes a private, participant-scoped rejection event; 5.2's reason return feeds it, nothing extra to build.
9. Item 29 — money events (wired, system deferred)

reserve / convert / release are emitted at every call site above via s.events.Emit(...). The event-system design is deferred — what an event physically is, where it goes (stream/consumers), whether synchronous or buffered, and how it relates to the snapshot/persistence path are a separate pass. The call sites are locked; the sink internals are a clean seam (EventSink) so nothing depends on them yet.

10. Cross-cutting properties, open & deferred
10.1 Cross-cutting
5.2 decides; 5.1 writes. No method mutates a balance directly.
In-process calls, race-free by construction (single goroutine per session; single-active-session rule).
The core invariant is the through-line of all four methods: reserved == cost/units of exactly the unfilled qty, always.
No store I/O on the hot path — methods mutate in-memory (Account via hands, ReservationBook directly) and call MarkSnapshotDirty (async); no synchronous Redis/Postgres write anywhere in 5.2.
Failure never partially mutates — reserve returns a typed reason before any move on the failure path; hand-guard failures after a positive check are programming errors (panic).
10.2 Open / deferred (carried, not resolved here)
Item 32 — per-archetype Gate-4 rule (budget-constrained vs pass-through agents, e.g. a market-maker meant to always provide liquidity). Structure is identical for all participants; only what the check does varies. Deferred to Layer-2 agent design; ReserveIfAvailable treats every participant identically until then.
Item 29 — the money-event system — call sites wired; system design deferred (§9).
PositionNowFlat / PositionFlat seam — Convert reads the post-RemovePosition position state to tell 5.4 whether the fill closed the position. Small wiring dependency on Tier-1.
Reservation records in the crash-recovery snapshot — the book's records are live session state (l5:snap:...:resv:{order_id}) mirrored by the async snapshotter; the cross-layer capture coordination is the deferred joint L5+L2+engine pass.
Order-level brokerage persistence keying — how the per-executed-order brokerage is keyed and persisted relative to the trade record is a Tier-2 write-path detail, deferred.