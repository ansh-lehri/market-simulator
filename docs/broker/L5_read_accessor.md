# L5 Prerequisite — `ReservationService` read accessor (`ReservationView`)

**Status:** confirmed required before **Phase 5** (order core). Also unblocks **Phase 6** (open-orders
read). Small, low-risk, pure-read L5 addition — **not** a broker change.

## Why (confirmed by source + doc re-run)
Phase 5's settlement adapter, on an `OutcomeMakerFill` (a resting order of ours gets hit), must decide
**`Filled` vs `PartiallyFilled`** — i.e. "is this order's remaining qty now 0?". The broker's
`OrderTrack` is deliberately thin (no qty — two-record rule), so it must **read** the remaining from L5.

No other channel carries it (re-run result):
- **`economy/reserve` events** (`ReserveEvent`/`ConvertEvent`/`ReleaseEvent`) carry cash/units deltas
  only — no `RemainingQty`, no `OrigQty`.
- **The exchange `OutcomeMakerFill` `EngineResult`** carries `Match.Trades` (the fill qty/price) +
  `LastPrice`; **`Match.Rested` is `nil`** on a maker fill ("not rested-anew" —
  `Exchange_Broker_Contract` §8). `Rested` is the only remaining-bearing field and is populated only
  when a remainder is first rested, never on subsequent maker fills. So the result gives *how much
  filled this time*, never *how much is left*.
- **Public tape** is anonymized — can't attribute fills.

The broker's only alternatives are to mirror filled-vs-original qty on `OrderTrack` (the exact qty
duplication the two-record rule forbids) or read from L5. The design reads from L5 — and L5 currently
has **no read accessor**.

## Current state (verified against the latest `economy` package)
`*ReservationService` exports exactly: `ReserveIfAvailable`, `Convert`, `Release`, `ReleaseExcess`,
`SetLogger`. All writes. The `RemainingQty` value lives on `ReservationRecord` (`book.go`), reachable
only via the **unexported** `book.get(orderID)` — inaccessible outside the `reserve` package. So the
Phase-5 doc's `s.resv.RemainingQty(OrderID)` does not compile against this code.

## The fix — one exported view accessor (serves Phase 5 + Phase 6)
Add to `economy/reserve` (e.g. `service.go`). Return a **value copy** (no pointer into the book —
message-boundary discipline), `bool=false` when the order id is unknown.

```go
// ReservationView — read-only snapshot of a reservation record, for the broker's
// settlement completion check (Phase 5) and open-orders read (Phase 6). Called on the
// owning shard goroutine (same single-writer discipline as the write methods) — no lock.
type ReservationView struct {
    OrderID      OrderID
    Side         contract.Side
    IsMarket     bool
    ReservedCash int64
    ReservedUnits int64
    RemainingQty int64
    OrigQty      int64
    EverFilled   bool
}

func (service *ReservationService) ViewReservation(orderID OrderID) (ReservationView, bool) {
    r := service.book.get(orderID)
    if r == nil {
        return ReservationView{}, false
    }
    return ReservationView{
        OrderID: r.OrderID, Side: r.Side, IsMarket: r.IsMarket,
        ReservedCash: r.ReservedCash, ReservedUnits: r.ReservedUnits,
        RemainingQty: r.RemainingQty, OrigQty: r.OrigQty, EverFilled: r.EverFilled,
    }, true
}
```
(Field set matches the economy note's A11 list: `Side`, `ReservedCash`, `ReservedUnits`,
`RemainingQty`, `OrigQty`. Trim/extend to the real `ReservationRecord` fields.)

## Consumers
- **Phase 5** `settleMakerFill`: `v, ok := s.resv.ViewReservation(oid); if ok && v.RemainingQty == 0 →
  Filled`. (Cross-doc note: the Phase-5 doc writes `s.resv.RemainingQty(oid)` — update that call site to
  the view accessor.)
- **Phase 6** open-orders read (A11): reads `Side`/`ReservedCash`/`ReservedUnits`/`RemainingQty`/
  `OrigQty` off the same view. **One accessor, both phases** — do not add a one-off `RemainingQty`
  method.

## Properties
- **Pure read / projection** — no money math, no mutation → **no auditor** needed.
- **Single-writer safe** — called on the owning shard goroutine, same as the write methods; no lock.
- **Placement:** `economy/reserve` (an L5 stage), done **before** the Phase-5 implementer session.
  Trivial to test: view reflects state after reserve/convert/release; unknown id → `false`.