Exchange 1.3 Auction Matcher — Implementation

Prerequisites: EXCHANGE_00_ARCHITECTURE.md (per-instrument sharding), EXCHANGE_11_ORDERBOOK_QUOTE.md (FundedOrder, value types), EXCHANGE_12_MATCHING_CORE.md (Trade, CancelReason, SeqSource), EXCHANGE_14_ADMISSION_GATE.md (AuctionBatch interface — 1.3 implements it), EXCHANGE_18_SESSION_PHASE_CLOCK.md (the close transition that triggers the clear).

What 1.3 owns: the closing auction for one instrument. During the closing-auction window the gate diverts orders here (Add) instead of matching them. At session close the whole collected batch clears at one uniform price — the price that maximizes executable volume, with a four-level tie-break cascade. It is the second of the exchange's two matchers; unlike the continuous core (1.2), nothing matches until the single clear.

0. Why an auction, and how it differs from continuous matching

The closing price is the day's most important price (what positions mark against). Rather than let it be whatever prints last in continuous trading, the auction aggregates all closing interest and computes one fair clearing price for everyone. Concretely:

	Continuous (1.2)	Auction (1.3)
When it matches	immediately, per incoming order	once, at session close
Price(s)	many — each trade at its maker's price	one uniform clearing price for all fills
Priority	price-time, taker walks makers	clearing-price search, then price-time allocation
Trigger	order arrival	the clock's Continuous→Closed transition

Everything that executes in the auction executes at the same clearing price P*, regardless of each order's own limit.

0.1 Scope decisions (locked)
Full four-level tie-break cascade (§3) — max volume → min surplus → pressure side → reference proximity.
Heavy-side allocation is price-then-time priority (§4) — consistent with the continuous book's priority, so agent behaviour is coherent across both phases.
Unfilled auction orders are cancelled at clear (§5) — the closing auction is terminal; nothing rests into a closed market.
No indicative price during collection (deferred) — orders are collected silently; P* is computed once, at clear. Indicative broadcasting can be added later without changing the clear.
Per-instrument, globally triggered — each instrument has its own AuctionBook; the one global close transition fans out to clear every instrument's auction (§6).
1. The auction book
go
// AuctionBook collects orders during the closing-auction window and clears them at
// session close. Per-instrument: lives in InstrumentState, owned by that instrument's
// shard worker, mutated only by it (no lock).
type AuctionBook struct {
    buys  []*FundedOrder // collected BUY orders (Limit and Market)
    sells []*FundedOrder // collected SELL orders
}

Orders keep their Seq (stamped at admission by stampSequence before being diverted), which is used for time priority in allocation (§4).

2. Collection — the AuctionBatch interface
go
// Add implements the AuctionBatch interface the gate calls on a diverted order
// (checkSessionPhase set route=routeAuction, routeOrder called auction.Add). Appends
// the order to the correct side. O(1). No matching happens here.
func (a *AuctionBook) Add(o *FundedOrder) {
    if o.Side == Buy {
        a.buys = append(a.buys, o)
    } else {
        a.sells = append(a.sells, o)
    }
}

An order reaching Add has already passed the full admission pipeline (structural, phase, tick, band — only liquidity was skipped for the auction route). So the batch contains only well-formed, funded, on-tick, in-band orders.

3. Finding the clearing price — the four-level cascade

For any candidate price P:

Demand(P) = total qty of buys willing to pay ≥ P (a buy limit ≥ P is eligible; market buys are always eligible).
Supply(P) = total qty of sells willing to accept ≤ P (a sell limit ≤ P is eligible; market sells are always eligible).
Executable(P) = min(Demand(P), Supply(P)) — how much can actually trade at P.

The candidate prices where the curve can change are exactly the distinct limit prices present in the batch (plus, conceptually, the reference for the market-only case). P* is chosen by applying, in order, until one price remains:

findClearingPrice(reference):
    if no crossing possible (max Executable over all candidates == 0):
        return ok=false                         // nothing trades (e.g. all buys below all sells)

    candidates := sorted distinct limit prices in the batch

    // Level 1 — maximum executable volume
    for each P in candidates: compute Executable(P)
    best := prices achieving the maximum Executable
    if len(best) == 1: return best[0]

    // Level 2 — minimum surplus (imbalance) among the level-1 winners
    // surplus(P) = |Demand(P) - Supply(P)| at the executable volume; pick the smallest
    best := prices in best with minimum surplus
    if len(best) == 1: return best[0]

    // Level 3 — market-pressure side among the level-2 winners
    // if net pressure is on the BUY side (Demand > Supply at these prices) -> pick the
    // HIGHEST price (buyers push up); if on the SELL side -> pick the LOWEST price.
    // (equal pressure -> fall through to level 4)
    best := narrow by pressure side
    if len(best) == 1: return best[0]

    // Level 4 — reference-price proximity: pick the price closest to `reference`
    // (the last traded / prior reference price). Deterministic final tie-break.
    return price in best minimizing |P - reference|

reference is the instrument's last-traded price (or the session reference if no trade occurred), supplied at clear time. Level 4 is guaranteed to resolve to a single price (if two are equidistant, pick the lower by convention — a fixed rule so clearing is fully deterministic).

This is the standard exchange cascade (NSE/NYSE/Nasdaq variants). Each level only breaks ties the previous level left; most real batches resolve at level 1 or 2.

4. Executing the clear — allocation at P*

Once P* is known:

Eligible buys: limit ≥ P* (and all market buys). Eligible sells: limit ≤ P* (and all market sells).
Executable total = min(eligible demand, eligible supply). One side is the scarce side (fills fully); the other is the heavy side (fills partially — a surplus remains).

Heavy-side allocation is price-then-time priority (decision), identical to the continuous book's ordering:

Sort the heavy side by price aggressiveness first: buys by limit descending (most willing to pay first), sells by limit ascending (most willing to accept first); market orders rank most-aggressive. Ties broken by Seq ascending (earliest arrival first).
Fill down that order until the executable total is exhausted. The order straddling the boundary may fill partially; everything past it does not fill.

The scarce side all fills (by construction it's fully covered). Every fill — scarce and heavy — prints at P*.

clearAllocate(P*, executableTotal, seq):
    trades := []
    scarce, heavy := the two sides, scarce fully filled
    // pair scarce-side fills against heavy-side fills in priority order,
    // each Trade at price P*, qty per the min of the two orders' remaining,
    // Seq stamped from seq.Next(), taker/maker semantics recorded (see §4.1)
    ...
    return trades
4.1 Taker/maker in an auction

An auction has no taker/maker in the continuous sense — all orders are equal participants clearing together. For the Trade record (which carries TakerOrder / MakerOrder), the auction fills pair a buy with a sell; by convention the buy is recorded as taker, the sell as maker (arbitrary but consistent), both Participants populated, Price = P*. The broker treats an auction fill exactly like a continuous one — it only needs the two order ids, side, qty, and price to run Convert. (Self-match prevention: a buy and sell from the same participant should not be paired; the allocation skips such a pairing and moves to the next counterparty, mirroring 1.2's STP intent.)

5. Unfilled orders

Every order that does not fully fill at clear — the entire heavy-side surplus, plus any order whose limit was outside P* (a buy limit < P*, a sell limit > P*) — is cancelled. The closing auction is terminal; nothing rests into a closed market. Each cancellation is a private terminal event carrying the echoed OrderID so the broker releases the reservation.

go
type Cancelled struct {
    OrderID OrderID
    Qty     Qty          // the unfilled quantity
    Reason  CancelReason // ReasonAuctionUnfilled (add to the CancelReason enum)
}

Add to the CancelReason enum (1.2): ReasonAuctionUnfilled.

6. The result and the trigger
go
type AuctionResult struct {
    ClearingPrice Ticks
    HasClear      bool        // false if nothing could match (no crossing orders)
    Trades        []Trade     // all at ClearingPrice
    Cancelled     []Cancelled // every unfilled order, for the broker to release
}

// Clear computes P* and matches the whole batch. Called ONCE, at the clock's
// Continuous->Closed transition. reference = the instrument's last-traded/session
// reference price; seq = this instrument's sequence source.
func (a *AuctionBook) Clear(reference Ticks, seq SeqSource) AuctionResult

Trigger and fan-out. The auction is triggered by the global session clock's Continuous→Closed transition (1.8). Because the clock is global (all instruments close together) but each AuctionBook is per-instrument, the exchange wiring fans the close out to every instrument's shard, and each instrument's shard calls Clear on its own AuctionBook. Each instrument runs its own closing auction independently, all triggered by the one close. (This fan-out is detailed in the wiring doc.)

On HasClear == false (no price crosses — e.g. every buy below every sell): no trades; all collected orders are cancelled (ReasonAuctionUnfilled) and released. There is simply no closing print for that instrument.

Output routing. The Trades flow out like continuous trades — public prints on the tape, private fills to the broker (all at P*). Each fill updates the instrument's last price to P* (the official close). Cancelled become private terminal events. The Event Emitter (1.9) shapes all of it.

7. Ownership & concurrency
One AuctionBook per instrument, in InstrumentState, on the instrument's shard. Mutated only by that shard worker — Add during the window, Clear at close — both on the same single-threaded shard loop, so no lock.
Because Add and Clear run on the same shard as the instrument's continuous matcher, there is no race between "collecting the auction" and "the last continuous trades" — the phase transition is processed in the shard's serial order.
8. Invariants 1.3 must hold
All auction fills print at the single clearing price P* — never at individual limits.
P* maximizes executable volume, with ties broken by the fixed four-level cascade to a single deterministic price.
Heavy-side allocation is price-then-time — same priority rule as the continuous book.
The scarce side fills fully; the heavy side fills to the executable total; nothing over-fills.
Every unfilled order is cancelled and released (echoed OrderID) — nothing rests past a closing auction.
No self-match — a same-participant buy/sell pairing is skipped in allocation.
Clear runs exactly once per instrument per session, at the close transition.
9. Deferred / handed elsewhere
Indicative clearing price during the collection window — deferred; would recompute P* on each Add and emit it, without changing the final clear.
Opening auction — PhaseOpeningAuction is reserved (1.8) but not used in the prototype; the same AuctionBook mechanism would serve it, triggered by a Closed→OpeningAuction→Continuous transition instead.
The close fan-out wiring — detailed in the dedicated sharding/wiring doc; 1.3 only exposes Clear.
Event shaping for auction trades/cancels — 1.9.