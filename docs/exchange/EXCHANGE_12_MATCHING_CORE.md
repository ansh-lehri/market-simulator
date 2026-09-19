Exchange 1.2 Continuous Matching Core — Implementation

Prerequisites: EXCHANGE_00_ARCHITECTURE.md (split, seam, invariants) and EXCHANGE_11_ORDERBOOK_QUOTE.md (the Book, PriceLevel, QuoteState, FundedOrder, value types). This doc uses those types directly and does not redefine them.

What 1.2 owns: taking an admitted order and matching it against the resting book — deciding every fill, every rest, every cancel — by calling the Book's mutators (1.1). It is the sole component that writes last price, and the one place price and slippage emerge. It decides; the Book executes. The matcher never touches a SideBook, a queue, or the index directly.

1. Additional value types
go
// STP policy, carried per-order so the broker/agent can select it.
// Default is CancelIncoming (real-exchange standard).
type STPMode int
const (
    CancelIncoming STPMode = iota // cancel the aggressor on self-cross (default)
    CancelResting                 // pull the resting own-order, continue the walk
    CancelBoth                    // cancel both own orders
)

// Reason a quantity did not fill (attached to the cancelled remainder).
type CancelReason int
const (
    ReasonNone CancelReason = iota
    ReasonNoLiquidity   // opposite book emptied mid-walk
    ReasonFundedCap     // market buy hit its MaxSpend envelope
    ReasonCircuitClamp  // walk reached the circuit band
    ReasonIOCLeftover   // IOC order's unfilled remainder
    ReasonSelfMatch     // STP cancelled the incoming (CancelIncoming/CancelBoth)
    ReasonAuctionUnfilled // auction order that did not fill at the clearing price (1.3)
)

STPMode is added to FundedOrder (extend the struct from the 1.1 doc):

go
type FundedOrder struct {
    // ... all fields from EXCHANGE_11 ...
    STP STPMode // self-match policy; zero value = CancelIncoming
}
2. The trade — what one fill print produces

A single match between taker and maker prints one trade, which fans out into one public print and two private fills (one per side). The matcher emits this neutral record; the Event Emitter (1.9) shapes it into wire events.

go
type Trade struct {
    Seq        Seq           // shared sequence stamp (from the emitter's sequence space)
    Instrument InstrumentID
    Price      Ticks         // the MAKER's resting price — always
    Qty        Qty

    TakerOrder OrderID
    MakerOrder OrderID
    TakerSide  Side          // side of the incoming order
    // MakerSide is the opposite of TakerSide by construction
    TakerParticipant ParticipantID
    MakerParticipant ParticipantID
}

Why Price is the maker's price, always. When taker T crosses maker M, the trade prints at M's resting price, never T's. Consequences, all of which the sim depends on: price-improvement accrues to the taker (a buy limit at 10025 that hits a resting ask at 10023 pays 10023, not 10025); the resting order gets exactly what it posted; and last price = the maker's price on each print — which is why price is emergent (a by-product of which resting orders got hit) rather than set by the taker.

3. The MatchResult — the matcher's whole output for one order

A single incoming order produces exactly one MatchResult. It is a value the goroutine acts on; no pointers cross the seam.

go
type MatchResult struct {
    Trades          []Trade       // every print, in walk order (may be empty)
    Rested          *RestingOrder // non-nil iff a GTC-limit remainder was placed in the book
    CancelledQty    Qty           // remainder that did not fill and did not rest (0 if none)
    CancelReason    CancelReason  // why CancelledQty was cancelled
    LastPrice       Ticks         // final last-price after this order (valid iff any Trades)
    LastPriceMoved  bool          // true iff at least one trade printed (last price changed)
}

Interpreting the combinations:

Limit, fully filled: Trades non-empty, Rested == nil, CancelledQty == 0.
Limit, partial then rests: Trades (maybe empty), Rested != nil for the remainder, CancelledQty == 0.
Market/IOC, partial then done: Trades, Rested == nil, CancelledQty > 0 with a reason.
Rejected by self-match (CancelIncoming): Trades empty (or partial up to the self-cross), CancelledQty = the unfilled part, CancelReason = ReasonSelfMatch.
4. The matcher and its collaborators

The matcher depends on three collaborators through interfaces. The Book is concrete (1.1, done). The circuit clamp (1.5) and the sequence source (part of 1.9) are defined here as interfaces and implemented when those components are built — same discipline as SideBook.

go
// Circuit clamp seam (implemented by 1.5). The matcher asks, before printing at a
// price, whether that price is within the session's circuit band. This is the
// "clamp-during-matching" half of the circuit rule; Gate 3 is the "reject-at-
// admission" half.
type CircuitClamp interface {
    PriceAllowed(p Ticks) bool
}

// Sequence source (implemented by 1.9's emitter). One monotonic space shared by
// public and private events, so trades and quote changes interleave deterministically.
type SeqSource interface {
    Next() Seq
}

type Matcher struct {
    book  *Book         // 1.1 — the hands; matcher calls its mutators, never its internals
    clamp CircuitClamp  // 1.5 — consulted per price level during the walk
    seq   SeqSource     // 1.9 — stamps each trade
}

func NewMatcher(book *Book, clamp CircuitClamp, seq SeqSource) *Matcher {
    return &Matcher{book: book, clamp: clamp, seq: seq}
}

The matcher holds no money collaborator and no participant/account reference of any kind — confirming it makes zero L5 calls (architecture §1.4).

5. The entry point

The admission gate (1.4), after admitting an order, hands it to:

go
// Match runs the continuous-matching walk for one admitted order and returns the
// full result. It mutates the book's structure only through Insert, Cancel, and
// ReduceFront; and writes last price only through Book.SetLastPrice (last price
// is the matcher's output, not a structural change — see EXCHANGE_11 §7.2).
// Precondition: the order is admitted (gates 0–3 passed) and its Seq is stamped.
func (m *Matcher) Match(o *FundedOrder) MatchResult

Auction-window orders never reach here — Gate 1 diverts them to the auction batch (1.3). Match is the continuous path only.

6. The walk — control flow

The taker is o; makers come from the opposite side. "Acceptable price" and the opposite side are chosen by o.Side:

Taker Buy → walks the ask side, best = lowest; a maker ask at p is acceptable iff (market order) always, or (limit) p <= o.LimitPrice.
Taker Sell → walks the bid side, best = highest; a maker bid at p is acceptable iff (market) always, or (limit) p >= o.LimitPrice.

Pseudocode of Match (single-goroutine, no locks):

remaining := o.Qty
spent     := Ticks(0)          // for the funded-cap on market buys
trades    := []Trade{}
oppside   := opposite side book of o.Side

for remaining > 0:
    level, ok := oppside.Best()
    if !ok:
        break                   // opposite book empty -> stop (no-liquidity)

    // (a) price acceptable vs the taker's limit?
    if o.Type == Limit and not priceAcceptable(level.Price, o):
        break                   // best price no longer crosses -> stop; remainder may rest

    // (b) circuit clamp: may a trade print at this price?
    if !m.clamp.PriceAllowed(level.Price):
        cancelReason = ReasonCircuitClamp
        break                   // walk reached the band -> halt

    front := level.Orders.Front().Value.(*RestingOrder)

    // (c) self-match check
    if front.Participant == o.Participant:
        switch o.STP:
          case CancelIncoming:  cancelReason = ReasonSelfMatch; goto stop_incoming
          case CancelResting:   m.book.Cancel(front.OrderID); continue   // pull maker, re-loop
          case CancelBoth:      m.book.Cancel(front.OrderID); cancelReason = ReasonSelfMatch; goto stop_incoming

    // (d) how much can we take from this maker?
    fillQty := min(remaining, front.Remaining)

    // (e) funded-cap (market BUY only): can we afford fillQty at this price?
    if o.Type == Market and o.Side == Buy:
        affordableQty := (o.MaxSpend - spent) / level.Price   // integer floor
        if affordableQty < fillQty:
            fillQty = affordableQty
        if fillQty == 0:
            cancelReason = ReasonFundedCap
            break                                              // can't afford even 1 unit

    // (f) print the trade at the MAKER's price
    t := Trade{ Seq: m.seq.Next(), Instrument: o.Instrument,
                Price: level.Price, Qty: fillQty,
                TakerOrder: o.OrderID, MakerOrder: front.OrderID,
                TakerSide: o.Side,
                TakerParticipant: o.Participant, MakerParticipant: front.Participant }
    trades = append(trades, t)

    // (g) execute the fill on the book (1.1 does the surgery)
    m.book.ReduceFront(level, fillQty)     // dec Remaining+TotalQty, drop node/level, refresh quote
    remaining -= fillQty
    spent     += fillQty * level.Price

    // (h) market funded-cap exhausted exactly?
    if o.Type == Market and o.Side == Buy and spent >= o.MaxSpend and remaining > 0:
        cancelReason = ReasonFundedCap
        break

stop_incoming:
// walk ended. write last price and decide the remainder.
result := assembleResult(o, trades, remaining, cancelReason)
return result

priceAcceptable(p, o): o.Side==Buy → p <= o.LimitPrice; o.Side==Sell → p >= o.LimitPrice.

7. Ending the walk — last price and the remainder

assembleResult finalizes three things:

Last price. If any trade printed, last price = the price of the final trade in trades. The matcher writes it into the quote via `m.book.SetLastPrice(price)` (EXCHANGE_11 §6, §7.2 — the only path that mutates LastPrice/HasLastPrice) and sets result.LastPriceMoved = true. If no trade printed, SetLastPrice is not called and last price is untouched.

The remainder (remaining units still unfilled) is disposed by order type:

Order type	Remainder disposition
Limit (GTC)	Rests: build a RestingOrder{OrderID, Participant, Side, Price: o.LimitPrice, Remaining: remaining, Seq: o.Seq} and call m.book.Insert(...). Set result.Rested.
Market	Cancelled: CancelledQty = remaining, reason = the reason set during the walk (ReasonNoLiquidity if the book emptied, ReasonFundedCap if capped, ReasonCircuitClamp if clamped). Never rests.
IOC	Cancelled: CancelledQty = remaining, reason = ReasonIOCLeftover (or the walk's clamp/self-match reason if that ended it). Never rests.

Self-match under CancelIncoming/CancelBoth: the walk broke to stop_incoming with cancelReason = ReasonSelfMatch; whatever filled before the self-cross stands as trades, and the unfilled remaining is cancelled with that reason (a GTC limit does not rest in this case — the incoming order was cancelled, not parked).

8. The three emergent behaviours (no code computes them)
Slippage is the arithmetic of steps (f)–(g) repeating across levels: a large taker prints 40@10023, then (if more levels existed) the next slice at a worse price, so its average fill drifts from the touch. Nothing computes an average; it is just the sequence of maker prices consumed.
Price is level.Price written to last-price in §7 — the price of whichever resting order was hit, never anything the taker named.
The funded-cap is steps (e)/(h): the walk stops spending when spent would exceed the broker-attached MaxSpend, using integer-floor affordability. A tiny residual of unspendable cash can remain (you cannot buy a fraction of a unit); that is correct.
9. Who mutates what — the decide/execute split (restated as contract)
The matcher reads the book only via Best() and level inspection, and mutates its structure only via ReduceFront, Insert, Cancel — the 1.1 bid/ask mutators. It never splices a queue node, never edits TotalQty, never touches the index.
The matcher writes last price via `Book.SetLastPrice` — the sole path that mutates LastPrice/HasLastPrice — because last price is its output (the price of the trade it decided), not a structural mutation of the book. The bid/ask mutators never touch that half of the quote.
All book-invariant maintenance (TotalQty, empty-level removal, index upkeep, bid/ask refresh) happens inside the three bid/ask mutators — one place, three methods — so the matcher cannot corrupt the book by forgetting an invariant.

This is why the standalone cancel path (a participant pulling a resting order with no matching) and the matcher's fill path both funnel through the same book.Cancel / book.ReduceFront: mutation logic lives once, in the hands.

10. Collaborator interfaces defined here, implemented later
CircuitClamp.PriceAllowed(Ticks) bool — implemented by 1.5. Until 1.5 exists, a stub returning always-true lets 1.2 be built and tested (the clamp simply never halts the walk).
SeqSource.Next() Seq — implemented by 1.9. A trivial monotonic counter stub serves for testing 1.2 in isolation.

Both are constructor-injected, so 1.2 compiles and unit-tests against stubs before 1.5/1.9 are written.

11. Deferred (documented, not built now)
FOK (fill-or-kill): all-or-nothing. Requires a look-ahead pass over the opposite side (sum acceptable-priced liquidity up to the limit) before mutating anything; if it cannot fully fill, reject with zero trades. Deferred for the prototype — no agent strategy issues FOK yet. The OrdType slot and a matchFOK entry point are reserved; when built, it runs the look-ahead, then either replays the walk to completion or returns an all-cancelled result.
12. Invariants 1.2 must hold
Every trade prints at the maker's price. Never the taker's.
Time priority within a level is respected — always consume Orders.Front() first (lowest Seq).
No negative fills, no over-fill — fillQty = min(remaining, front.Remaining), and (market buy) further capped by affordability.
A self-match never prints a trade between the same participant's orders, under any STP mode.
Market and IOC never rest; only GTC limit remainders rest.
Last price moves only on an actual print, and equals the final trade's maker price.
The matcher makes zero calls across the money seam — it holds no account reference; the only external consult is the in-engine CircuitClamp.