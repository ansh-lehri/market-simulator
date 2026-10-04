**EXCHANGE ⇄ BROKER**
 
The Interface Contract
 
What the exchange accepts from the broker, what it returns, and how every *order, cancel, partial fill, auction divert, and resting-order (maker) fill* is reported back — written for whoever is building the broker.
 
| **How to read this document** This is the **settled contract**. Everything below is written as what the exchange **accepts**, **returns**, or **does** — you can build the broker against it as a fixed interface. A few behaviours (the auction-fill outcome shape, and the maker-side fill for resting orders) are being finalised on the exchange side. Where that is the case, the contract states the guarantee the exchange *will honour*, and a short note marks that the plumbing is being completed in the background. You do not need to design around the gap — design against the guarantee. |
| --- |
 
# **Contents**
 
# **1.  The mental model in one screen**
 
The broker and the exchange live in **one process** and talk over **Go channels**. The connection is deliberately narrow: **one channel in, one path out**. Everything — every instrument, every order type — is multiplexed over that single edge and self-described by tags on each message.
 
| **The three channels of the edge** **Inbound** (broker → exchange): the broker sends InboundMessage — either a SubmitOrder or a CancelOrder. **Private out** (exchange → broker): the broker receives EngineResult — the per-order outcome, stamped with the broker's own id and echoing the order id. This is what the broker settles against. **Public out** (exchange → tape): the anonymized market-data stream — trade prints, quote/depth snapshots, OHLCV candles. Broadcast, carries *no* identity. The broker relays this to users; it is *not* how the broker learns about its own fills. |
| --- |
 
| **Two standing rules you can rely on** **Rule 1 — ****One-in / one-out.** You never see per-instrument channels. You hold one inbound and one outbound path regardless of how many instruments or shards the exchange runs internally. The exchange can re-shard internally without your code changing. **Rule 2 — ****Every inbound message carries a **BrokerID**. **The exchange treats it as opaque and **echoes it back** on the corresponding private result, so your private events route back to you and never leak to another broker. One broker today; the field is present so multi-broker is additive. |
| --- |
 
One consequence to internalise up front: **an order****'****s private result is not always immediate, and not always singular over the order****'****s lifetime.** A submitted order gets one result at submission. But an order diverted to the closing auction resolves *later*; and a resting limit order fills *later still*, when a future taker hits it. Sections 6, 7 and 8 are entirely about these later events — they are the parts most easily missed.
 
# **2.  Value types — the integer discipline**
 
Before the structures: every quantity and price the exchange exchanges with you is an **integer**. This is a correctness requirement, not an optimization — a price-keyed book cannot use floats. The broker must speak the same integer language.
 
| **Type** | **Meaning** |
| --- | --- |
| **Ticks  (int64)** | A price, expressed as an integer count of ticks. ₹501.05 at a ₹0.05 tick = 10021. The rupee value is reconstructed only for display (ticks × tick_size); never send a float. |
| **Qty  (int64)** | A quantity in whole integer units. A live order's qty is always > 0. |
| **OrderID  (string)** | Broker-assigned, opaque to the exchange, echoed on every event. Your handle to reconcile. |
| **ParticipantID  (string)** | Opaque. Used ONLY for self-match prevention, never for money. |
| **InstrumentID  (string)** | Opaque instrument key, e.g. "ACME". The routing key on every message. |
| **BrokerID  (string)** | Opaque. Who sent the message; echoed on the private result. |
| **Seq  (uint64)** | Monotonic sequence. Two meanings by context: an order's admission Seq (time priority) and an event's Seq (tape ordering). |
 
# **3.  What the exchange ACCEPTS — input formats**
 
Everything the broker sends is one type — InboundMessage — a thin wrapper that lets the single inbound channel carry both “submit an order” and “cancel an order.”
 
## **3.1  The wrapper — InboundMessage**
 
type InboundKind int
 
const (
 
    SubmitOrder InboundKind = iota  // a new funded order to admit + match
 
    CancelOrder                     // pull a resting order by id
 
)
 
 
 
type InboundMessage struct {
 
    Kind       InboundKind    // discriminator: which variant is set
 
    BrokerID   BrokerID       // Rule 2: who sent it; echoed on the private result
 
    Instrument InstrumentID   // routing key — ingress hashes this to a shard
 
    Submit     *FundedOrder   // non-nil iff Kind == SubmitOrder
 
    Cancel     *CancelRequest // non-nil iff Kind == CancelOrder
 
}
 
| **Field** | **Type** | **Values** | **Meaning** |
| --- | --- | --- | --- |
| **Kind** | InboundKind | SubmitOrder │ CancelOrder | Which variant this is. Tells the shard whether to run the full admission pipeline (submit) or a direct book pull (cancel). |
| **BrokerID** | BrokerID | any non-empty id | Who sent it. Present on EVERY message (Rule 2). Opaque to the exchange; stored only to echo on the private result. |
| **Instrument** | InstrumentID | e.g. "ACME" | The instrument this message concerns. This is the ROUTING KEY — ingress hashes it to pick the owning shard. Required on both submit AND cancel (a cancel must reach the shard that owns the order’s book). |
| **Submit** | *FundedOrder | non-nil for submit | The order itself. Fields in 3.2. |
| **Cancel** | *CancelRequest | non-nil for cancel | Just the order id to pull. Fields in 3.3. |
 
## **3.2  The submit payload — FundedOrder**
 
This is what the broker sends **after it has already reserved funds**. The exchange **trusts** it is funded — it carries no balance, no account reference, no money field except the pre-attached reservation cap (MaxSpend).
 
type Side    int;  const ( Buy Side = iota; Sell )
 
type OrdType int;  const ( Limit OrdType = iota; Market )   // stops are broker-side, never seen here
 
type TIF     int;  const ( GTC TIF = iota; IOC; FOK )       // Market/IOC/FOK never rest
 
type STPMode int;  const ( CancelIncoming STPMode = iota; CancelResting; CancelBoth )
 
 
 
type FundedOrder struct {
 
    OrderID     OrderID        // broker-assigned, opaque, echoed on all events
 
    Participant ParticipantID  // opaque; self-match prevention only
 
    Instrument  InstrumentID
 
    Side        Side
 
    Type        OrdType
 
    TIF         TIF
 
    Qty         Qty            // requested quantity, integer, > 0
 
    LimitPrice  Ticks          // set for Limit; ignored for Market
 
    MaxSpend    Ticks          // market-BUY reservation cap; 0 for sells/limits
 
    STP         STPMode        // self-match policy; zero value = CancelIncoming
 
}
 
| **Field** | **Type** | **Values** | **Meaning** |
| --- | --- | --- | --- |
| **OrderID** | OrderID | unique, broker-assigned | Your handle for the order. The exchange never interprets it, only ECHOES it on every event so you can match outcomes to the reservation you made. |
| **Participant** | ParticipantID | any id | Who owns the order. Used ONLY for self-match prevention — never for money. |
| **Instrument** | InstrumentID | e.g. "ACME" | Which instrument to trade. |
| **Side** | Side | Buy │ Sell | Buy or sell. |
| **Type** | OrdType | Limit │ Market | Order type. ONLY these two exist. Stop orders live on the broker; a triggered stop reaches the exchange as an ordinary Market/Limit. |
| **TIF** | TIF | GTC │ IOC │ FOK | Time-in-force. GTC-limit rests; Market/IOC/FOK never rest. (FOK is documented; treat as deferred.) |
| **Qty** | Qty | integer, > 0 | Quantity requested, whole units. |
| **LimitPrice** | Ticks | on the tick grid; set for Limit, 0 for Market | Limit price as an integer tick count. Off-grid or a price on a Market order is rejected as malformed. |
| **MaxSpend** | Ticks | market BUY: best_ask×qty + buffer; else 0 | The reservation envelope. The exchange’s funded-cap walk caps a market buy against THIS number, with no callback to the broker. This is how funding stays broker-side yet the cap is enforced inside the match. |
| **STP** | STPMode | CancelIncoming (default) │ CancelResting │ CancelBoth | What to do if the order would match the same Participant’s resting order. Zero value = cancel-incoming (the real-exchange standard). |
 
## **3.3  The cancel payload — CancelRequest**
 
type CancelRequest struct {
 
    OrderID OrderID  // the resting order to remove
 
}
 
| **Field** | **Type** | **Values** | **Meaning** |
| --- | --- | --- | --- |
| **OrderID** | OrderID | an existing resting order’s id | The resting order to pull from the book. No price, no funding — a pure book operation. (Cancel semantics: Section 5.) |
 
# **4.  What the exchange RETURNS — output formats**
 
Every private outcome the broker receives is one type — EngineResult. Outcomes are discriminated *inside* it (you switch on Outcome), so you always read one shape.
 
## **4.1  The private result — EngineResult**
 
type Outcome int
 
const (
 
    OutcomeMatched        Outcome = iota // went to the continuous matcher (has Match)
 
    OutcomeDiverted                      // collected into the closing auction (resolves later)
 
    OutcomeRejected                      // failed admission; never entered book or auction
 
    OutcomeCancelled                     // a cancel succeeded
 
    OutcomeCancelRejected                // a cancel found nothing to remove
 
    OutcomeMakerFill                     // a RESTING order of yours was hit by a taker (see §8)
 
)
 
 
 
type EngineResult struct {
 
    Outcome      Outcome
 
    OrderID      OrderID       // echoed on EVERY outcome — you reconcile / release on this
 
    BrokerID     BrokerID      // echoed so the event routes back to you
 
    Match        *MatchResult  // non-nil for OutcomeMatched and OutcomeMakerFill
 
    Rejection    *Rejection    // non-nil for OutcomeRejected
 
    CancelledQty Qty           // set for OutcomeCancelled (qty to release)
 
}
 
| **Field** | **Type** | **Values** | **Meaning** |
| --- | --- | --- | --- |
| **Outcome** | Outcome | the 6 values above | What happened. You switch on this. |
| **OrderID** | OrderID | the order’s id | Echoed on EVERY outcome, including diverted and rejected — so you can always tie the result to your reservation. |
| **BrokerID** | BrokerID | your id | Echoed so the private event routes to you. |
| **Match** | *MatchResult | non-nil when fills exist | The fills / rest / cancel detail. Present for matched (taker) results AND for maker fills of your resting orders (§8). |
| **Rejection** | *Rejection | non-nil on reject | Why admission failed (§4.3). |
| **CancelledQty** | Qty | the unfilled qty | On OutcomeCancelled — how much reservation to release. |
 
| **Outcome-by-outcome — what the broker does** OutcomeMatched  — read Match: Convert the fills, rest-or-release the remainder. One-shot (§7). OutcomeDiverted  — no fills yet. HOLD the reservation. A second result for the same OrderID arrives when the auction clears (§6). OutcomeRejected  — read Rejection.Reason. Release the whole reservation; nothing entered the book. OutcomeCancelled  — release CancelledQty. OutcomeCancelRejected  — no state change; nothing to do (the order was already gone). OutcomeMakerFill  — your resting order was hit. Convert the fill; the remaining resting qty stays working (§8). |
| --- |
 
## **4.2  When matched — MatchResult and Trade**
 
A single incoming order produces exactly **one** MatchResult carrying **all** its prints. (This one-shot guarantee is Section 7.)
 
type MatchResult struct {
 
    Trades         []Trade       // every print, in walk order (may be empty)
 
    Rested         *RestingOrder // non-nil iff a GTC-limit remainder was placed in the book
 
    CancelledQty   Qty           // remainder that did not fill and did not rest (0 if none)
 
    CancelReason   CancelReason  // why CancelledQty was cancelled
 
    LastPrice      Ticks         // last-price after this order (valid iff any Trades)
 
    LastPriceMoved bool          // true iff at least one trade printed
 
}
 
 
 
type Trade struct {
 
    Seq              Seq          // event sequence stamp
 
    Instrument       InstrumentID
 
    Price            Ticks        // the MAKER's resting price — always
 
    Qty              Qty
 
    TakerOrder       OrderID
 
    MakerOrder       OrderID
 
    TakerSide        Side         // side of the incoming order (maker side is the opposite)
 
    TakerParticipant ParticipantID
 
    MakerParticipant ParticipantID
 
}
 
| **Field** | **Type** | **Values** | **Meaning** |
| --- | --- | --- | --- |
| **Trades** | []Trade | walk order; may be empty | Every fill this order produced. Empty if it matched nothing (e.g. rested outright, or self-match-cancelled). |
| **Rested** | *RestingOrder | non-nil iff remainder rested | The GTC-limit remainder now working in the book. Keep this reservation held — it fills later as a maker (§8). |
| **CancelledQty** | Qty | 0 if none | Remainder that neither filled nor rested (market/IOC leftover, funded-cap, circuit clamp, self-match). |
| **CancelReason** | CancelReason | see enum below | Why that qty was cancelled — so you know why you’re releasing. |
| **LastPrice** | Ticks | valid iff any trade | Last-traded price after this order. |
| **LastPriceMoved** | bool | true/false | Whether a trade printed at all. |
 
Trade.Price is **always the maker’s resting price**, never the taker’s. A buy limit at 10025 that hits a resting ask at 10023 pays **10023**. Price-improvement accrues to the taker; the resting order gets exactly what it posted; last price = the maker price on each print. Price is emergent, not set by the taker.
 
| **CancelReason** | **Meaning** |
| --- | --- |
| **ReasonNone** | Nothing was cancelled. |
| **ReasonNoLiquidity** | The opposite book emptied mid-walk. |
| **ReasonFundedCap** | A market buy hit its MaxSpend envelope. |
| **ReasonCircuitClamp** | The walk reached the circuit band edge. |
| **ReasonIOCLeftover** | An IOC order’s unfilled remainder. |
| **ReasonSelfMatch** | STP cancelled the incoming (CancelIncoming / CancelBoth). |
| **ReasonAuctionUnfilled** | An auction order that did not fill at the clearing price (§6). |
 
## **4.3  When rejected — Rejection**
 
type Rejection struct {
 
    Reason RejectReason  // what the broker acts on
 
    Stage  string        // which stage produced it — diagnostic only
 
}
 
| **RejectReason  (Stage)** | **Meaning** |
| --- | --- |
| **RejectMalformed  (****"****structural****"****)** | Bad shape: qty ≤ 0, a Market order carrying a price, missing fields, etc. |
| **RejectMarketClosed  (****"****session_phase****"****)** | Order arrived while the session was closed / outside hours. |
| **RejectNoLiquidity  (****"****liquidity****"****)** | A market order against an empty opposite book. |
| **RejectBadTick  (****"****tick_grid****"****)** | Limit price off the tick grid. |
| **RejectBeyondCircuit  (****"****circuit_band****"****)** | Limit price beyond the circuit band. |
 
Act on Reason; Stage is human-readable traceability only, not logic-bearing. On any rejection: release the whole reservation.
 
# **5.  Cancellation — how the exchange handles an explicit cancel**
 
Yes — a broker sending an explicit cancel is a **first-class, fully-structured flow**, not an afterthought. Here is exactly what the exchange does with it.
 
## **5.1  What the exchange does — step by step**
 
- **Accepts it on the wire.** You send InboundMessage{Kind: CancelOrder, Cancel: &CancelRequest{OrderID}}, stamped with your BrokerID and the Instrument.
 
- **Routes it by instrument.** Ingress hashes Instrument to the owning shard — the same shard that holds the order’s book. (This is why Instrument is required on a cancel; routing is never by OrderID.)
 
- **Executes an O(1) book pull.** The shard calls book.Cancel(OrderID): locate the node via the order-index map, unlink it, decrement TotalQty, drop the emptied price level. No gate, no matcher — a cancel is a pure book operation and cannot be “rejected” for tick/band/phase reasons.
 
- **Returns a private result.** On success: EngineResult{OutcomeCancelled, OrderID, CancelledQty: remaining} — release that qty. If the order was already gone: OutcomeCancelRejected (a normal outcome, no state change).
 
- **Emits public market data.** A vanished resting bid/ask is real market data, so a quote/depth update goes out on the tape.
 
## **5.2  The edges you must design for**
 
| **Constraints the broker inherits** **Whole-order only.** CancelRequest is just an OrderID. You cannot cancel “40 of the 70 remaining.” To reduce size you cancel and resubmit. **Resting orders only.** Market/IOC/FOK never rest, so there is nothing to cancel by the time you could send one. A cancel meaningfully targets only a working GTC-limit; anything else → OutcomeCancelRejected. **Cancel-vs-fill race is resolved, not prevented.** A cancel and a taker that would hit the same order land on the same shard and are processed *serially in channel order*. A cancel can “lose” to a fill that arrived just before it — coming back as OutcomeCancelRejected (already consumed) or OutcomeCancelled with a *smaller* CancelledQty than you expected. This is why you release CancelledQty as echoed, never the qty you assumed. **Modify is not a primitive.** There is no in-place modify. A price or size-up change loses time priority, so it IS a new order: the broker orchestrates cancel + fresh submit itself. |
| --- |
 
# **6.  Doubt #1 — diverted orders: how auction fills come back**
 
| **The broker designer****'****s question** “OutcomeDiverted carries no payload. Its fills ‘arrive later when the auction clears.’ Do those later fills come back as a matched result echoing the *same OrderID*, so I settle them like continuous fills? And I must hold the reservation across the divert→clear gap — confirm.” |
| --- |
 
## **6.1  What the exchange does**
 
During the PhaseClosingAuction window, orders do not match immediately. The gate routes them into a per-instrument batch. Everything clears at **one uniform price **P* at session close. So a diverted order gets **two** private results, at two different times:
 
### **Time 1 — at submission (immediate)**
 
EngineResult{ Outcome: OutcomeDiverted, OrderID: "o-99", BrokerID: "B1" }
 
// no fills. ACK only: "accepted, sitting in the auction batch, pending."
 
// Broker: HOLD the reservation. Do not convert, do not release.
 
### **Time 2 — at session close (later — could be minutes)**
 
The clock’s Continuous→Closed transition fires. The auction computes P* (a four-level tie-break cascade) and matches the whole batch at that single price. Your order now fills fully, partially, or not at all — and the exchange returns a **second** result **keyed to the same **OrderID:
 
// Internally the auction produces:
 
type AuctionResult struct {
 
    ClearingPrice Ticks       // P* — every fill prints here
 
    HasClear      bool        // false if nothing could cross
 
    Trades        []Trade     // all at P*
 
    Cancelled     []Cancelled // every unfilled order, for the broker to release
 
}
 
// The exchange wraps these back to you as EngineResult, per order, same OrderID:
 
 
 
EngineResult{                                   // your order filled at the close
 
    Outcome: OutcomeMatched, OrderID: "o-99", BrokerID: "B1",
 
    Match: &MatchResult{
 
        Trades: []Trade{ {Price: 10020 /*P**/, Qty: 60, MakerOrder:"o-99", ...} },
 
        CancelledQty: 40, CancelReason: ReasonAuctionUnfilled,  // unfilled remainder
 
        LastPrice: 10020, LastPriceMoved: true,
 
    },
 
}
 
## **6.2  The contract you can rely on**
 
| **The exchange guarantees** The later auction result is keyed to the **same **OrderID as the divert. It is delivered in the EngineResult shape and settled by your *continuous* path — Convert the fills at P*, release any ReasonAuctionUnfilled remainder. All auction fills print at the single clearing price P*, regardless of each order’s own limit. You hold the reservation across the whole divert→clear gap. This is the one case where a reservation lives across an *unbounded* time gap; your reservation bookkeeping must survive it. |
| --- |
 
| **Being built in the background** The exact Outcome tag on the second result (reuse OutcomeMatched vs. a dedicated OutcomeAuctionFilled) is being finalised exchange-side. **Design against the guarantee**: same OrderID, EngineResult-shaped, fills at P*, unfilled remainder carries ReasonAuctionUnfilled. Build your settlement to treat an auction fill exactly like a continuous fill and the final tag will slot in without a redesign. |
| --- |
 
# **7.  Doubt #2 — partial-fill cadence: one result or many?**
 
| **The broker designer****'****s question** “A single incoming order that fills across several prints — one EngineResult with all Trades in it, or a stream of results over time? If it’s one-shot, settlement is simpler.” |
| --- |
 
## **7.1  The contract**
 
| **The exchange guarantees — one EngineResult per incoming order** A single incoming order runs **one** match call that walks the book to completion (fill / rest / cancel) and returns **exactly one **MatchResult. The shard emits **one **EngineResult for it, with **every** print batched in Match.Trades. So a taker sweeping five levels → one result, five Trades, plus Rested / CancelledQty for the remainder. Settlement is one-shot: Convert all trades, rest-or-release the remainder, done. |
| --- |
 
## **7.2  The scope of “one-shot” — read this carefully**
 
“One-shot” means **one result per incoming order at submission time**. It does **not** mean “one result for the whole life of the order.” An order’s life continues *after* its submission result in two cases:
 
- A **diverted** order resolves later, at the auction clear (§6).
 
- A **rested** order fills later, when a future taker hits it — as a **maker**. That is a separate private result, and it is exactly Doubt #3 (§8).
 
So: per-*submission* the reporting is one-shot; per-order-*lifetime* it is not. Keep those two clearly apart in the settlement engine.
 
# **8.  Doubt #3 — a resting order fills as a maker: how the broker hears about it**
 
| **The broker designer****'****s question (the sharp one)** “When my limit rests, it’s a maker. Later some other taker hits it. That trade is part of the *taker’s* result, echoing the *taker’s* OrderID — not mine. So how does my broker learn its own resting order just filled? If results echo only the taker’s OrderID, the maker’s broker never gets a private fill. This is the normal case — a resting order filling — and I must settle it.” |
| --- |
 
## **8.1  What the exchange has at the moment of a maker fill**
 
At the instant a taker hits your resting order o-M, the Trade record carries **both** sides in full — it was designed to, precisely so the maker side can be notified:
 
Trade{
 
    Seq: 107, Instrument: "ACME",
 
    Price: 10021,            // = the MAKER's resting price = YOUR price
 
    Qty:   30,               // how much of YOUR order just filled
 
    TakerOrder: "o-T",  TakerParticipant: "p-T",
 
    MakerOrder: "o-M",  MakerParticipant: "p-M",   // <- YOU
 
    TakerSide:  Buy,         // maker side is the opposite
 
}
 
// and the RestingOrder the exchange is holding & just decremented:
 
RestingOrder{ OrderID:"o-M", Participant:"p-M", Side:Sell,
 
              Price:10021, Remaining:70, Seq:5501 }  // 100 - 30 filled = 70 left
 
| **So the exchange HAS the full economic picture of your fill** which order filled — MakerOrder = o-M whose — MakerParticipant = p-M at what price — Price = 10021 (your resting price; the maker-price rule) how much — Qty = 30 your side — from RestingOrder.Side how much is still working — Remaining = 70 sequencing — Trade.Seq. Nothing about the fill itself is missing. |
| --- |
 
## **8.2  What was missing — and what the exchange now does about it**
 
Two things were missing, and note carefully: they are about **delivery**, not content.
 
- **The routing address.** The resting order recorded OrderID, Participant, Side, Price, Remaining, Seq — but **not the **BrokerID. So the exchange knew the participant but not *which broker connection* to send the maker fill to.
 
- **The emit step.** The specified flow emitted only the *taker’s* EngineResult. There was no code path emitting a *second* result to each maker.
 
| **The exchange WILL do this (being built in the background)** Neither missing piece is data the exchange can’t have. The fix is entirely exchange-side and cheap: RestingOrder now retains BrokerID, stamped at submission and kept for the order’s resting life — so a maker fill can be routed to the right broker. For **each **Trade, the exchange emits a **second, maker-side **EngineResult, keyed to MakerOrder and the maker’s BrokerID — a “your resting order filled” event. |
| --- |
 
### **The maker-side result the broker will receive**
 
EngineResult{
 
    Outcome:  OutcomeMakerFill,        // "a resting order of yours was hit"
 
    OrderID:  "o-M",                   // YOUR order id (not the taker's)
 
    BrokerID: "B_maker",               // routed to you
 
    Match: &MatchResult{
 
        Trades: []Trade{ /* that one trade: Price 10021, Qty 30, MakerOrder o-M ... */ },
 
        // Remaining working qty (70) stays in the book; not cancelled, not rested-anew.
 
        LastPrice: 10021, LastPriceMoved: true,
 
    },
 
}
 
## **8.3  The contract you can rely on**
 
| **The exchange guarantees** Every fill of your resting order reaches you as a private EngineResult keyed to **your** OrderID — never only the taker’s. It carries the fill price (= your resting price), the filled qty, and enough to run the same Convert path as a taker fill. The unfilled remainder stays working in the book; you keep holding its reservation until it fills further or you cancel it. You do **not** reconstruct maker fills from the public tape — you can’t; the public tape is **anonymized** (price + qty only) and cannot tell you which print hit your order. The private maker-fill event is the only mechanism, and it is guaranteed. |
| --- |
 
| **Why this matters for the broker****'****s core** A resting order filling is the **normal** case, not an edge case. Your settlement engine must treat a maker fill as a fully first-class private outcome — same Convert-reservation-to-position path as a taker fill — and must expect an order to receive maker fills *incrementally over time* (a resting order can be hit by many takers, each a separate OutcomeMakerFill). This is the per-order-lifetime reporting that §7 warned is *not* one-shot. |
| --- |
 
# **9.  Worked examples — input in, output out**
 
| **Shared book state for every example** Instrument "ACME". Tick size such that ₹501.05 = 10021 ticks. Circuit band [9500, 10500]. Session PhaseContinuous (except §9.6). Book: Asks:  10023  [ D: 40 ]     (participant p-D, order o-D1)        10021  [ X: 30 ]     (participant p-X, order o-X1) Bids:  10019  [ Y: 50 ]     (participant p-Y, order o-Y1) Last price: 10020 |
| --- |
 
## **9.1  Market buy, fully filled**
 
*The clean case — sweeps two levels, fully filled, nothing rests.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
 
    Submit: &FundedOrder{
 
        OrderID: "o-42", Participant: "p-Z", Instrument: "ACME",
 
        Side: Buy, Type: Market, TIF: IOC,
 
        Qty: 50, LimitPrice: 0, MaxSpend: 520000, STP: CancelIncoming,
 
    },
 
}
 
**Walk: **30 @ 10021 (X) then 20 @ 10023 (D). Spend = 300630 + 200460 = 501090, under MaxSpend. Fully filled.
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeMatched, OrderID: "o-42", BrokerID: "B1",
 
    Match: &MatchResult{
 
        Trades: []Trade{
 
            {Seq:101, Instrument:"ACME", Price:10021, Qty:30,
 
             TakerOrder:"o-42", MakerOrder:"o-X1", TakerSide:Buy,
 
             TakerParticipant:"p-Z", MakerParticipant:"p-X"},
 
            {Seq:102, Instrument:"ACME", Price:10023, Qty:20,
 
             TakerOrder:"o-42", MakerOrder:"o-D1", TakerSide:Buy,
 
             TakerParticipant:"p-Z", MakerParticipant:"p-D"},
 
        },
 
        Rested: nil, CancelledQty: 0, CancelReason: ReasonNone,
 
        LastPrice: 10023, LastPriceMoved: true,
 
    },
 
}
 
**Broker action: **Convert 30@10021 + 20@10023 into position; release the reserved-but-unspent remainder of MaxSpend (520000 − 501090).
 
## **9.2  Limit buy, partial fill then rests**
 
*Fills what it can at/under its limit, then rests the rest as a maker.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
 
    Submit: &FundedOrder{
 
        OrderID: "o-43", Participant: "p-Z", Instrument: "ACME",
 
        Side: Buy, Type: Limit, TIF: GTC,
 
        Qty: 100, LimitPrice: 10021, MaxSpend: 0, STP: CancelIncoming,
 
    },
 
}
 
**Walk: **10021 ask (X, 30) is ≤ limit → fills 30. Next ask 10023 > limit → stops. 70 remain → rests at 10021.
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeMatched, OrderID: "o-43", BrokerID: "B1",
 
    Match: &MatchResult{
 
        Trades: []Trade{
 
            {Seq:103, Instrument:"ACME", Price:10021, Qty:30,
 
             TakerOrder:"o-43", MakerOrder:"o-X1", TakerSide:Buy,
 
             TakerParticipant:"p-Z", MakerParticipant:"p-X"},
 
        },
 
        Rested: &RestingOrder{
 
            OrderID:"o-43", Participant:"p-Z", Side:Buy,
 
            Price:10021, Remaining:70, Seq:5501,
 
        },
 
        CancelledQty: 0, CancelReason: ReasonNone,
 
        LastPrice: 10021, LastPriceMoved: true,
 
    },
 
}
 
**Broker action: **Convert the 30 filled; keep the 70 reserved — the order is now working in the book and will fill later as maker fills (§8).
 
## **9.3  Market buy capped by MaxSpend (funded-cap)**
 
*Same book, but the broker only reserved enough for ~35 units.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
 
    Submit: &FundedOrder{
 
        OrderID: "o-44", Participant: "p-Z", Instrument: "ACME",
 
        Side: Buy, Type: Market, TIF: IOC,
 
        Qty: 60, MaxSpend: 350000, STP: CancelIncoming,
 
    },
 
}
 
**Walk: **30 @ 10021 = 300630. At 10023 affordable = floor((350000−300630)/10023) = 4. Fills 4 @ 10023. MaxSpend exhausted → cancel remainder.
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeMatched, OrderID: "o-44", BrokerID: "B1",
 
    Match: &MatchResult{
 
        Trades: []Trade{
 
            {Seq:104, Price:10021, Qty:30, TakerOrder:"o-44",
 
             MakerOrder:"o-X1", TakerSide:Buy, ...},
 
            {Seq:105, Price:10023, Qty:4,  TakerOrder:"o-44",
 
             MakerOrder:"o-D1", TakerSide:Buy, ...},
 
        },
 
        Rested: nil,
 
        CancelledQty: 26,                 // 60 - 34 filled
 
        CancelReason: ReasonFundedCap,    // hit MaxSpend
 
        LastPrice: 10023, LastPriceMoved: true,
 
    },
 
}
 
**Broker action: **Convert the 34 filled; release the reservation portion for the 26 cancelled.
 
## **9.4  Rejected at admission (off-tick limit)**
 
*A limit price not on the tick grid never enters the book.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
 
    Submit: &FundedOrder{
 
        OrderID: "o-45", Participant: "p-Z", Instrument: "ACME",
 
        Side: Buy, Type: Limit, TIF: GTC,
 
        Qty: 10, LimitPrice: 100215,  // between grid points -> off-tick
 
        STP: CancelIncoming,
 
    },
 
}
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeRejected, OrderID: "o-45", BrokerID: "B1",
 
    Match: nil,
 
    Rejection: &Rejection{ Reason: RejectBadTick, Stage: "tick_grid" },
 
}
 
**Broker action: **Release the whole reservation — nothing entered the book. (Same shape for RejectMarketClosed / RejectNoLiquidity / RejectBeyondCircuit / RejectMalformed.)
 
## **9.5  Self-match (CancelIncoming)**
 
*p-X, who has a resting sell o-X1 @ 10021, sends a buy that would cross into it.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
 
    Submit: &FundedOrder{
 
        OrderID: "o-46", Participant: "p-X",   // SAME participant as maker o-X1
 
        Instrument: "ACME", Side: Buy, Type: Market, TIF: IOC,
 
        Qty: 20, MaxSpend: 210000, STP: CancelIncoming,
 
    },
 
}
 
**Walk: **Best ask o-X1 is owned by p-X → self-match. CancelIncoming → incoming cancelled, no trade.
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeMatched, OrderID: "o-46", BrokerID: "B1",
 
    Match: &MatchResult{
 
        Trades: []Trade{},              // no trade between same participant
 
        Rested: nil,
 
        CancelledQty: 20, CancelReason: ReasonSelfMatch,
 
        LastPrice: 0, LastPriceMoved: false,
 
    },
 
}
 
**Broker action: **Release the full reservation.
 
## **9.6  Limit order diverted to the closing auction**
 
*Same order, but the session is now PhaseClosingAuction. Two results, two times.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
 
    Submit: &FundedOrder{
 
        OrderID: "o-99", Participant: "p-Z", Instrument: "ACME",
 
        Side: Sell, Type: Limit, TIF: GTC,
 
        Qty: 100, LimitPrice: 10020, STP: CancelIncoming,
 
    },
 
}
 
**Walk: **During collection the gate diverts to the auction batch — no immediate match.
 
### **Exchange returns**
 
// Time 1 - at submission (immediate ACK):
 
EngineResult{ Outcome: OutcomeDiverted, OrderID: "o-99", BrokerID: "B1" }
 
 
 
// Time 2 - at session close, auction clears at P* = 10020:
 
EngineResult{
 
    Outcome: OutcomeMatched, OrderID: "o-99", BrokerID: "B1",  // SAME OrderID
 
    Match: &MatchResult{
 
        Trades: []Trade{ {Price:10020, Qty:60, MakerOrder:"o-99", ...} },
 
        CancelledQty: 40, CancelReason: ReasonAuctionUnfilled,
 
        LastPrice: 10020, LastPriceMoved: true,
 
    },
 
}
 
**Broker action: **On Time 1: HOLD the reservation. On Time 2: Convert 60 @ P*, release the 40 unfilled. (See §6.)
 
## **9.7  Cancel (success)**
 
*Cancel the resting limit o-43 from §9.2, which had 70 remaining.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: CancelOrder, BrokerID: "B1", Instrument: "ACME",
 
    Cancel: &CancelRequest{ OrderID: "o-43" },
 
}
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeCancelled, OrderID: "o-43", BrokerID: "B1",
 
    CancelledQty: 70,
 
}
 
// plus a public quote/depth update on the tape (a resting bid vanished)
 
**Broker action: **Release the reservation for 70. (If part had filled first, CancelledQty would be smaller — release exactly what’s echoed.)
 
## **9.8  Cancel (not found)**
 
*Same request, but o-43 already filled or was already cancelled.*
 
### **Broker sends**
 
InboundMessage{
 
    Kind: CancelOrder, BrokerID: "B1", Instrument: "ACME",
 
    Cancel: &CancelRequest{ OrderID: "o-43" },
 
}
 
### **Exchange returns**
 
EngineResult{
 
    Outcome: OutcomeCancelRejected, OrderID: "o-43", BrokerID: "B1",
 
}
 
**Broker action: **None — a normal outcome, no state change (the fill/cancel that removed it was already settled through its own result).
 
## **9.9  A resting order fills as a maker (the §8 case, end to end)**
 
*o-43 rests with 70 remaining (from §9.2). Later a taker o-77 from another broker sells into it.*
 
### **Broker sends**
 
// Another broker's taker (shown for context - it is NOT your message):
 
InboundMessage{
 
    Kind: SubmitOrder, BrokerID: "B2", Instrument: "ACME",
 
    Submit: &FundedOrder{ OrderID:"o-77", Participant:"p-W",
 
        Side: Sell, Type: Market, TIF: IOC, Qty: 40 },
 
}
 
**Walk: **o-77 sells 40 into your resting buy o-43 (70 avail) at your price 10021. 40 fills; 30 of yours still rests.
 
### **Exchange returns**
 
// B2 (the taker's broker) receives the taker result:
 
EngineResult{ Outcome: OutcomeMatched, OrderID:"o-77", BrokerID:"B2",
 
    Match: &MatchResult{ Trades:[{Price:10021, Qty:40,
 
        TakerOrder:"o-77", MakerOrder:"o-43", ...}], ... } }
 
 
 
// B1 (YOUR broker, the maker) receives a SEPARATE result, same instant:
 
EngineResult{
 
    Outcome: OutcomeMakerFill, OrderID: "o-43", BrokerID: "B1",  // YOUR OrderID
 
    Match: &MatchResult{
 
        Trades: []Trade{ {Seq:108, Price:10021, Qty:40,
 
            TakerOrder:"o-77", MakerOrder:"o-43", TakerSide:Sell,
 
            TakerParticipant:"p-W", MakerParticipant:"p-Z"} },
 
        LastPrice: 10021, LastPriceMoved: true,
 
    },
 
}
 
**Broker action: **Convert 40 @ 10021 into position. 30 of o-43 stays working — keep its reservation held; further takers produce more OutcomeMakerFill results over time.
 
# **10.  Settlement quick-reference**
 
Every private outcome the broker will see, and the one action each demands:
 
| **On this Outcome...** | **Meaning** |
| --- | --- |
| **OutcomeMatched** | → Convert every trade in Match.Trades; if Rested, keep that reservation held; if CancelledQty > 0, release it (reason in CancelReason). |
| **OutcomeDiverted** | → HOLD the reservation. Await a later result on the same OrderID at auction clear. |
| **OutcomeRejected** | → Release the whole reservation. Nothing entered the book. |
| **OutcomeCancelled** | → Release CancelledQty (exactly as echoed — may be less than you asked). |
| **OutcomeCancelRejected** | → Do nothing. The order was already gone and settled elsewhere. |
| **OutcomeMakerFill** | → Convert the trade(s). Remaining resting qty stays working — keep its reservation. Expect more of these over time for the same order. |
 
| **Three things the broker must never assume** **Never assume an order resolves at submission. **Diverted and rested orders resolve later — across an unbounded gap for auctions, and incrementally for resting makers. **Never release the qty you asked for on a cancel — **release the CancelledQty the exchange echoes. A fill may have beaten your cancel. **Never try to learn your fills from the public tape. **It is anonymized. All of your fills — taker and maker — arrive on the private EngineResult stream keyed to your OrderID. |
| --- |
 
**End of contract. **Input (§3), output (§4), cancel (§5), the three doubts resolved (§6–8), examples (§9), settlement map (§10). Where a behaviour is being finalised exchange-side, the guarantee it will honour is stated — build against the guarantee.