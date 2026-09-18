Exchange 1.4 Admission Gate (reduced) — Implementation

Prerequisites: EXCHANGE_00_ARCHITECTURE.md (split, seam, invariants), EXCHANGE_11_ORDERBOOK_QUOTE.md (Book, QuoteState, FundedOrder, value types), EXCHANGE_12_MATCHING_CORE.md (Matcher, MatchResult, SeqSource). Uses those types directly; does not redefine them.

What 1.4 owns: the receiving edge of the exchange. A FundedOrder that arrives hits this pipeline first. The pipeline validates the order (structural, session phase, liquidity, tick grid, circuit band), stamps its time-priority sequence, and routes it to either the continuous matcher (1.2) or the auction batch (1.3) — or rejects it. It produces the engine's single output type, EngineResult.

Gate 4 is gone (architecture §1.4). Funding is the broker's job, checked before submission. This pipeline never consults money and holds no account reference.

0. Philosophy — why the exchange re-checks what the broker already checked

The broker also holds the rulebook and a (lagging) copy of the orderbook, so it can and will pre-check hours, tick, band, and liquidity before submitting — an optimization to avoid wasting an exchange round-trip on an order it knows will bounce. The exchange keeps its own gates anyway because the broker checks to be polite; the exchange checks to be safe. The exchange is the sole owner of the book and the session clock, so it is the only component that can authoritatively say "the market is closed now" or "that price is off-tick." The broker's orderbook view arrives via the tape, slightly behind truth; only the exchange catches the order that was fundable-and-matchable a microsecond ago but is not now. The exchange never trusts that the broker checked — a stale view or bug on the broker side must never be able to corrupt the book.

The sole exception is funding (old Gate 4): the exchange does not re-check it, because it structurally cannot (no account access). Everything else is checked in both places — broker for efficiency, exchange for correctness.

1. The single engine output type

The whole pipeline returns exactly one type. Outcomes are discriminated inside it, not by different return types — the receiving edge always gets one shape and switches on Outcome.

go
type Outcome int
const (
    OutcomeMatched  Outcome = iota // passed all stages, went to the continuous matcher
    OutcomeDiverted                // passed all stages, collected into the auction batch
    OutcomeRejected                // failed a stage; never entered the book or the auction
    OutcomeCancelled                     // a cancel succeeded
	OutcomeCancelRejected                // a cancel found nothing to remove
	OutcomeMakerFill                // a RESTING order of yours was hit by a taker
)

type EngineResult struct {
    Outcome Outcome
    OrderID OrderID // echoed on EVERY outcome — the broker needs it to reconcile / release
    BrokerID     BrokerID
    Match     *MatchResult // non-nil iff Outcome == OutcomeMatched (from 1.2)
    Rejection *Rejection   // non-nil iff Outcome == OutcomeRejected
    // OutcomeDiverted carries no extra payload: the order now sits in the auction
    // batch; its fills arrive later when the auction clears (1.3).
    CancelledQty Qty
}

OrderID sits at the top level (not only inside the payloads) so every outcome — including a diverted order — echoes it back. A rejected order's echo lets the broker release the reservation it made; a diverted order's echo tells the broker "accepted, pending auction."

2. The rejection type
go
type RejectReason int
const (
    RejectMalformed    RejectReason = iota // structural: bad shape/type/qty/price fields
    RejectMarketClosed                     // session phase: closed / outside hours
    RejectNoLiquidity                      // liquidity: market order vs empty opposite book
    RejectBadTick                          // tick grid: price off the grid
    RejectBeyondCircuit                    // circuit band: price beyond the band
)

type Rejection struct {
    Reason RejectReason
    Stage  string // which stage produced it — diagnostic/logging only, not logic-bearing
                  // values: "structural","session_phase","liquidity","tick_grid","circuit_band"
}

The broker acts on Reason; Stage is human-readable traceability only.

3. Collaborators

The pipeline consults other components to decide. Each is a question the order's own fields can't answer. Concrete ones (Book, Matcher) are done; the rest are interfaces defined here and implemented when their component is built — the same "depend on an interface, defer the concrete type" discipline as SideBook and CircuitClamp.

go
// Session phase — implemented by 1.8 (Session-Phase/Clock). Answers "is the market
// open, and in which mode?" A stub returning PhaseContinuous tests the pipeline alone.
type Phase int
const (
    PhaseClosed         Phase = iota // outside session -> reject
    PhaseContinuous                  // normal trading -> route to matcher
    PhaseClosingAuction              // collection window -> route to auction batch
    // PhaseOpeningAuction reserved; prototype may not use it
)
type SessionPhase interface {
    Current() Phase
}

// Auction batch — implemented by 1.3. Collects a diverted order into the current
// batch; the order matches later, at auction clear.
type AuctionBatch interface {
    Add(o *FundedOrder)
}

The rulebook is injected configuration, not a live component — present at session start, never phoned. The pipeline reads only the tick size from it:

go
type Rulebook struct {
    TickSize Ticks // grid unit; a limit price is valid iff price % TickSize == 0
    // other components read other rulebook fields; the gate reads only TickSize here
}

The circuit band bounds are NOT in the rulebook. They are owned by the instrument's CircuitBand (1.5), which is the single source of truth for both this gate's checkCircuitBand and the matcher's PriceAllowed — so the admit-side and the clamp-side can never disagree. checkCircuitBand reads the bounds via ctx.band.Bounds() (see §4). The rulebook carries the band width (a percentage), which 1.5 uses to compute the bounds once at session start; the gate never sees the percentage, only the computed bounds through 1.5.

Price representation note (decision): Ticks is a base integer unit (e.g. paise), and TickSize is the instrument's tradeable grid as a multiple of it (e.g. 5). So the tick-grid check is real: price % TickSize == 0. This is not vestigial.

Circuit band (decision): the band is owned per-instrument by CircuitBand (1.5), computed once at session start from the instrument's frozen daily reference and the rulebook band width (floating bands deferred). The gate reads the bounds via ctx.band.Bounds() and does a pure comparison, computing nothing — and reads the same bounds object the matcher's clamp uses, so admit-side and clamp-side agree by construction.

4. Pipeline shared context and control types

Each stage receives the order plus a shared context carrying the collaborators and the small amount of working state stages pass to each other.

go
type routeTarget int
const (
    routeMatch   routeTarget = iota // -> continuous matcher (default)
    routeAuction                     // -> auction batch
)

type gateCtx struct {
    // Shared across all instruments (session-level):
    phase SessionPhase // the global session clock (1.8); read-only here
    rules Rulebook     // injected config (tick size, band width)

    // Per-instrument — supplied by the shard from the target InstrumentState before
    // running the pipeline (see the sharding architecture doc). Each is the state of
    // the ONE instrument this order names:
    book    *Book        // this instrument's book/quote (1.1/1.7)
    band    *CircuitBand // this instrument's band (1.5) — bounds for checkCircuitBand
    seq     SeqSource    // this instrument's per-instrument sequence (1.9)
    matcher *Matcher     // this instrument's matcher (1.2)
    auction AuctionBatch // this instrument's auction batch (1.3)

    route routeTarget // set by checkSessionPhase; read by routeOrder. defaults to routeMatch
}

How gateCtx is populated (sharding preview). The exchange runs a fixed pool of shard workers; each instrument is assigned to one shard by a stable hash of its id, and that shard owns the instrument's InstrumentState (book, band, seq, matcher, auction). When an order arrives, its shard looks up the InstrumentState for order.Instrument and builds a gateCtx pointing at that instrument's per-instrument collaborators, plus the shared session phase and rulebook. The pipeline logic below is identical regardless of instrument — only the collaborators the ctx points at differ. The full ingress→shard→egress wiring is its own dedicated doc; the gate only needs to know its ctx is pre-populated for the right instrument.

go

// A stage inspects the order and context and returns a control signal.
type stageResult struct {
    stop    bool          // true -> terminate the pipeline now
    outcome *EngineResult // set iff stop == true: the final result to return
}

type Stage func(o *FundedOrder, ctx *gateCtx) stageResult

// helpers
func cont() stageResult { return stageResult{stop: false} }
func reject(o *FundedOrder, reason RejectReason, stage string) stageResult {
    return stageResult{stop: true, outcome: &EngineResult{
        Outcome: OutcomeRejected, OrderID: o.OrderID,
        Rejection: &Rejection{Reason: reason, Stage: stage},
    }}
}
5. The pipeline — an ordered, reorderable slice of stages

The pipeline is a slice of stage functions run in order until one stops it. It is not one monolithic function: any stage can be reordered, replaced, or removed by editing the slice, each stage is independently testable, and the "Gate 4 removed" change is literally that stage's absence from the slice.

go
var pipeline = []Stage{
    checkStructural,     // shape/type/qty/price-field validity
    checkSessionPhase,   // reject if closed; set route (match vs auction)
    checkLiquidity,      // market order vs empty opposite book (skipped for auction route)
    checkTickGrid,       // limit price on the tick grid
    checkCircuitBand,    // limit price within the circuit band
    stampSequence,       // assign time-priority Seq — survivors only, just before the fork
    routeOrder,          // terminal: matcher.Match OR auction.Add; builds EngineResult
}

func RunPipeline(o *FundedOrder, ctx *gateCtx) EngineResult {
    for _, stage := range pipeline {
        r := stage(o, ctx)
        if r.stop {
            return *r.outcome
        }
    }
    panic("pipeline did not terminate") // routeOrder is always last and always stops
}

Stage names are descriptive of what they check — never positional (checkTickGrid, not gate2). The Rejection.Stage string values follow the same rule.

6. Stage logic

Each stage below: what it reads, its logic, what it returns.

6.1 checkStructural

Reads: the order's own fields only. No collaborator. Pure shape-checking — a limit must carry a price, a market must carry none. (Stop types never reach the exchange — they are broker-side; see the 1.1 order menu.)

checkStructural(o, ctx):
    if o.Qty <= 0:                                       return reject(o, RejectMalformed, "structural")
    if o.Type not in {Limit, Market}:                    return reject(o, RejectMalformed, "structural")
    switch o.Type:
        Limit:   if o.LimitPrice <= 0:                   return reject(o, RejectMalformed, "structural")
        Market:  if o.LimitPrice != 0:                   return reject(o, RejectMalformed, "structural")
    if o.Side not in {Buy, Sell}:                        return reject(o, RejectMalformed, "structural")
    return cont()
6.2 checkSessionPhase

Reads: ctx.phase.Current() (1.8). Routes or rejects by phase; sets ctx.route. On the closing-auction window it sets the route and continues — the order still faces the remaining gates (decision: auction orders run tick and band checks too).

checkSessionPhase(o, ctx):
    switch ctx.phase.Current():
        PhaseClosed:          return reject(o, RejectMarketClosed, "session_phase")
        PhaseContinuous:      ctx.route = routeMatch;   return cont()
        PhaseClosingAuction:  ctx.route = routeAuction; return cont()
    return reject(o, RejectMarketClosed, "session_phase")   // defensive default
6.3 checkLiquidity

Reads: ctx.book.Quote() (1.7 HasBid/HasAsk). A market order against an empty opposite book is rejected early. Only market orders — a limit against an empty book simply rests. Skipped for auction-routed orders (decision): an auction collects and clears in a batch, so it needs no resting opposite book at admission.

checkLiquidity(o, ctx):
    if ctx.route == routeAuction:                        return cont()   // auction clears in batch
    if o.Type != Market:                                 return cont()   // only market needs liquidity now
    q := ctx.book.Quote()
    if o.Side == Buy  and not q.HasAsk:                  return reject(o, RejectNoLiquidity, "liquidity")
    if o.Side == Sell and not q.HasBid:                  return reject(o, RejectNoLiquidity, "liquidity")
    return cont()
6.4 checkTickGrid

Reads: ctx.rules.TickSize. A limit price must sit on the grid. Market orders have no resting price to check.

checkTickGrid(o, ctx):
    if o.Type == Market:                                 return cont()   // no resting price
    if o.LimitPrice % ctx.rules.TickSize != 0:           return reject(o, RejectBadTick, "tick_grid")
    return cont()
6.5 checkCircuitBand

Reads: ctx.band.Bounds() (1.5 — this instrument's band; the same object the matcher's clamp uses). Rejects an order whose immediately-restable limit price is beyond the band. This is the reject-half of the circuit rule; the clamp-during-matching half lives in 1.2/1.5. Market orders have no price to reject on (they are clamped during the walk instead).

checkCircuitBand(o, ctx):
    if o.Type == Market:                                 return cont()   // clamp handles at walk time
    lower, upper := ctx.band.Bounds()
    if o.LimitPrice < lower or o.LimitPrice > upper:
                                                         return reject(o, RejectBeyondCircuit, "circuit_band")
    return cont()
6.6 stampSequence

Reads: ctx.seq.Next() (1.9). Assigns the monotonic time-priority sequence, now that the order has survived every gate and is about to fork. Only survivors reach here, so no Seq is spent on a rejected order. Single stamping site feeding both forks.

stampSequence(o, ctx):
    o.Seq = ctx.seq.Next()
    return cont()
6.7 routeOrder (terminal stage)

Reads: ctx.route, plus ctx.matcher / ctx.auction. The single exit. Sends the stamped order to continuous matching or the auction batch and builds the one EngineResult. Always stops the pipeline.

routeOrder(o, ctx):
    switch ctx.route:
        routeMatch:
            mr := ctx.matcher.Match(o)                   // 1.2 -> MatchResult
            return stageResult{stop: true, outcome: &EngineResult{
                Outcome: OutcomeMatched, OrderID: o.OrderID, Match: &mr }}
        routeAuction:
            ctx.auction.Add(o)                           // 1.3 collects into the batch
            return stageResult{stop: true, outcome: &EngineResult{
                Outcome: OutcomeDiverted, OrderID: o.OrderID }}
7. Where the pipeline sits in the flow

An arriving FundedOrder (delivered by the inbound channel — channel design is separate, architecture §8) is handed to RunPipeline. The returned EngineResult goes back out: a rejection or divert becomes a private terminal event to the broker (carrying the echoed OrderID); a match's MatchResult carries the trades (to the tape and to the broker as private fills) plus any rested/cancelled remainder. The Event Emitter (1.9) shapes all of that into wire events; the pipeline itself only produces the EngineResult value.

8. Invariants 1.4 must hold
A rejected order never mutates the book or consumes a Seq — rejection stops the pipeline before stampSequence and before routeOrder.
Every order that reaches matching or auction has a stamped Seq, assigned at exactly one site (stampSequence), feeding both forks.
OrderID is echoed on every EngineResult, all three outcomes.
The pipeline consults no money state and holds no account reference — Gate 4 is absent; funding is the broker's.
Auction-routed orders run every gate except liquidity — structural, phase, tick, and band all apply; only the immediate-matchability liquidity check is skipped.
Stage identifiers are descriptive, never positional — in code and in the Rejection.Stage diagnostic string.
The pipeline always terminates in routeOrder — the only stage besides a rejection that stops it, and it is always last.
9. Deferred / handed to other components
Session phase truth — 1.8 implements SessionPhase; until then a stub returns PhaseContinuous.
Auction collection & clearing — 1.3 implements AuctionBatch.Add and the clear; the pipeline only diverts into it.
The clamp-during-matching half of circuits — 1.2/1.5; checkCircuitBand is only the reject-at-admission half.
Stop orders — not an exchange concern at all. Stop-market and stop-limit orders are held, monitored, and triggered on the broker side; the broker submits the resulting order as an ordinary funded Market/Limit order through the normal inbound channel when the trigger crosses. The exchange has no stop types, no trigger field, and no dormant-order storage.