Exchange — Sharding & Wiring Architecture (the spine)

Status: Locked. This is the spine that ties every exchange component (1.1–1.9) together: the connection edge, how orders are routed, the shard worker pool and its loop, how the emitter's two channels leave, the session lifecycle, and the full end-to-end request flows.

Prerequisites — read the component docs first; this wires finished pieces: EXCHANGE_00_ARCHITECTURE.md (split, seam, §6a sharding decision), EXCHANGE_11_ORDERBOOK_QUOTE.md (Book, QuoteState, FundedOrder, value types), EXCHANGE_12_MATCHING_CORE.md (Matcher, MatchResult, SeqSource), EXCHANGE_13_AUCTION_MATCHER.md (AuctionBook, AuctionResult), EXCHANGE_14_ADMISSION_GATE.md (pipeline, EngineResult, gateCtx), EXCHANGE_15_CIRCUIT_SYSTEMS.md (CircuitBand), EXCHANGE_18_SESSION_PHASE_CLOCK.md (SessionClock, phase monitor), EXCHANGE_19_EVENT_EMITTER.md (Emitter, TapeEvent, OHLCVAggregator).

0. Two standing rules (non-negotiable)

Rule 1 — one-in / one-out per connection. Anyone connecting to the exchange speaks over exactly one inbound channel and receives over one outbound path (a public tape channel + a private channel — the two together are the "out"). Orders and events for all instruments are multiplexed over these, self-described by an Instrument tag. There are never per-instrument channels exposed to a connected party. In the demo these are Go channels (one process); a later process split swaps them for a network transport without changing internal logic.

Rule 2 — every inbound message carries a BrokerID. The broker stamps a BrokerID on every message it sends (submit or cancel). The exchange echoes it on the corresponding private EngineResult, so that when multiple brokers connect, each broker's private events can be routed back to it and never leak to another broker. (The public tape is broadcast and carries no BrokerID.) The demo runs one broker, but the field is present from day one so multi-broker is an additive change, not a retrofit.

1. Topology (the whole exchange at a glance)
                 ══════════════════ ONE PROCESS ══════════════════
  ┌────────┐     one inbound Go chan
  │ BROKER │──── InboundMessage ────────────────┐
  │ (+L5,  │      (Submit | Cancel,              │
  │  Gate4,│       tagged BrokerID+Instrument)   ▼
  │  stops)│                              ┌─────────────┐
  │        │                              │  INGRESS    │  route by hash(Instrument) % P
  │        │                              │ (1 goroutine)│
  │        │                              └──────┬──────┘
  │        │                     ┌───────────────┼───────────────┐
  │        │                     ▼               ▼               ▼
  │        │              ┌───────────┐   ┌───────────┐   ┌───────────┐
  │        │              │  SHARD 0  │   │  SHARD 1  │ … │ SHARD P-1 │   (P worker goroutines)
  │        │              │ owns map: │   │ owns map: │   │ owns map: │
  │        │              │ {A,D,…}   │   │ {B,E,…}   │   │ {C,F,…}   │
  │        │              │ each inst:│   │           │   │           │
  │        │              │  Book,    │   │  serial   │   │  serial   │
  │        │              │  Matcher, │   │  loop:    │   │  loop:    │
  │        │              │  Band,    │   │  orders + │   │  orders + │
  │        │              │  Auction, │   │  1s ticker│   │  1s ticker│
  │        │              │  OHLCV,   │   │           │   │           │
  │        │              │  Emitter  │   │           │   │           │
  │        │              └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
  │        │                    │  writes       │               │
  │        │       private ◀────┴───────────────┴───────────────┘
  │        │◀──── EngineResult (one shared private Go chan, tagged BrokerID) ──┘
  │        │
  │        │       public  ◀──── TapeEvent (one shared public Go chan, tagged Instrument)
  └────────┘             │                    ▲ all shards write here
                         ▼                    │ (many writers, Go-safe)
                 ┌──────────────┐             │
                 │ tape consumers│  broker relay to users, charts, persistence, Layer 6
                 └──────────────┘

  shared, above the shards:  SessionClock (global; phase read via atomic by every shard)

Every arrow is a Go channel. Every box is a goroutine (or a pool of them). Nothing leaves the process. If the broker is later split out, only the three arrows crossing the broker boundary become network streams.

2. The edge input type — InboundMessage

The one inbound channel carries InboundMessage, a thin wrapper letting one channel express both "submit an order" and "cancel an order." FundedOrder is unchanged and lives intact inside the submit variant.

go
type InboundKind int
const (
    SubmitOrder InboundKind = iota // a new funded order to admit + match
    CancelOrder                     // pull a resting order by id
)

type InboundMessage struct {
    Kind       InboundKind    // discriminator: which variant is set
    BrokerID   BrokerID       // Rule 2: who sent it; echoed on the private result
    Instrument InstrumentID   // routing key — ingress hashes this to a shard
    Submit     *FundedOrder   // non-nil iff Kind==SubmitOrder (the intact funded order)
    Cancel     *CancelRequest // non-nil iff Kind==CancelOrder
}

type CancelRequest struct {
    OrderID OrderID // the resting order to remove
}

Field-by-field:

Kind — tells the shard which handler to run (submit → full pipeline; cancel → book pull). Ingress does not read it; it routes both kinds identically.
BrokerID — the connecting broker's id. Present on every message (Rule 2). The exchange treats it as opaque; it only stores it to echo on the private EngineResult.
Instrument — the instrument the message concerns. This is the routing key: ingress computes hash(Instrument) % P to pick the shard. Present on both submit and cancel (a cancel must reach the same shard that owns the order's book).
Submit — the full FundedOrder (order id, participant, side, type, TIF, qty, limit price, MaxSpend reservation envelope). Untouched by the wrapper.
Cancel — just the OrderID to pull. A cancel needs no price, no funding — it is a pure book operation.

Note the FundedOrder itself should also carry BrokerID (or ingress copies the message's BrokerID onto the order) so it is available deep in the pipeline when the EngineResult is built. Either is fine; the invariant is that the result can be stamped with the originating broker.

3. Per-instrument state — InstrumentState

Everything one instrument needs, bundled. Owned by exactly one shard; touched only by that shard's goroutine, so nothing inside needs a lock.

go
type InstrumentState struct {
    instrument InstrumentID // this instrument's id (the map key)

    book    *Book             // 1.1/1.7 — order book + derived quote
    matcher *Matcher          // 1.2 — continuous matching over `book`
    band    *CircuitBand      // 1.5 — this instrument's frozen circuit band
    auction *AuctionBook      // 1.3 — closing-auction batch for this instrument
    ohlcv   *OHLCVAggregator  // 1.9 — rolls 1-min candles from this instrument's trades
    emitter *Emitter          // 1.9 — owns the per-instrument event Seq; writes the 2 out channels

    admitSeq uint64           // 1.4 — per-instrument admission sequence (time priority);
                              // distinct from the emitter's event Seq
    rules    Rulebook         // injected config the gate reads (tick size, band width, etc.)
}

Field-by-field:

instrument — identity; also the key under which this bundle sits in the shard's map.
book — the sole owner of this instrument's resting orders, quote, and last price. The matcher, gate, and cancel path all operate through it.
matcher — runs the continuous walk; holds references to book, band (as its CircuitClamp), and emitter (as its SeqSource). Built once at instrument registration.
band — frozen [lower, upper] computed at session start from this instrument's reference and the rulebook width. Read by the gate (Bounds) and the matcher (PriceAllowed) — one object, so admit-side and clamp-side agree.
auction — collects diverted orders during the closing-auction window; cleared once at close.
ohlcv — fed each trade; rolled every minute by the shard's ticker to emit a candle.
emitter — the choke point: owns this instrument's event sequence counter, holds the two shared outbound channel handles, exposes the Emit* methods. Implements SeqSource (so the matcher/auction stamp trade Seqs from it).
admitSeq — the time-priority counter stamped on each admitted order (stampSequence). Per-instrument (priority only matters within one book). Separate from the emitter's event Seq.
rules — the injected rulebook fields the gate reads (tick size; band width feeds the band at construction).
4. The shard — a worker owning many instruments
go
type Shard struct {
    id       int
    inbound  <-chan InboundMessage         // this shard's slice of routed messages
    instrs   map[InstrumentID]*InstrumentState // the instruments assigned to this shard
    clock    *SessionClock                 // shared global clock (read-only here, via atomic)
    ticker   <-chan Time                   // 1-second tick for time-based work
    public   chan<- TapeEvent              // shared public out (passed to each emitter)
    private  chan<- EngineResult           // shared private out (passed to each emitter)
}

Field-by-field:

id — which shard (0..P-1); diagnostic.
inbound — the channel ingress forwards this shard's messages onto. Only messages whose hash(Instrument)%P == id arrive here, so every instrument this shard reads is in its instrs map.
instrs — the shard's own instruments. Lookups are instrs[msg.Instrument]. Adding an instrument = inserting here (no new goroutine).
clock — the one global SessionClock. The shard reads phase via clock.Current() (atomic load) inside the gate, and calls clock.checkTransition(now) on its ticker.
ticker — fires ~once/second; drives candle rolls and phase-transition checks.
public, private — the two shared outbound channels, handed to each instrument's emitter at registration. Many shards write them; Go makes that safe; per-instrument ordering holds because one shard writes one instrument's events serially.
4.1 Why a fixed pool, not one goroutine per instrument

P is sized to CPU cores (configurable), not to instrument count. Instruments grow into the instrs maps; goroutine count stays P + ingress + (monitor folded into shards). 5 instruments or 5,000, the concurrency shape is unchanged, and adding an instrument is a map insert. Within a shard, its instruments are processed serially, preserving the single-writer/lock-free/deterministic guarantee per instrument; different shards run in parallel (free concurrency), sharing no state.

5. Ingress — routing the one inbound channel
go
type Ingress struct {
    inbound   <-chan InboundMessage      // THE one edge inbound channel
    shardIn   []chan InboundMessage      // one internal channel per shard (len == P)
    numShards int
}

func (g *Ingress) run() {
    for msg := range g.inbound {
        s := hash(msg.Instrument) % g.numShards
        g.shardIn[s] <- msg              // forward; no logic, no validation here
    }
}

Ingress is a pure router: read one message, hash its Instrument, forward to the owning shard's channel. It does not validate, admit, or inspect Kind — all of that is the shard's job. It exists solely because Rule 1 mandates a single inbound edge that must fan out to the shard pool. hash is any stable hash of the instrument id (e.g. FNV); stability matters so an instrument always lands on the same shard (its state lives there).

Cross-process later: ingress is where a network receiver would deserialize incoming messages before forwarding — the fan-out stays identical.

6. The shard loop — the orchestrator

One goroutine per shard, running a select over its inbound messages and its ticker. It is the driver that calls every component and the emitter.

Shard.run():
  for {
    select {

    case msg := <-inbound:
        inst := instrs[msg.Instrument]        // O(1) lookup; guaranteed present
        switch msg.Kind {
          case SubmitOrder: handleSubmit(inst, msg.BrokerID, msg.Submit)
          case CancelOrder: handleCancel(inst, msg.BrokerID, msg.Cancel)
        }

    case now := <-ticker:                      // ~1/second
        handleTick(now)
    }
  }
6.1 handleSubmit — the full path (exactly the component design)
handleSubmit(inst, brokerID, order):
    ctx := buildGateCtx(inst, order)           // points gate at THIS instrument's
                                               // book/band/seq/matcher/auction + shared phase/rules
    result := RunPipeline(order, ctx)          // 1.4: gate stages → matcher OR auction fork
    result.BrokerID = brokerID                 // Rule 2: stamp for private routing

    inst.emitter.EmitResult(result)            // PRIVATE → the originating broker (always)

    if result.Outcome == OutcomeMatched && result.Match != nil:
        m := result.Match
        inst.emitter.EmitTrades(m.Trades)              // PUBLIC prints (identity stripped)
        for t in m.Trades: inst.ohlcv.ingestTrade(t.Price, t.Qty)   // feed candles
        if quoteChanged: inst.emitter.EmitQuote(inst.book.Quote())  // PUBLIC (on change)
        if depthChanged: inst.emitter.EmitDepth(inst.book.TopN(10)) // PUBLIC (on change)
    // OutcomeDiverted: order sits in inst.auction; nothing more now — clears at close
    // OutcomeRejected: private EngineResult already carries the rejection; no public

RunPipeline is the admission pipeline from 1.4 unchanged: checkStructural → checkSessionPhase → checkLiquidity → checkTickGrid → checkCircuitBand → stampSequence → routeOrder. routeOrder either calls inst.matcher.Match(order) (→ MatchResult) or inst.auction.Add(order) (→ diverted).

6.2 handleCancel — the short path (skips gate + matcher)
handleCancel(inst, brokerID, req):
    removed, found := inst.book.Cancel(req.OrderID)     // 1.1 — O(1) via the order index
    if !found:
        inst.emitter.EmitResult(EngineResult{           // already gone / never existed
            Outcome: OutcomeCancelRejected, OrderID: req.OrderID, BrokerID: brokerID})
        return
    if quoteChanged: inst.emitter.EmitQuote(inst.book.Quote())   // PUBLIC — book changed
    if depthChanged: inst.emitter.EmitDepth(inst.book.TopN(10))  // PUBLIC
    inst.emitter.EmitResult(EngineResult{                        // PRIVATE → broker
        Outcome: OutcomeCancelled, OrderID: req.OrderID, BrokerID: brokerID,
        CancelledQty: removed.Remaining})                        // broker Releases this qty

A cancel is not admitted — no gate, no matcher. Its only failure is not-found (a normal outcome). It emits public quote/depth (a vanished resting order is real market data) and a private result so the broker releases the leftover reservation. A modify is the broker sending cancel + a fresh submit; the exchange has no modify path (a price/size-up change loses time priority, so it is a new order).

Two Outcome values are added for cancels: OutcomeCancelled, OutcomeCancelRejected.

6.3 handleTick — time-based work (candles + phase)
handleTick(now):
    // (a) session phase — one shared global clock; this shard checks it and acts
    transition := clock.checkTransition(now)      // nil if no boundary crossed
    if transition != nil:
        handleTransition(transition)              // emits session events; at close, clears auctions

    // (b) OHLCV rolls for this shard's instruments
    for inst in instrs:
        if inst.ohlcv.intervalElapsed(now):
            candle := inst.ohlcv.roll(now)
            inst.emitter.EmitCandle(candle)

Because the clock is global but every shard runs its own ticker, each shard checks the transition independently. To avoid P shards all firing the same close, the transition side-effects (auction clear, session events) are guarded so the first shard to observe the transition triggers global work once — or, more simply, the global close fan-out (§7) is driven by a single owner. (See §7 for the chosen mechanism.)

7. The global close fan-out (auction clear)

The SessionClock is global — all instruments close together — but each AuctionBook is per-instrument on its own shard. So the single Continuous→Closed transition must reach every instrument's shard to clear its auction.

Mechanism: a dedicated monitor owner (either shard 0, or a tiny separate monitor goroutine) owns clock.checkTransition. On the close transition it broadcasts a ClearAuction control message to every shard (one per shard, over the shard inbound channels as a control variant, or a dedicated control channel per shard). Each shard, on receiving it, iterates its own instruments and calls:

onClearAuction(now):
    for inst in instrs:
        ar := inst.auction.Clear(referenceFor(inst), inst.emitter)   // AuctionResult
        inst.emitter.EmitTrades(ar.Trades)                            // PUBLIC prints at P*
        for t in ar.Trades: inst.ohlcv.ingestTrade(t.Price, t.Qty)
        inst.emitter.EmitResult(wrapAuction(ar, /*brokerId per order*/))  // PRIVATE fills+cancels
        inst.emitter.EmitSession(SessionClosed)

This keeps each instrument's auction cleared on its own shard (single-writer preserved) while a single trigger (the global close) fans out to all. Using one monitor owner avoids P shards each independently deciding the market closed.

Cross-instrument note: with thousands of instruments this is a burst of clearing at one instant. Acceptable for the prototype; a future refinement could stagger clears. Listed in §11.

8. Session lifecycle — startup to shutdown
Exchange struct (top level):
  inbound   chan InboundMessage      // the one edge inbound
  public    chan TapeEvent           // the one edge public out
  private   chan EngineResult        // the one edge private out
  ingress   *Ingress
  shards    []*Shard                 // len P
  clock     *SessionClock            // global
  numShards int

Startup (session setup — driven by Layer 7):

Layer 7 provides the session schedule (phase boundaries) and, per instrument, its reference price (prior close / seed) and the rulebook (tick size, band width).
Build the global SessionClock from the schedule.
Create P shards, each with its inbound channel and the two shared outbound handles.
For each instrument: build its InstrumentState — Book, CircuitBand (NewCircuitBand(reference, bandWidth)), Matcher(book, band, emitter), AuctionBook, OHLCVAggregator, Emitter(instrument, public, private) — and insert it into the map on shard hash(instrument)%P.
Start ingress and all shard goroutines. Start the monitor owner (§7).

Steady state: brokers send InboundMessages; the flows in §9 run.

Shutdown: the final schedule boundary is PhaseClosed; after the close auction clears and SessionClosed is emitted, no more orders are accepted (the gate rejects on PhaseClosed). Goroutines drain and stop. (Persistence of end-of-session state is the deferred item in §11.)

Adding an instrument mid-life (if ever needed): build its InstrumentState, insert into the map on its hashed shard. No new goroutine, no edge change, no other instrument affected.

9. End-to-end flows
9.1 Flow A — a continuous BUY order, matched

Scenario: broker B1 submits a market buy for 60 units of ACME; the book has asks.

① Broker B1 builds InboundMessage{
     Kind: SubmitOrder, BrokerID: "B1", Instrument: "ACME",
     Submit: &FundedOrder{OrderID:"o-42", Participant:"p-7", Instrument:"ACME",
                          Side:Buy, Type:Market, Qty:60, MaxSpend: <reserved cash>} }
   → writes it to the exchange's one inbound Go channel.
   (B1 already reserved funds via its own Gate-4/L5 before sending — the exchange trusts this.)

② INGRESS reads the message, computes hash("ACME")%P = shard 2, forwards to shard 2's inbound.
   (No validation here — pure routing.)

③ SHARD 2 pulls the message, sees Kind==SubmitOrder, looks up instrs["ACME"] → InstrumentState.
   Calls handleSubmit(inst, "B1", order).

④ RunPipeline (gate, 1.4) on the order:
     checkStructural   → market order, qty 60>0, no price fields → pass
     checkSessionPhase → clock.Current()==PhaseContinuous → route=match, pass
     checkLiquidity    → market buy; book.Quote().HasAsk==true → pass
     checkTickGrid     → market has no price → pass
     checkCircuitBand  → market has no price → pass (clamp applies during the walk)
     stampSequence     → order.Seq = inst.admitSeq++   (time priority)
     routeOrder        → route==match → inst.matcher.Match(order)

⑤ MATCHER (1.2) walks the ask side best→worst:
     - band.PriceAllowed(levelPrice) checked before each print (clamp)
     - self-match skipped/cancelled per STP if participant matches
     - funded-cap: accumulates spend, capped by MaxSpend
     - each fill: Trade{Seq: emitter.Next(), Price: makerPrice, Qty, TakerOrder:"o-42",
                        MakerOrder:<maker>, participants...}; book.ReduceFront(level, qty)
     - last price ← final trade's maker price (written to inst.book quote)
   Returns MatchResult{Trades:[...], CancelledQty (if book emptied / capped), LastPrice...}
   Wrapped as EngineResult{Outcome:Matched, OrderID:"o-42", Match:&mr}.

⑥ SHARD stamps result.BrokerID="B1" and emits:
     PRIVATE:  emitter.EmitResult(result)  → EngineResult on the private chan (tagged B1)
     PUBLIC:   emitter.EmitTrades(mr.Trades)     → anonymized PublicTrade prints
               ohlcv.ingestTrade(each)           → folds into the 1-min candle
               EmitQuote(book.Quote())  (ask top changed) → PUBLIC quote update
               EmitDepth(book.TopN(10)) (depth changed)   → PUBLIC depth update

⑦ OUT:
     - Broker B1 reads the EngineResult off the private chan (keyed by OrderID "o-42"),
       runs Convert on the filled qty and Release on any capped/cancelled remainder.
     - All tape consumers read the PublicTrade / Quote / Depth events (no identity).
       OHLCV candle for ACME emits on the next minute roll.
9.2 Flow B — a LIMIT order during the closing auction (divert → clear)
① Broker B1 submits InboundMessage{Kind:SubmitOrder, BrokerID:"B1", Instrument:"ACME",
     Submit: FundedOrder{OrderID:"o-99", Side:Sell, Type:Limit, Qty:100, LimitPrice: <on-tick>}}.

②–③ INGRESS → shard 2 → handleSubmit.

④ RunPipeline:
     checkStructural  → pass
     checkSessionPhase→ clock.Current()==PhaseClosingAuction → route=auction, pass (continues)
     checkLiquidity   → route==auction → skipped
     checkTickGrid    → limit price on grid → pass
     checkCircuitBand → limit within band → pass
     stampSequence    → order.Seq stamped
     routeOrder       → route==auction → inst.auction.Add(order)  (collected, NOT matched)
   EngineResult{Outcome:Diverted, OrderID:"o-99", BrokerID:"B1"}.

⑤ SHARD: emitter.EmitResult(diverted)  → PRIVATE ack to B1 ("accepted, pending auction").
   No public event (nothing traded).

   … more orders collected the same way during the window …

⑥ At session close, the CLOCK's Continuous→Closed transition fires. The monitor owner
   broadcasts ClearAuction to all shards. Shard 2 runs onClearAuction for ACME:
     ar := inst.auction.Clear(reference, emitter)
        → four-level cascade finds P*; price-then-time allocation; unfilled cancelled
     EmitTrades(ar.Trades)  → PUBLIC prints, all at P* (the official close)
     ohlcv.ingestTrade(each) → close candle reflects P*
     EmitResult(wrapAuction(ar)) → PRIVATE fills (Convert) + cancels (Release) to brokers
     EmitSession(SessionClosed)  → PUBLIC session-closed event

⑦ OUT: brokers settle auction fills/cancels from the private stream; tape shows the
   closing prints and the close event.
9.3 Flow C — a CANCEL
① Broker B1 submits InboundMessage{Kind:CancelOrder, BrokerID:"B1", Instrument:"ACME",
     Cancel: &CancelRequest{OrderID:"o-99"}}.

②–③ INGRESS → hash("ACME")%P = shard 2 (same shard that owns o-99's book) → handleCancel.

④ inst.book.Cancel("o-99"):
     - index lookup → the resting node; O(1) unlink; TotalQty decremented; empty level dropped
     - returns (removed, found=true)

⑤ SHARD emits:
     PUBLIC:  EmitQuote/EmitDepth if the top/depth changed (a vanished bid/ask is real data)
     PRIVATE: EngineResult{Outcome:Cancelled, OrderID:"o-99", BrokerID:"B1",
                           CancelledQty: removed.Remaining}

⑥ OUT: Broker B1 reads the private result and runs Release on removed.Remaining
   (frees the reservation for the unfilled quantity). Tape reflects the book change.

   (If o-99 had already filled/cancelled, book.Cancel returns found=false and the shard
    emits Outcome:CancelRejected — a normal outcome, no state change, no public event.)
10. Concurrency model (summary)
Single writer per instrument: every instrument's state is owned by one shard and mutated only by that shard's serial loop → no locks anywhere in the components.
Parallelism across shards: different instruments on different shards run concurrently, sharing no mutable state.
The one cross-shard shared read: the global session phase, held as an atomic.Int32 in SessionClock — one writer (the monitor owner via checkTransition), many shard readers (Current()), lock-free.
Shared outbound channels, many writers: Go channels are safe for concurrent writers; per-instrument event ordering holds because one shard writes one instrument's events in sequence.
Two sequences, both per-instrument: admitSeq (order time priority) and the emitter's event Seq (event ordering) — distinct counters, neither shared across shards.
11. Limitations & deferred (of the exchange & the exchange↔broker relation)

Known and accepted for the prototype; none block the demo, and each is additive later:

No persistence of exchange internals. The book, quotes, sequences, and in-flight state live only in memory. A process crash loses the live session (and any in-flight EngineResults the broker hadn't yet applied, which can desync reservation bookkeeping). Persistence/crash-recovery for the exchange's own internals will be added after exchange development is complete — it is self-contained to the exchange and does not affect other layers, so deferring it is safe.
Multi-broker private routing is present-but-untested. BrokerID is carried and echoed (Rule 2), so routing each private EngineResult to its broker is a small egress step; the demo runs one broker. Public tape is already multi-broker (broadcast).
In-process only. Broker↔exchange are Go channels in one process. Splitting into separate processes needs a network transport + serialization, and — once cross-process — the fill stream loses Go's free ordered/exactly-once property and needs sequencing/ack/replay. The message-boundary discipline is what keeps that swap from touching internal logic.
Unmanaged backpressure. If a broker or tape consumer reads slowly, the shared outbound channels can fill and block the shard writing them, stalling matching for every instrument on that shard. No per-consumer buffering or drop policy yet.
Trust in broker correctness. Funding (Gate 4) and stops live in the broker; the exchange trusts every inbound order is funded and cannot catch an unfunded order.
Global session clock. All instruments share one schedule (open/close together). Per-instrument or per-broker hours would require making the clock non-global.
Auction-clear burst. The global close clears every instrument's auction at one instant; with thousands of instruments this spikes work at close. No staggering yet.
Cancel/modify are minimal. Cancel is a book pull; modify is cancel+resubmit. No in-place modify, no partial-cancel semantics beyond removing the whole resting order.