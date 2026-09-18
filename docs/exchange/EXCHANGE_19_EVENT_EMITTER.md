Exchange 1.9 Event Emitter — Implementation

Prerequisites: EXCHANGE_00_ARCHITECTURE.md (per-instrument sharding, the one-in/one-out edge), EXCHANGE_11_ORDERBOOK_QUOTE.md (QuoteState, Book, value types), EXCHANGE_12_MATCHING_CORE.md (Trade, MatchResult, SeqSource), EXCHANGE_14_ADMISSION_GATE.md (EngineResult), EXCHANGE_13_AUCTION_MATCHER.md (AuctionResult), EXCHANGE_18_SESSION_PHASE_CLOCK.md (phase transitions).

What 1.9 owns: the exchange's entire outbound voice. Every other component produces neutral records (a Trade, a MatchResult, an EngineResult, an AuctionResult, a phase transition) and talks to no one. 1.9 turns those into wire events and pushes them onto the two outbound channels. It also owns the per-instrument event sequence (the SeqSource the matcher/auction have been stubbing) and the OHLCV aggregator that emits candles continuously.

It is the last component built, because it consumes the outputs of all the others.

0. Transport reality (demo) — Go channels, one process

Broker and exchange are two goroutines in one process (architecture §2). So the two outbound "channels" are literally Go channels — in-memory, goroutine-to- goroutine, no TCP, no sockets, no serialization. The emitter writes Go values; the broker (and other in-process consumers) read them.

The "one-in / one-out connection edge" rule (architecture §6a) still holds: everything the exchange emits leaves on one public channel and one private channel. When the platform later splits broker and exchange into separate processes, these two Go channels are replaced by a network transport (WebSocket / gRPC stream) — and, because the emitter only ever sends values (never shares a pointer across the seam), that swap does not touch emission logic. Re-broadcasting the public tape to browsers is a Layer 6 concern built on top of the public channel; the exchange's job ends at putting tagged, sequenced events on it.

1. The two streams
Public tape (broadcast, anonymized): market truth anyone may see — trade prints, quote changes, depth, OHLCV candles, session events. Carries no participant identity. This is requirements "OHLCV data" + "trade-related market data."
Private stream (to the broker / order placer): per-order outcomes — fills, rests, cancels, rejections, auction results — each echoing OrderID. This is requirement "trade execution data," and anything the broker needs to run Convert/Release rides here. It reuses the existing EngineResult structure; no new private-event type.

One real event can produce both: a continuous trade emits one anonymized public print and (inside the EngineResult) private fills for each side.

2. The per-instrument sequence (resolves SeqSource)

Every emitted event carries a Seq so a consumer can order events and detect gaps. The sequence is per-instrument, not global:

Events for different instruments have no meaningful order between them; forcing a global counter would make it cross-shard shared state (contention) for no benefit.
Events for the same instrument must be ordered (a quote change, then a trade, then a depth update). Each instrument's emitter owns its own counter.

This per-instrument event sequence is a separate counter from the admission Seq (order time-priority, §1.4/1.1): one order can produce many events over its life, so they cannot share a counter. The matcher's and auction's SeqSource.Next() — used to stamp Trade.Seq — is this event sequence (a trade's Seq orders the trade event on the tape). SeqSource is therefore implemented by the emitter.

Because each instrument's emitter is owned by one shard worker and called only from that shard's serial loop (§6), the counter needs no lock, and per-instrument event order is guaranteed (each instrument's events are written to the shared channels in sequence by its own shard).

3. Public tape events

One tagged-union type on the single public channel, so the one-out rule holds while one channel carries every public kind.

go
type TapeKind int
const (
    KindTrade TapeKind = iota
    KindQuote
    KindDepth
    KindCandle
    KindSession
)

type TapeEvent struct {
    Seq        Seq          // per-instrument event sequence
    Instrument InstrumentID
    Kind       TapeKind
    // exactly one of the following is set, per Kind:
    Trade   *PublicTrade
    Quote   *QuoteState     // snapshot (from 1.1/1.7), emitted on change
    Depth   *DepthSnapshot
    Candle  *Candle
    Session *SessionEvent
}

// Anonymized trade print — NO participant ids on the public tape.
type PublicTrade struct {
    Price Ticks
    Qty   Qty
}

// Top-N depth per side (decision: N = 10). Lossy by design — the tape shows the top
// of book, not the entire ladder.
type DepthLevel struct {
    Price    Ticks
    TotalQty Qty
}
type DepthSnapshot struct {
    Bids []DepthLevel // top-N, best first
    Asks []DepthLevel // top-N, best first
}

type SessionEventKind int
const (
    SessionOpened SessionEventKind = iota
    AuctionWindowOpened
    SessionClosed
)
type SessionEvent struct { Kind SessionEventKind }
4. OHLCV aggregator (built inside the exchange)

OHLCV candles are derived from trades, bucketed by a fixed 1-minute interval (decision), and emitted continuously — a candle is emitted every interval even if no trades occurred (a flat candle at the last price), so the feed never goes silent. Building this inside the exchange (rather than emitting only raw trades and letting a consumer aggregate) is what makes "continuous per-instrument OHLCV" cheap and correct.

go
type Candle struct {
    Instrument InstrumentID
    Start      Time  // bucket start
    Open, High, Low, Close Ticks
    Volume     Qty
    Seq        Seq   // event sequence, stamped when emitted
}

type OHLCVAggregator struct {
    instrument InstrumentID
    interval   Duration // 1 minute
    curStart   Time     // current bucket start
    o, h, l, c Ticks
    volume     Qty
    hasTrade   bool     // any trade in the current bucket yet?
    lastClose  Ticks    // last known price, for flat candles when no trades
}

// ingestTrade folds one trade into the current candle. Called by the shard for each
// trade in a MatchResult / AuctionResult.
func (a *OHLCVAggregator) ingestTrade(price Ticks, qty Qty) {
    if !a.hasTrade {
        a.o, a.h, a.l = price, price, price
        a.hasTrade = true
    }
    if price > a.h { a.h = price }
    if price < a.l { a.l = price }
    a.c = price
    a.volume += qty
    a.lastClose = price
}

// roll finalizes the elapsed bucket, returns the candle to emit, and starts a fresh
// bucket. Called by the shard's per-second monitor when the interval has elapsed.
// If no trade landed in the bucket, emits a flat candle at lastClose (volume 0) so the
// feed is continuous.
func (a *OHLCVAggregator) roll(now Time) Candle {
    var c Candle
    if a.hasTrade {
        c = Candle{a.instrument, a.curStart, a.o, a.h, a.l, a.c, a.volume, 0}
    } else {
        c = Candle{a.instrument, a.curStart, a.lastClose, a.lastClose, a.lastClose, a.lastClose, 0, 0}
    }
    // reset for the next bucket
    a.curStart = now
    a.hasTrade = false
    a.volume = 0
    return c // Seq stamped by the emitter at EmitCandle
}

// intervalElapsed reports whether now has crossed into the next bucket.
func (a *OHLCVAggregator) intervalElapsed(now Time) bool { return now.Sub(a.curStart) >= a.interval }

The aggregator is per-instrument, in InstrumentState, on the shard. It is fed trades during matching and rolled by the same per-second monitor that drives the session clock (1.8) — so candle emission is time-driven and independent of order flow.

5. The emitter — the single choke point

One Emitter per instrument owns the event sequence and the two channel handles, and exposes the methods the shard calls. It is the only place internal records become external events.

go
type Emitter struct {
    instrument InstrumentID
    eventSeq   uint64              // per-instrument counter
    public     chan<- TapeEvent    // the one shared public channel (many instrument writers)
    private    chan<- EngineResult // the one shared private channel (to broker)
}

func (e *Emitter) next() Seq { e.eventSeq++; return Seq(e.eventSeq) }

// SeqSource implementation — matcher/auction stamp Trade.Seq from here.
func (e *Emitter) Next() Seq { return e.next() }

// ---- public tape ----

// EmitTrades pushes one anonymized print per trade (participant ids stripped).
func (e *Emitter) EmitTrades(trades []Trade) {
    for _, t := range trades {
        e.public <- TapeEvent{
            Seq: t.Seq, Instrument: e.instrument, Kind: KindTrade,
            Trade: &PublicTrade{Price: t.Price, Qty: t.Qty},
        }
    }
}

// EmitQuote pushes a quote snapshot (call only when the quote changed).
func (e *Emitter) EmitQuote(q QuoteState) {
    e.public <- TapeEvent{Seq: e.next(), Instrument: e.instrument, Kind: KindQuote, Quote: &q}
}

// EmitDepth pushes a top-N depth snapshot (call only when depth changed).
func (e *Emitter) EmitDepth(d DepthSnapshot) {
    e.public <- TapeEvent{Seq: e.next(), Instrument: e.instrument, Kind: KindDepth, Depth: &d}
}

// EmitCandle pushes an OHLCV candle (called on interval roll).
func (e *Emitter) EmitCandle(c Candle) {
    c.Seq = e.next()
    e.public <- TapeEvent{Seq: c.Seq, Instrument: e.instrument, Kind: KindCandle, Candle: &c}
}

// EmitSession pushes a session lifecycle event (from clock transitions).
func (e *Emitter) EmitSession(kind SessionEventKind) {
    e.public <- TapeEvent{Seq: e.next(), Instrument: e.instrument, Kind: KindSession,
        Session: &SessionEvent{Kind: kind}}
}

// ---- private stream ----

// EmitResult pushes the per-order outcome to the broker / order placer. Reuses the
// existing EngineResult (echoes OrderID; carries fills/rested/cancelled/rejection/
// diverted). No new type.
func (e *Emitter) EmitResult(r EngineResult) { e.private <- r }

Notes:

Trade.Seq is stamped by the matcher/auction when the trade is created (via Next()), so EmitTrades reuses it rather than re-stamping — the trade event's sequence is fixed at the moment the trade prints, keeping trade order and event order identical.
EmitQuote/EmitDepth are called by the shard only when the book actually changed the quote/top-N depth (the book's mutators already know this — 1.1 §6), so the tape isn't spammed with no-op snapshots.
Both channels have many writers (one emitter per instrument) and are Go channels, so concurrent writes from different shards are safe; per-instrument ordering holds because each instrument's events are written by its own single shard loop.
6. Emission map — who emits what, at which point

The shard loop (a worker goroutine's for-loop, detailed in the wiring doc) is the driver: it runs a component, takes the neutral result, and calls emitter methods. The event-relevant behaviour:

Trigger	Component (result)	Public	Private
Continuous order processed	Matcher 1.2 (MatchResult)	trade prints; quote update if changed; depth update if changed; each trade also fed to OHLCV ingestTrade	EngineResult (fills / rested / cancelled, echoing OrderID)
Order rejected at admission	Gate 1.4 (EngineResult{Rejected})	—	EngineResult (rejection)
Order diverted to auction	Gate 1.4 (EngineResult{Diverted})	—	EngineResult (diverted ack)
Interval boundary (per-second monitor)	OHLCV aggregator	candle (OHLCV)	—
Session close → auction clear	Auction 1.3 (AuctionResult)	auction trade prints at P*; final quote/depth; each fed to OHLCV	EngineResult-wrapped auction fills / cancels (echoing OrderID)
Phase transition	Clock wiring 1.8 (PhaseTransition)	session event (opened / auction-window / closed)	—

Pseudo-driver, continuous path:

result := runPipeline(order, ctx)              // EngineResult
emitter.EmitResult(result)                     // private -> broker
if result.Match != nil:
    emitter.EmitTrades(result.Match.Trades)    // public prints (Seq already stamped)
    for t in result.Match.Trades: ohlcv.ingestTrade(t.Price, t.Qty)
    if quoteChanged: emitter.EmitQuote(book.Quote())
    if depthChanged: emitter.EmitDepth(book.TopN(10))

Per-second monitor addition (same loop that drives the clock):

for inst in this shard's instruments:
    if inst.ohlcv.intervalElapsed(now):
        emitter(inst).EmitCandle(inst.ohlcv.roll(now))

The AuctionResult and EngineResult shapes carry per-order fills/cancels; wrapping auction fills into EngineResult for the private stream (so the broker treats continuous and auction fills identically) is a small mapping done by the shard when it calls EmitResult. If a gap shows up there, EngineResult is extended rather than a parallel private type introduced.

7. Ownership & concurrency (event-relevant sharding only)
One Emitter and one OHLCVAggregator per instrument, held in InstrumentState, owned by that instrument's shard worker. Called only from that shard's serial loop → no lock on the sequence counter or the aggregator.
The two outbound channels are shared across all instruments/shards (Go channels accept many writers). Ordering guarantee is per-instrument, which is the only ordering that matters.
(Full shard/ingress/egress wiring — how the shard loop is structured, how the per-second monitor is shared with the clock, how the global close fans out to each instrument's auction — is the dedicated wiring doc, not here.)
8. Invariants 1.9 must hold
Public events carry no participant identity — PublicTrade has price/qty only.
Private events always echo OrderID — via EngineResult.
Every event carries a per-instrument Seq; per-instrument event order equals emission order.
Trade.Seq is stamped once, at trade creation, and reused on the public print — trade order and tape order agree.
OHLCV is continuous — a candle every interval per instrument, flat at last price when no trades.
Quote/depth emitted only on actual change — no no-op snapshots.
The private stream reuses EngineResult — no parallel private-event type.
The emitter is the only component that writes the outbound channels — every other component returns neutral records.
9. Deferred / handed elsewhere
Network transport & per-consumer fan-out (WebSocket to browsers, per-instrument subscription filtering) — Layer 6, built on top of the public channel. The exchange emits everything tagged; Layer 6 routes.
Multiple candle intervals (5-min, etc.) — the prototype emits 1-min; additional intervals would be extra aggregators or downstream re-bucketing.
The shard loop / ingress / egress wiring — the dedicated sharding/wiring doc.
Mapping AuctionResult → EngineResult for the private stream — a small shard- side adaptation; documented with the wiring.