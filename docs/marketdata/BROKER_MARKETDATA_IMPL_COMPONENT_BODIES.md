Broker Market-Data Component — Internal Bodies (Implementation Design)

Altitude. Implementation design, not production code: struct shapes, method signatures, concrete SQL/field access, and internal logic as flow/pseudocode — bodies sketched as flow, per the project's standing rule for *_IMPLEMENTATION_DESIGN.md docs.

Where this sits. This continues the base pair from the previous thread:

BROKER_MARKETDATA_DESIGN.md — the conceptual design + decision registry D-1…D-21.
BROKER_MARKETDATA_IMPLEMENTATION_DESIGN.md — the code-level counterpart (CandleStore, rollup, HTTP, routing).

That pair deliberately left three things as "next / open". This document closes all three, grounded against fresh exchange + broker source and the L7 docs read this session:

Closed here	Was, in the base pair
Ingestor assembly (loop body) — §1	IMPL §5: "the full for ev := range tape assembly … is the next implementation step, not yet written."
MarketSnapshot concurrency pick — §2.0	DESIGN §10 Open / IMPL §6.3: "RWMutex-map vs atomic-ptr-per-instrument — not picked."
FanOut body — §3	DESIGN §10 Next: "FanOut and MarketSnapshot struct bodies — flows designed; method bodies remain."
Exchange-clock tz → UTC conversion — §1.4	DESIGN/IMPL Parked: "what tz emit.Candle.Start carries → the UTC conversion at Insert."

Two of these also carry corrections that the L7 docs / fresh source forced; each is flagged [CORRECTS] inline.

Grounding (fresh source, this session)
exchange/emit/tape.go — emit.TapeEvent{Seq, Instrument, Kind, Trade/Quote/Depth/Candle/Session *…}; the five Kind consts (KindTrade/KindQuote/KindDepth/KindCandle/KindSession); payloads are pointers, exactly one non-nil per Kind. PublicTrade{Price, Qty}; DepthSnapshot{Bids,Asks []DepthLevel{Price,TotalQty}}; Candle{Instrument,Start,O/H/L/C,Volume,Seq}; SessionEvent{Kind} with SessionOpened/AuctionWindowOpened/SessionClosed.
exchange/emit/ohlcv.go — the exchange's OHLCVAggregator.Roll emits a closed candle every interval, continuous (a flat candle at lastClose on a zero-trade minute). Candle.Start is set from the shard's tick time.
exchange/book/quote.go — QuoteState{BestBid,BidQty,HasBid, BestAsk,AskQty,HasAsk, LastPrice,HasLastPrice} (returned by value across the seam).
exchange/wire/shard_run.go + handle_tick.go — case now := <-shard.ticker: handleTick(now) → ohlcv.Roll(now); the ticker is a Go time.Ticker, now is a real wall-clock instant (see §1.4).
broker/funnel/settlement_consumer.go — func (b *Broker) runSettlementConsumer(private <-chan contract.EngineResult): the exact house pattern the ingestor mirrors (one goroutine per instance, slog start/exit, for range self-terminating on channel close, skip-and-log never panic).
broker/funnel/mkt_cache.go — the real MarketPriceCache interface, keyed (ExchangeID, InstrumentID) (see §2.6 + the wiring doc).
contract/values.go — InstrumentID = string, Ticks = int64, Qty = int64, Seq = uint64.

Package name marketdata is provisional (placement fork D-2). Decision this session: a separate package (broker/marketdata) rather than folding into funnel — but content is what's load-bearing; the package clause is not.

1. Ingestor assembly (closes IMPL §5)
1.0 House pattern it mirrors

The ingestor is the public-tape sibling of the built runSettlementConsumer (which drains the exchange's private EngineResult stream). Same shape, per broker/exchangeseam/CLAUDE.md (the broker↔exchange wire-seam constitution), which names the Public-tape consumer as Point 3, deferred:

one goroutine per exchange instance, the sole consumer of that instance's tape;
self-terminating: the for range ends when the exchange closes the channel in Stop();
slog.Info on start and on exit;
data problems are skip-and-log, never panic (killing the loop would halt all market data for that instance);
no per-event recover, no stop flag — teardown is channel-close only.
1.1 Signature (reconciled with WireInstance — gains exch)
func runMarketDataIngestor(
    tape     <-chan emit.TapeEvent,   // this instance's Exchange.Public read-end; sole consumer
    exch     ExchangeID,              // instance id; stamped on receipt (NOT on the wire) — see wiring doc
    market   string,                  // 'IN'/'US' — store rows + snapshot keys + tz clock
    mode     Mode,                    // canonical | onDemand — persist-or-not, set once
    store    *CandleStore,            // shared durable layer (Stage 1)
    snapshot *MarketSnapshot,         // this instance's retained live state (§2)
    rollup   *streamRollup,           // this instance's forming-bar state (IMPL §4)
    fanout   *FanOut,                 // this instance's live distributor (§3)
)

All of these are injected at wire time by WireInstance (see BROKER_MARKETDATA_IMPL_WIRING.md). The base IMPL doc's §5.1 injected set is unchanged except exchangeID is now first-class (needed for the account-side cache registry keyed by (ExchangeID, InstrumentID)).

1.2 Ingestor-local state (single-writer, no lock)

The streaming-rollup forming map[instrument]map[Timeframe]*Candle (IMPL §4.1) is touched only by this goroutine, so it needs no lock — same discipline as the exchange's own OHLCVAggregator. MarketSnapshot and FanOut carry their own synchronization (§2, §3); the ingestor just calls into them.

1.3 Loop + teardown
runMarketDataIngestor(...):
    slog.Info("marketdata: ingestor started", exch, market, mode)
    for ev := range tape:          // exits when exchange Stop() closes Public
        route(ev)                   // the demux, §1.5
    onClose()                       // teardown, §1.8
    slog.Info("marketdata: ingestor exited (Public closed)", exch, market)
1.4 convert() — the tz question, CLOSED: no conversion step [CORRECTS parked item]

Trace (this session): emit.Candle.Start is produced by ohlcv.Roll(now), where now is the value off the shard's Go time.Ticker — i.e. a real absolute instant from the runtime clock (example/main.go seeds OHLCVStart from time.Now()). A Go time.Time is an absolute instant plus a display location; the instant is unambiguous regardless of the location tag.

Consequence: when pgx writes a time.Time into the TIMESTAMPTZ column, Postgres normalizes to the UTC instant automatically. So the ingestor performs no timezone conversion at insert — the parked "what tz is Start?" question dissolves. convert() is a plain field copy:

func toStoreCandle(market string, c *emit.Candle) Candle
  → Candle{ Market:market, Instrument:c.Instrument, Start:c.Start,   // instant passed through as-is
            Open:c.Open, High:c.High, Low:c.Low, Close:c.Close, Volume:c.Volume }

emit.Candle.Seq is dropped (meaningless once stored). End is not set here (derived on read, IMPL §1.4). Country-local rendering still happens only at the HTTP read edge (UTC → market tz), exactly as DESIGN §2 locks — unaffected.

Caveat worth recording: the stored instant is faithful regardless, but it lines up with the intended Indian session wall-clock only because the exchange runs on real time (time.Now()). If a future simulated clock ever stamps non-real instants, the stored value is still internally consistent; nothing in the ingestor changes.

1.5 Demux (all five emit kinds, Model-A routing)

ev is emit.TapeEvent; exactly one payload pointer is non-nil per Kind:

switch ev.Kind:

case emit.KindTrade:           // ev.Trade = *emit.PublicTrade{Price, Qty}
    snapshot.SetLast(ev.Instrument, ev.Trade.Price)         // lastPrice (+ seed sessionOpen if unset & phase open)
    fanout.Publish(snapshotPacket(exch, market, ev.Instrument, snapshot.Get(ev.Instrument)))

case emit.KindQuote:           // ev.Quote = *book.QuoteState (Has* flags load-bearing)
    snapshot.SetQuote(ev.Instrument, *ev.Quote)
    fanout.Publish(snapshotPacket(...))

case emit.KindDepth:           // ev.Depth = *emit.DepthSnapshot{Bids,Asks []DepthLevel{Price,TotalQty}}
    snapshot.SetDepth(ev.Instrument, *ev.Depth)
    fanout.Publish(snapshotPacket(...))

case emit.KindCandle:          // ev.Candle = *emit.Candle
    c := toStoreCandle(market, ev.Candle)
    if mode == canonical: store.Insert(ctx, c)              // persist-then-forward (provisional); err → log, continue
    for bar in rollup.Ingest(c):                            // 1m passthrough; 5/30/60m on boundary (IMPL §4)
        fanout.Publish(candlePacket(exch, market, bar))

case emit.KindSession:         // ev.Session = *emit.SessionEvent{Kind}
    if ev.Session.Kind == emit.SessionClosed:
        for bar in rollup.FlushAll(): fanout.Publish(candlePacket(exch, market, bar))  // final partial 30/60m bars
    snapshot.ApplySession(ev.Session.Kind)                  // INSTANCE-WIDE phase (§2.5)
    fanout.Publish(snapshotPacket(...))                     // session folds into snapshot (wire protocol doc, decision (a))

Model-A fact this nails: trades touch only the snapshot's lastPrice — the forming bar is not built from trades. Candles come solely from KindCandle, which the exchange emits continuously (even a flat zero-trade minute), so the rollup stream never has mid-session holes.

1.6 Ordering + degrade rules
Persist-then-forward (candles): store.Insert first, then fanout.Publish. Provisional (D-15) — may flip to forward-first/async if the live feed feels slow.
Insert failure → slog.Error + forward anyway (a missing row is the already-accepted "crash gap," not a hard stop).
fanout.Publish is non-blocking (§3.2) — the drain never stalls on a slow socket.
No panic on data issues — skip-and-log, mirroring the settlement consumer.
1.7 Session handling — instance-wide phase (decision this session)

ApplySession sets one phase flag for the whole instance (the exchange clock is global — EXCHANGE_10_WIRING_SHARDING.md (the exchange wiring/sharding doc): all instruments open/close together). But:

sessionOpen (the day-change base price) stays per-instrument, seeded from each instrument's first trade after SessionOpened.
SessionClosed triggers a global forming-bar flush across all instruments — the only way a mid-bucket final 30m/60m bar closes.

So: phase = instance-wide; open-price = per-instrument.

1.8 Destinations, teardown, and the B5 future sink

Three internal destinations today: CandleStore (candle, canonical), MarketSnapshot (trade/quote/depth/session), FanOut (both packet types). On teardown (onClose), beyond letting the forming/snapshot state fall out of scope, the ingestor must deregister this instance from the shared cache registry — cacheRegistry.remove(exch) — so the account side stops reading a dead snapshot (detailed in the wiring doc).

Future sink (not built): BROKER_DESIGN_TRACKER.md (the broker responsibility tracker) defines this component as B10 ("Tape relay & fan-out") and says it must also feed the stop-watcher (B5). No stop-watcher exists in the code today (B5 NOT STARTED), so there is nothing to wire — a seam to add a third fan-out target later, not designed now.

2. MarketSnapshot body (closes the concurrency pick)
2.0 Decision: concurrency = (B) atomic copy-on-write [closes DESIGN §10 Open]

Profile: one writer (the ingestor), frequent writes (every Trade/Quote/Depth), occasional reads (account BestAsk/LastPrice at order-place; WS join-seed), and the state contains slices (depth.Bids/Asks). The slice is the tiebreaker: under a plain RWMutex, a reader handing out depth must deep-copy the slices (a shallow copy shares the backing array → torn read while the writer swaps).

Chosen: (B) — each instrument holds an atomic.Pointer[instSnap]; the writer builds a new immutable instSnap with the one changed field and Stores it; readers Load and read freely. Lock-free reads, zero per-read copy, and the slice hazard vanishes (the writer never mutates a published snap). Cost = one small alloc per update. Keeps the account hot path (BestAsk at order-place) non-blocking. (A) RWMutex-map was the alternative; (B) wins on the slice + hot-read grounds.

2.1 [CORRECTS base IMPL §6] Instruments are tape-learned, so the map GROWS at runtime

The base IMPL doc implied a fixed per-instrument map. But BROKER_MARKET_DATA_L7_EXPECTATIONS.md (the component's contract toward L7) E3 is explicit: L7 does not provide instrument identities; the component learns them from the tape. WireInstance receives no instrument list. So the instrument map grows as new instruments first appear on the tape, which rules out a fixed lock-free map.

Resolution (stays within the CoW model): the map itself is an atomic.Pointer to an immutable map[InstrumentID]*atomic.Pointer[instSnap]. A never-before-seen instrument triggers a copy-the-map-and-swap (rare — once per instrument, at session start); per-instrument field updates stay the per-instrument pointer swaps. Reads stay lock-free. Single writer (the ingestor) means the map-grow read-modify-swap has no competing writer.

2.2 Struct
MarketSnapshot struct {
    exch  ExchangeID                                           // this snapshot's one instance (stamped at wire)
    phase SessionPhase                                          // INSTANCE-WIDE; writer-only plain field (§2.5)
    inst  atomic.Pointer[instMap]                               // immutable map, copy-on-insert (§2.1)
}
type instMap = map[contract.InstrumentID]*atomic.Pointer[instSnap]

// instSnap is IMMUTABLE once stored — replaced, never mutated in place.
instSnap struct {
    lastPrice   contract.Ticks ; hasLast bool                   // from Trade.Price
    quote       book.QuoteState                                  // value copy; carries its own Has* flags
    depth       emit.DepthSnapshot                                // fresh slices from the exchange each Depth event
    sessionOpen contract.Ticks ; hasOpen bool                    // per-instrument day-change base
}
2.3 Writer API — ingestor goroutine only (single writer, CoW)
clone(i) → *instSnap:   p := load(i); cp := *p; return &cp        // shallow; reuses depth slice unless replaced
slot(i)  → *atomic.Pointer[instSnap]:                             // get-or-create the per-instrument slot
    m := inst.Load(); if s,ok := m[i]; ok: return s
    m2 := copy(m); m2[i] = newPtr(&instSnap{}); inst.Store(m2); return m2[i]   // copy-on-insert (rare)

SetLast(i, price):   s := clone(i); s.lastPrice, s.hasLast = price, true
                     if phase == open && !s.hasOpen: s.sessionOpen, s.hasOpen = price, true
                     slot(i).Store(s)
SetQuote(i, q):      s := clone(i); s.quote = q;  slot(i).Store(s)
SetDepth(i, d):      s := clone(i); s.depth = d;  slot(i).Store(s)   // d is the exchange's fresh value
ApplySession(kind):  phase = map(kind)                            // instance-wide, plain field
                     if kind == SessionOpened:                     // reset per-instrument day base
                        for i in inst.Load(): s := clone(i); s.hasOpen=false; slot(i).Store(s)

Single writer ⇒ plain Load → copy → Store, no CAS loop.

2.4 Reader API — account pull + WS seed/push (lock-free, by-value)
BestAsk(i)  → p,ok := inst.Load()[i]; if !ok { return 0,false }; s := p.Load()
              if s.quote.HasAsk { return s.quote.BestAsk, true }; return 0,false
LastPrice(i)→ p,ok := inst.Load()[i]; if !ok { return 0,false }; s := p.Load()
              if s.hasLast { return s.lastPrice, true }; return 0,false
Get(i)      → p,ok := inst.Load()[i]; if !ok { return nil }; return p.Load()   // immutable *instSnap for WS seed + publish

Readers Load and read — no copy, no lock. Get returns the immutable pointer; the WS join-seed and the ingestor's publish both consume it without copying (safe — never mutated). Honors the exchange seam rule (EXCHANGE_11_ORDERBOOK_QUOTE.md, the order-book/quote doc: consumers get emitted values, never a pointer into live book state) and the broker read-surface "by-value copy" rule.

2.5 Session fields
phase is touched only by the ingestor (written in ApplySession, read in SetLast to gate sessionOpen seeding) → a plain field, no atomic. If a cross-goroutine reader ever needs "is market open," it gets its own atomic then; today nothing off-goroutine reads it.
sessionOpen/hasOpen are per-instrument (on instSnap), reset across all instruments on SessionOpened.
2.6 MarketPriceCache mapping

The per-instance snapshot exposes instrument-keyed BestAsk(i)/LastPrice(i) above. The real seam broker/funnel/mkt_cache.go is keyed (ExchangeID, InstrumentID) — the registry that adds the exch dimension and satisfies the interface is wiring, detailed in BROKER_MARKETDATA_IMPL_WIRING.md. Miss semantics (straight from that file): BestAsk miss → market-buy rejects "no market price"; LastPrice miss → A9/A14 mark degrades to MarkAvailable:false, position still ships. lastPrice source = Trade (every print); quote.LastPrice treated as redundant (DESIGN §7.2).

3. FanOut body (closes the FanOut body)
3.0 Two corrections from fresh source [CORRECTS]
No house RWMutex-registry to mirror. The base DESIGN §8.4 said this "matches the house RWMutex-registry pattern already in the codebase." Fresh grep: the broker has no such pattern — its only concurrency primitive is the shard inbox channels. FanOut's lock-based registry is a new pattern for this codebase (not wrong, just not mirroring anything).
subKey drops ExchangeID. The base IMPL §7.1 keyed subKey by {ExchangeID, Instrument, Stream, TF}. But per BROKER_MARKET_DATA_L7_EXPECTATIONS.md §1, FanOut is one-per-instance, so a single FanOut only ever has one ExchangeID feeding it → ExchangeID in its subKey is redundant. Reduced to {Stream, Instrument, TF}. (ExchangeID still rides on outbound packets and is the account-side cache key — it just isn't needed inside a per-instance FanOut's inventory.)
3.1 Struct
subKey struct { Stream stream; Instrument contract.InstrumentID; TF Timeframe }   // Stream ∈ {candle, snapshot}; TF zero for snapshot

FanOut struct {
    mu     sync.RWMutex
    subs   map[subKey]map[*Conn]struct{}
    byConn map[*Conn][]subKey                 // reverse index → O(1) disconnect
    snap   *MarketSnapshot                     // read-only, for the snapshot join-seed
}

Conn struct {
    buf    chan outPacket    // bounded per-connection outbound buffer
    lagged atomic.Bool       // tripped on a drop; the write goroutine acts on it
    // ws socket handle + the two goroutines live on the WS side
}
3.2 Publish — ingestor goroutine, hot path, non-blocking
func (f *FanOut) Publish(p outPacket)
    k := subKey{p.Stream, p.Instrument, p.TF}
    f.mu.RLock()
    for c := range f.subs[k]:
        select { case c.buf <- p:  default: c.lagged.Store(true) }   // non-blocking; full → drop + flag
    f.mu.RUnlock()

N ingestors (across instances/FanOuts) Publish in parallel; within one FanOut, RLock lets concurrent publishes proceed. Never blocks on a slow socket.

3.3 Subscribe / Unsubscribe / Disconnect — WS read goroutine, write-lock (rare)
func (f *FanOut) Subscribe(c *Conn, keys []subKey)                 // batch — one Lock covers all inserts
    f.mu.Lock(); for k in keys: f.subs[k][c]={}; f.byConn[c] += k; f.mu.Unlock()
    for k in keys where k.Stream == snapshot:                       // R2: snapshot seeds, candle does NOT
        if s := f.snap.Get(k.Instrument); s != nil:
            nonBlockingSend(c.buf, snapshotSeed(k, s))

func (f *FanOut) Unsubscribe(c, keys): mu.Lock(); delete each (k,c) from subs + prune byConn; mu.Unlock()
func (f *FanOut) Disconnect(c): mu.Lock(); for k in byConn[c]: delete(subs[k],c); delete byConn[c]; mu.Unlock(); close(c.buf)

Switch instrument/timeframe = Unsubscribe + Subscribe. Teardown purges via the reverse index — nothing durable to settle.

3.4 Per-connection goroutines + the buffer handoff

A WS connection is full-duplex and concurrent writes to one conn are illegal (gorilla). So two goroutines per connection:

readLoop(c):   for { msg := conn.ReadMessage(); parse subscribe/unsubscribe → FanOut.Subscribe/Unsubscribe }
               on read error → FanOut.Disconnect(c); return

writeLoop(c):  for p := range c.buf:                               // SOLE socket writer
                   if c.lagged.Load() && overThreshold: FanOut.Disconnect(c); return   // slow-client eviction
                   set write deadline
                   if err := conn.WriteMessage(encode(p)); err != nil: FanOut.Disconnect(c); return

The handoff chain: ingestor → Publish drops into c.buf (non-blocking) → writeLoop drains c.buf → WriteMessage to socket. The buffer exists precisely because all pushes (from the ingestor) must be serialized through one writer per connection. writeLoop also owns write deadlines + periodic ping (gorilla control frames), since it is the only writer. When Disconnect closes c.buf, the for range ends → writeLoop returns.

3.5 Transport — one mux, two routes
mux.Handle("GET /candles", candleHandler)   // history (base IMPL §3)
mux.Handle("GET /ws",      wsHandler)        // wsHandler: Upgrader.Upgrade(w,r) hijacks → spawn readLoop+writeLoop → block

Both on the same stdlib net/http mux and port — consistent with the broker's existing handlers (order/http.go, reads/* all use stdlib net/http; no router lib).

3.6 Slow-client policy + WS library
Slow client: buffer-full drops; a snapshot drop is harmless (next snapshot supersedes); a candle drop is a chart gap → lagged past a threshold → disconnect → client reconnects and re-fetches history via GET /candles, then resumes live (clean rehydrate, no WS backfill).
WS library: gorilla/websocket assumed (the upgrade/deadline/ping/close semantics above are its model). Still the one open build-time pick; no WS dependency exists in go.mod yet, so this adds the first.
Status after this doc

Closed: ingestor assembly, MarketSnapshot concurrency (= B atomic CoW), the tape-learned-map correction, FanOut body, the subKey-drops-exch correction, the no-house-pattern correction, and the exchange-clock-tz question (no conversion).

Still open (build-time, carried in the base IMPL §8): WS library final pick (gorilla); End population site (recommend in RangeRead1m); package placement (decided separately as its own marketdata package); persist timing (persist-then-forward default).

Covered elsewhere: the wiring/WireInstance lifecycle and the three L7-doc corrections → BROKER_MARKETDATA_IMPL_WIRING.md. The WS JSON contract → BROKER_MARKETDATA_WS_WIRE_PROTOCOL.md.