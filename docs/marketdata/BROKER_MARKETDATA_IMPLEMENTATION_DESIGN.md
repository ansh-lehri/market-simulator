Broker Market-Data Component — Implementation Design

Altitude of this document. Implementation design, not production code. It gives: struct shapes, method signatures, concrete SQL, and internal logic as flow/pseudocode — the load-bearing bits — and deliberately stops short of full working Go. Bodies are sketched as flow. (This is the project's standing rule for *_IMPLEMENTATION_DESIGN.md docs.)

Companion: BROKER_MARKETDATA_DESIGN.md (the conceptual design + decision registry D-1…D-21). Scope is this thread only. Package name marketdata is provisional (placement fork, D-2).

Grounding conventions (from fresh source, this thread)
Postgres is pgx / *pgxpool.Pool (connectivity/db/postgres.go), not database/sql. Use pool.Exec/Query/QueryRow(ctx, …).
Durable store conventions (broker/store/*, esp. trades.go): ctx first; SQL as package consts at top; a scanXxxRow helper; defer rows.Close(); check rows.Err(); sentinel errors wrapped fmt.Errorf("…: %w (%v)", ErrSentinel, err); slog on both ok/err paths; pgx.ErrNoRows only on QueryRow.
HTTP conventions (broker/order/http.go): errorBody{Error string}; writeJSON(w,status,payload,path); writeError(w,status,msg,path); recoverPanic(w,path) as the first defer in ServeHTTP; stdlib net/http, routes registered on one mux.
Value types (contract/values.go): InstrumentID = string, Ticks = int64, Qty = int64, Seq = uint64 (aliases).
Tape types (exchange/emit/tape.go): emit.TapeEvent union; emit.Candle.Start time.Time; book.QuoteState; DepthSnapshot/DepthLevel{Price, TotalQty}.
1. CandleStore (Stage 1 — durable 1-min OHLCV)
1.1 Types
// store-boundary row (type B); also what the rollup engine folds.
Candle struct {
    Market     string
    Instrument contract.InstrumentID
    Start      time.Time    // bucket start; stored UTC (TIMESTAMPTZ)
    End        time.Time    // DERIVED, never stored (Start + grain, exclusive)
    Open, High, Low, Close contract.Ticks
    Volume     contract.Qty
}

CandleStore struct { pool *pgxpool.Pool }

func NewCandleStore(pool *pgxpool.Pool) *CandleStore
// package-local sentinel; infra/driver only. Empty read is NOT this.
var ErrLoadFailed = errors.New("broker/marketdata: durable load failed")
1.2 Schema (migration — to be authored; not in current codebase)
sql
CREATE TABLE candles (
    market      TEXT        NOT NULL,
    instrument  TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,   -- store UTC
    open        BIGINT      NOT NULL,
    high        BIGINT      NOT NULL,
    low         BIGINT      NOT NULL,
    close       BIGINT      NOT NULL,
    volume      BIGINT      NOT NULL,
    PRIMARY KEY (market, instrument, timestamp)   -- only index; also the range index
);
1.3 SQL (package consts)
sql
-- insertCandleSQL (idempotent; a re-delivered minute is a no-op)
INSERT INTO candles (market, instrument, timestamp, open, high, low, close, volume)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (market, instrument, timestamp) DO NOTHING

-- selectRange1mSQL (half-open [from,to); keyset-forward on timestamp)
SELECT market, instrument, timestamp, open, high, low, close, volume
FROM candles
WHERE market = $1 AND instrument = $2
  AND timestamp >= $3 AND timestamp < $4
  AND ($5::timestamptz IS NULL OR timestamp > $5)
ORDER BY timestamp ASC
LIMIT $6

Note the cast is ::timestamptz (column is tz-aware), vs the account store's ::timestamp only where its column is naive.

1.4 Signatures + flow
func (s *CandleStore) Insert(ctx, c Candle) error

Flow: pool.Exec(ctx, insertCandleSQL, c.Market, c.Instrument, c.Start, c.Open, c.High, c.Low, c.Close, c.Volume); err → slog.Error + wrap ErrLoadFailed; else nil. Conflict = 0 rows, no error. Canonical-only is the ingestor's decision, not here. (c.End is not persisted.)

func (s *CandleStore) RangeRead1m(ctx, market string, inst contract.InstrumentID,
        from, to time.Time, afterTS *time.Time, limit int) ([]Candle, error)

Flow:

limit <= 0 → return ([]Candle{}, nil) (no SQL).
map afterTS: nil → SQL NULL arg; else *afterTS.
pool.Query(ctx, selectRange1mSQL, market, inst, from, to, afterArg, limit); query err → wrap ErrLoadFailed.
defer rows.Close(); loop scanCandleRow → append; check rows.Err() → wrap.
set End = Start.Add(1*time.Minute) per row (derived here, or left to caller — pick one place; recommend here so every reader gets it).
empty rows → ([]Candle{}, nil) (legit gap, not error).
No store-side cap beyond the passed limit (the rollup engine owns the cap).
func scanCandleRow(rows pgx.Rows) (Candle, error)   // Scan order mirrors the SELECT column list exactly
2. Rollup engine (Stage 3 — shared by HTTP + live)
2.1 Timeframe
type Timeframe int   // minute count: TF1m=1, TF5m=5, TF30m=30, TF60m=60
func ParseTimeframe(s string) (Timeframe, bool)   // "5m"→TF5m; off-allowlist → (0,false)

Allowlist = {1,5,30,60}. Off-list → caller returns 400 (HTTP) / nack (WS).

2.2 Local-tz bucket alignment
func bucketStart(ts time.Time, tf Timeframe, loc *time.Location) time.Time

Flow: convert ts into loc (market tz from the registry §5), truncate to tf minutes in that local wall-clock, return. 1m → pass-through. Load-bearing for 30m/60m under IST (+5:30).

2.3 Market → IANA registry
func tzFor(market string) (*time.Location, bool)   // "IN"→Asia/Kolkata, "US"→America/New_York

Same allowlist shape as bringup/country.go. Readers: the rollup engine (align + serialize) and the HTTP handler (parse inbound local→UTC, emit timezone).

2.4 The fold
func RollUp(rows []Candle, tf Timeframe, loc *time.Location) []Candle

Flow (rows ASC, one instrument):

if tf == TF1m: return rows                 // pass-through
cur := nil
out := []
for m in rows:
    b := bucketStart(m.Start, tf, loc)
    if cur == nil || cur.Start != b:
        if cur != nil: out = append(out, *cur)
        cur = &Candle{ Market:m.Market, Instrument:m.Instrument,
                       Start:b, End:b.Add(tf.dur()),
                       Open:m.Open, High:m.High, Low:m.Low, Close:m.Close, Volume:m.Volume }
    else:
        cur.High = max(cur.High, m.High)
        cur.Low  = min(cur.Low,  m.Low)
        cur.Close = m.Close
        cur.Volume += m.Volume
if cur != nil: out = append(out, *cur)
return out

Empty bucket never created (gap). Partial bucket aggregates present minutes. Returns completed bars only (forming minute never in input).

3. HTTP history handler
3.1 Query + response types
CandleQuery struct {
    Market string; Instrument contract.InstrumentID; TF Timeframe
    From, To time.Time; AfterTS *time.Time; Limit int
}

CandlePage struct { Candles []Candle; NextCursor *string }   // engine output (pure)

// wire envelope (handler builds this)
CandleResponse struct {
    Market, Instrument, Timeframe, Timezone string
    From, To string                 // naive-local ISO echoes
    Candles []BarDTO
    NextCursor *string
}
BarDTO struct { Start, End string; Open, High, Low, Close int64; Volume int64 }   // times naive-local ISO; no unit
3.2 Engine method
func (e *ReadEngine) QueryCandles(ctx, q CandleQuery) (CandlePage, error)

Flow:

rowLimit := q.Limit*int(q.TF) + oneBucketSlack
raw := store.RangeRead1m(ctx, q.Market, q.Instrument, q.From, q.To, q.AfterTS, rowLimit)
loc,_ := tzFor(q.Market)
bars := RollUp(raw, q.TF, loc)
// paging / trailing-partial-bucket:
full := len(raw) == rowLimit
if full && len(bars) > 0:
    drop := bars[len(bars)-1]                 // possibly-incomplete trailing bucket
    bars = bars[:len(bars)-1]
    cursor := ptr(firstMinuteOf(drop))         // 1-min ts = drop.Start (its first minute)
else:
    cursor := nil                              // short batch → last bar complete
if len(bars) > q.Limit: bars, cursor = trimTo(q.Limit, bars)   // also sets cursor to boundary
return CandlePage{bars, cursor}

(firstMinuteOf for an aligned bucket is its Start; the next read uses it as afterTS, exclusive.) Keep this paging logic in the engine; the handler only maps types.

3.3 Handler (ServeHTTP) — durable-read family
defer recoverPanic(w, path)
q, err := parseCandleQuery(r)          // validation → writeError(400): missing market/instrument,
                                       //   ParseTimeframe false, bad/empty times, from>=to
                                       //   (parse From/To/cursor naive-local → tzFor → UTC)
ctx, cancel := context.WithTimeout(r.Context(), cfg.ReadTimeout); defer cancel()
page, err := engine.QueryCandles(ctx, q)
switch {
case errors.Is(err, ErrLoadFailed):    // incl. ctx timeout surfaced as load-fail
    writeError(w, 503, "...", path)     // NOT 504
case err == nil:
    writeJSON(w, 200, toResponse(q, page, loc), path)   // empty candles[] still 200
}

toResponse: convert each bar UTC→local→naive ISO; set Timezone, echo Market/Instrument/Timeframe/From/To. Depends on a small candleReader port (satisfied by ReadEngine) for test stubs (mirrors brokerPort/storePort).

Route registration: mux.Handle("GET /candles", h) alongside GET /ws. Config{ReadTimeout} — rides the parked wiring.

4. Streaming rollup (live, Model A)

Ingestor-local, single-writer (no lock) — only the ingestor touches it.

4.1 State
forming map[contract.InstrumentID]map[Timeframe]*Candle
4.2 Fold one closed 1-min candle
func (r *streamRollup) Ingest(c Candle) (finalized []Candle)

Flow:

out := []
for tf in {TF1m,TF5m,TF30m,TF60m}:          // R1: always all 4
    b := bucketStart(c.Start, tf, r.loc)
    f := forming[c.Instrument][tf]
    if f == nil || f.Start != b:             // new bucket (or defensive gap): flush stale, open fresh
        if f != nil: out = append(out, finalize(f))
        f = openBar(b, tf, c); forming[c.Instrument][tf] = f
    else:
        foldIn(f, c)                          // high=max, low=min, close=last, vol+=
    if c.Start == b.Add((tf-1 min)):          // last minute of this bucket → emit now
        out = append(out, finalize(f)); forming[c.Instrument][tf] = nil
return out                                    // 1m every minute; 5/30/60m on boundary; ~0 lag
4.3 Session-close flush
func (r *streamRollup) FlushAll(inst) []Candle   // on SessionClosed: finalize every non-nil forming bar, clear

Only way the final (possibly partial) 30m/60m bar of the day closes.

5. Ingestor — routing (loop body is the next unparked piece)

What the ingestor routes and emits is designed below. The full for ev := range tape assembly (goroutine body, degrade rules, teardown) is the next implementation step, not yet written.

5.1 Injected construction set

{ tape <-chan emit.TapeEvent, market string, exchangeID funnel.ExchangeID, mode {canonical|onDemand}, store *CandleStore, snapshot *MarketSnapshot, rollup *streamRollup, fanout *FanOut }

5.2 Demux (per emit.TapeEvent, Model-A routing)
switch ev.Kind:
case Trade:
    snapshot.SetLast(ev.Instrument, ev.Trade.Price)      // + seed sessionOpen if unset
    fanout.Publish(snapshotPacket(exchangeID, market, ev.Instrument, snapshot.Get(ev.Instrument)))
case Quote:
    snapshot.SetQuote(ev.Instrument, *ev.Quote)
    fanout.Publish(snapshotPacket(...))
case Depth:
    snapshot.SetDepth(ev.Instrument, *ev.Depth)
    fanout.Publish(snapshotPacket(...))
case Candle:
    c := toStoreCandle(market, ev.Candle)                 // 6-field copy + market stamp
    if mode == canonical: store.Insert(ctx, c)            // persist-then-forward (provisional); err → log, continue
    for bar in rollup.Ingest(c):
        fanout.Publish(candlePacket(exchangeID, market, bar))
case Session:
    if ev.Session.Kind == SessionClosed:
        for bar in rollup.FlushAll(ev.Instrument): fanout.Publish(candlePacket(...))
    snapshot.ApplySession(ev.Instrument, ev.Session.Kind) // SessionOpened clears sessionOpen
    fanout.Publish(snapshotPacket(...))

Teardown: range ends when the exchange closes Public on Stop().

5.3 Output packet shapes (the only things crossing to broker)
candlePacket   { ExchangeID; Market; Instrument; TF Timeframe; Bar BarDTO|Candle }
snapshotPacket { ExchangeID; Market; Instrument; Snapshot <MarketSnapshot view> }
6. MarketSnapshot (own component; sibling to FanOut)
6.1 Per-instrument state
instrumentSnap struct {
    lastPrice contract.Ticks; hasLast bool
    quote     book.QuoteState           // carries its own Has* flags
    depth     DepthSnapshot
    sessionOpen contract.Ticks; hasOpen bool
}
MarketSnapshot struct { /* per-instrument map, see 6.3 */ }
6.2 Write API (ingestor, single writer per instance) + read API
// writes (model a: mutate one field, whole snap is the new live snapshot)
SetLast(inst, p); SetQuote(inst, q); SetDepth(inst, d); ApplySession(inst, kind)
// reads
Get(inst) instrumentSnap              // WS stream payload + join seed
BestAsk(inst) (contract.Ticks, bool)  // funnel.MarketPriceCache — quote.BestAsk + quote.HasAsk
LastPrice(inst) (contract.Ticks, bool)// funnel.MarketPriceCache — lastPrice + hasLast

BestAsk/LastPrice are what replace nopMarketCache — public-tape sourced (verified, D-17).

6.3 Concurrency — OPEN (two candidate mechanisms, not yet picked)
(a) RWMutex + map[instrument]*instrumentSnap — writer Locks briefly; readers RLock. Matches the house registry pattern.
(b) atomic pointer per instrument — writer swaps an immutable snap pointer; readers Load lock-free (keeps the account-side BestAsk/LastPrice hot path non-blocking), at a tiny alloc per update. Write is frequent (every Trade/Quote/Depth); reads are occasional (place-order, subscribe). Lean (b), but decide when the first real reader lands (the MarketPriceCache wiring or the WS join).
7. FanOut (distribution; passive shared struct)
7.1 Struct
subKey struct { ExchangeID string; Instrument contract.InstrumentID; Stream stream; TF Timeframe }
       // Stream ∈ {candle, snapshot}; TF zero for snapshot

FanOut struct {
    mu     sync.RWMutex
    subs   map[subKey]map[*Conn]struct{}
    byConn map[*Conn][]subKey              // reverse index for O(1) disconnect
}

Conn struct { buf chan outPacket; /* ws socket handle, lagged flag */ }
7.2 Publish (ingestor goroutine; N in parallel under RLock)
func (f *FanOut) Publish(p outPacket)
  key := keyOf(p)                          // {ExchangeID, Instrument, Stream, TF}
  f.mu.RLock(); conns := f.subs[key]
  for c := range conns:
      select { case c.buf <- p:  default: c.markLagged() }   // non-blocking; full → drop
  f.mu.RUnlock()
7.3 Subscribe / Unsubscribe (WS read goroutine; write-lock, rare)
func (f *FanOut) Subscribe(c *Conn, key subKey, seed *MarketSnapshot)
  f.mu.Lock(); f.subs[key][c]=∅; f.byConn[c]=append(...,key); f.mu.Unlock()
  if key.Stream == snapshot:
      c.buf <- snapshotSeedPacket(seed.Get(key.Instrument))   // R2: candle does NOT seed

func (f *FanOut) Unsubscribe(c *Conn, key subKey)             // delete from subs[key] + byConn[c]

Batch subscribe = one inbound frame → loop keys → one Lock covering all inserts (the batch win).

7.4 Disconnect
func (f *FanOut) Disconnect(c *Conn)
  f.mu.Lock(); for k in byConn[c]: delete(subs[k], c); delete(byConn, c); f.mu.Unlock(); close(c.buf)
7.5 Per-connection goroutines (WS side)
readLoop(c):   for { msg := conn.ReadMessage(); dispatch subscribe/unsubscribe → FanOut }   // on err → Disconnect
writeLoop(c):  for p := range c.buf { conn.WriteMessage(encode(p)) }   // SOLE socket writer; on err → Disconnect

Two goroutines per connection because concurrent writes to one WS conn are illegal (gorilla) — the buffer + single writeLoop serialize all pushes.

7.6 Transport wiring
mux.Handle("GET /candles", candleHandler)
mux.Handle("GET /ws", wsHandler)   // wsHandler: Upgrader.Upgrade(w,r) → spawn readLoop+writeLoop → block
8. Open implementation choices (decide before coding these)
Item	Where	Options
MarketSnapshot concurrency	§6.3	RWMutex-map vs atomic-ptr-per-instrument
WS library	§7.5	gorilla/websocket (assumed) vs coder/nhooyr
End population site	§1.4	in RangeRead1m (every reader gets it) vs in rollup only
Package placement	all	broker/funnel vs broker/marketdata
Persist timing	§5.2	persist-then-forward (default) vs async writer
9. Parked (rides a still-moving seam — not this component's internals)
Ingestor↔exchange attachment (WireInstance / Phase 7).
MarketPriceCache wiring: injected-at-NewBroker cache → per-instance snapshot resolution (instrument→instance).
Exchange-clock tz (what tz emit.Candle.Start carries → the UTC conversion at Insert).
Candle-table migration file; market→IANA registry as code; WS wire encoding + packet timestamp fields.
Agent live transport (Stage 5) — WS reused; wire parked for L2 redesign.