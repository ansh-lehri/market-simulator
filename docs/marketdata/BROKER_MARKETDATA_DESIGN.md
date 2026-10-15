Broker Market-Data Component — Design

Scope of this document. The conceptual design (the what and the why) of the broker's market-data component, as discussed and locked in the "broker tier 5" design thread. It covers the storage/schema, the timezone model, timeframe rollup, the HTTP history surface, the live-streaming model, the ingestor's routing, the MarketSnapshot, and the WebSocket surface. The code-level counterpart lives in BROKER_MARKETDATA_IMPLEMENTATION_DESIGN.md.

This document records only what this thread settled. Where a decision revised something from the earlier BROKER_MARKET_DATA_AND_STREAMING.md (e.g. the storage column type, the stream-split model), the revision is marked [REVISES]. Open and parked items are listed in §10 — nothing there is silently assumed.

0. Component overview
0.1 What it is

A public, instrument-keyed, account-free surface. It is a different axis from the broker's order/settlement side:

Order/settlement axis — account-keyed, routed into the account shards, moves money. (Already built: broker/funnel punch + settlement consumer.)
Market-data axis (this component) — instrument-keyed, non-shard, no account state, no fills, no auth. It consumes the exchange's public tape, persists OHLCV, and serves price/depth/OHLCV to clients (human charts + watchlists) and to agents.

"L6 (streaming)" and "L8 (persistence)" in the older design notes are operations this component performs itself, not separate services.

0.2 Two consumer paths (the spine)

Any consumer reads market data one of two ways:

HTTP history (pull) — past candles for a chart's initial load or an agent's backtest. A static read: store → rollup → convert → respond. Fully designed (this thread).
WebSocket live (push) — live data as it happens. Fully designed at the flow level (this thread); component bodies are the next step (§10).
0.3 Three data lifetimes
Data	Persisted?	Lifetime
OHLCV candles	Yes (1-min base)	cross-session, durable (Postgres)
Quote / price	No	live-only, in-memory
Depth (order book)	No	live-only, in-memory

Only OHLCV crosses to disk. Higher timeframes are rolled up on read, never stored.

0.4 Value discipline (inherited, load-bearing)

All prices are raw integer ticks (contract.Ticks = int64), all volumes raw qty (contract.Qty = int64). No floats cross any boundary, and no unit/currency field appears on any wire payload (consistent with the built account side, decision P6-D33). Display scaling (tick-size → rupees) is a render concern, outside this component.

0.5 Package placement — provisional

Working package name: marketdata. Whether it lives in broker/funnel (as *Broker methods, like the settlement consumer) or in its own package is an open placement fork (broker/exchangeseam/CLAUDE.md explicitly leaves this to "orientation's call — content is load-bearing, path is not"). All designs here are identical wherever it lands; only the package clause and import path change.

1. Storage & schema (Stage 1)
1.1 The table

One table, candles, holding 1-minute OHLCV only:

Column	Type	Meaning
market	TEXT	the broker's own market label ('IN', …). The exchange's InstrumentID is opaque and carries no market, so market must be the broker's own column.
instrument	TEXT	contract.InstrumentID (opaque string, e.g. "ACME")
timestamp	TIMESTAMPTZ	bucket start (the candle's minute). [REVISES] — see §1.3
open,high,low,close	BIGINT	contract.Ticks (int64)
volume	BIGINT	contract.Qty (int64)

Primary key: (market, instrument, timestamp). This PK is the only index, and it doubles as the range-scan index for reads — no secondary index.

1.2 Properties
1-minute only. The base grain. Everything coarser is derived on read (§3). The 1-min grain itself depends on the exchange being configured with a 1-min OHLCV interval (InstrumentSpec.OHLCVInterval) — the broker receives whatever the exchange emits; it does not control the interval.
Append-only, idempotent. A closed minute never changes, so re-delivery of the same (market, instrument, timestamp) is a harmless no-op, never an error.
Canonical-only. Only canonical-market candles are persisted. On-demand sessions never hit the store (that decision lives in the ingestor, §6 — the store writes whatever it is handed).
1.3 Timezone: store-UTC [REVISES]

The earlier doc stored a naive session-local TIMESTAMP. Revised: the column is TIMESTAMPTZ and we store UTC. Postgres keeps the instant in UTC internally and can render any timezone on read. This is consistent with the built account side — participant_trades.exit_at is already TIMESTAMPTZ. The full timezone model is §2.

1.4 The read/write boundary type (type "B")

The store speaks its own row type, not the exchange's emit.Candle:

Candle{ Market; Instrument; Start; End; Open; High; Low; Close; Volume }

Rationale for a store-local type over passing emit.Candle directly:

Keeps the durable layer importing only contract (the allowed shared package), never exchange/emit.
Drops emit.Candle's Seq (meaningless once stored) and redundant fields.
Same shape the rollup engine folds — a stored 1-min row and a folded higher-timeframe bar share one type.

The ingestor does the small copy from emit.Candle → store Candle, stamping market (which it holds). The store never sees emit.Candle.

End is the bucket's exclusive close time (Start + grain): a 1-min row at 19:05 has End = 19:06; a folded 5m bar at 19:05 has End = 19:10. End is derived, never stored (no end column) — it's set in code on read.

1.5 Operations (design intent; signatures in the impl doc)
Insert — persist one closed 1-min candle, idempotent (ON CONFLICT DO NOTHING). Infra failure is reported; a conflict is not a failure.
RangeRead1m — read stored 1-min candles for one (market, instrument) over a half-open [from, to) window, oldest-first, keyset-paged forward on timestamp. The limit is honored verbatim — the per-response cap lives in the rollup engine, not the store. Empty result is a legitimate gap (empty slice, not an error); only infra/driver failure is an error.
2. Timezone model (cross-cutting)

The component stores UTC and presents country-local. This threads through the schema (§1), the rollup bucketing (§3), and the HTTP edge (§4).

2.1 The two conversions
Request in: client sends naive-local times → broker attaches the market's timezone → converts to UTC → queries the store.
Response out: store returns UTC rows → broker converts to market-local → bucket-aligns in local → serializes as naive-local ISO-8601 strings.

So the UTC store is invisible at the edge — a client only ever sends and sees market-local time.

2.2 Bucket alignment must be in country-local time (the subtle one)

IST is UTC+5:30. If 30m/60m buckets were aligned in UTC, an Indian hourly bar would start at :30 past the IST hour — wrong on the chart. So alignment happens after converting to local tz. 1m and 5m are unaffected by the half-hour offset but run the same local path for consistency; 30m and 60m are where this is load-bearing.

2.3 The market → IANA timezone registry

A small lookup (IN → "Asia/Kolkata", US → "America/New_York"), same allowlist shape as bringup/country.go's country gate. Two readers: the rollup engine (to bucket-align + serialize local) and the HTTP handler (to parse inbound times local→UTC and to put timezone on the response).

2.4 Wire format

Every time string on the wire — from, to, each bar's start/end — is naive-local ISO-8601 (e.g. "2026-09-17T19:05:00"), with a timezone field on the response naming which tz those naive strings are in.

3. Rollup / timeframe aggregation (Stage 3)
3.1 Offered timeframes

1m, 5m, 30m, 60m. An allowlist — anything off it is rejected (400 on HTTP), never silently rounded.

3.2 Bucket alignment — wall-clock, in local tz

Each bar's start is the input minute truncated down to the nearest timeframe mark in the market's local tz: 5m 19:07 → 19:05; 30m 19:34 → 19:30; 60m → top of the local hour. No knowledge of session-open is needed. 1m is pass-through.

3.3 The fold

Over 1-min rows (oldest-first) for one instrument, grouping by bucket:

open = first minute's open
high = running max
low = running min
close = last minute's close
volume = sum
End = bucket start + timeframe (exclusive)

One pass. Two behaviors fall out for free: a bucket with no rows is never created → omitted → renders as a chart gap (overnight/inter-session); a partial bucket (session opens/closes mid-bucket) aggregates whatever minutes are present.

3.4 Backend-side, one implementation

The rollup runs server-side, and the same fold serves HTTP history and the live stream, so humans and agents get byte-identical bars. This is non-negotiable because agents have no rendering library — they cannot fold client-side, which is the whole reason rollup is server-side.

The engine always returns completed bars. The forming (current) minute is never in the store, so the live forming bar is the streaming path's concern (§5), not the fold's.

4. HTTP history surface

A pure durable read — straight to the store, off-shard, no session, no funnel, no auth/account (public axis). It therefore inherits the broker's durable-read status family (per broker/reads/CLAUDE.md), the same family as the built trades endpoint.

4.1 Request

GET /candles?market=&instrument=&timeframe=&from=&to=&cursor=&limit=

Validated into a query object. 400 on: missing market/instrument; timeframe off the allowlist; unparseable times or from >= to. Absent cursor → first page. Absent/oversized limit → default/clamped (the engine owns the cap).

4.2 The read → fold → page chain
Compute how many 1-min rows to pull for limit folded bars (limit × timeframe + a one-bucket slack).
RangeRead1m over [from, to) from the cursor.
Fold to the requested timeframe.
Trim to limit bars; compute the next cursor.
4.3 Paging / cursor rule

The cursor is a 1-min timestamp fed back as the next read's afterTS (keyset-forward). The one subtlety is the trailing partial bucket: paging in the 1-min domain while returning folded bars means the last bar of a page can be a bucket not yet complete in this batch.

Rule:

If the batch came back full (more rows likely exist) → drop the final in-progress bar and set the cursor to that dropped bucket's first minute, so the next page re-folds it whole. No split bucket, no duplicate.
If the batch came back short (hit to or ran out) → the last bar is genuinely complete → keep it, cursor = null (last page).
4.4 Response envelope
CandleResponse{
    Market      // echo, "IN"
    Instrument  // echo, "ACME"
    Timeframe   // echo, "5m"
    Timezone    // IANA tz used for local conversion, "Asia/Kolkata"
    From        // echo of requested window start, naive-local ISO
    To          // echo of requested window end,   naive-local ISO
    Candles []Bar
    NextCursor  // null = last page
}
Bar{ Start; End; Open; High; Low; Close; Volume }

Bars carry no market/instrument (they're in the request) and no unit (bare int64). Start/End are naive-local ISO strings. The engine stays pure (returns bars + cursor only); the handler builds this envelope and adds the echoed context — shaping is the response layer's job, not the store/engine's.

4.5 Status map (durable-read family)
200 on success, including empty (candles: []) — never 404/409 for "no data."
503 on infra failure or context timeout — never 504 (504 is a live-read code; this is a durable read).
400 on bad params.
4.6 Timezone symmetry

Request: parse from/to/cursor naive-local → attach market tz → UTC → query. Response: UTC rows → local → bucket-align local → serialize naive-local. The client never touches UTC.

5. Live streaming model (Stage 4, read side)
5.1 Model A — finalized bars only (LOCKED; may switch to B later)

The key reframe: the snapshot stream already carries live lastPrice (updated per trade), so the UI's live-price movement does not depend on the candle stream. That lets the candle stream be simple:

The candle stream pushes a bar only when its bucket closes (server-rolled, identical to HTTP history).
Live intra-bar movement comes from the snapshot's lastPrice — the UI extends the rightmost bar's tip / draws a live price line from it.

Trade-off accepted: the rightmost bar's high/low wick does not grow live — only the price tip moves. Slightly less "real-chart," much simpler, and perfectly adequate for now. If true forming wicks are wanted later, switch to Model B (broker builds a forming bar from trade prints and pushes updates + a finalize). Not now.

5.2 Agents take closes only

TA signals compute on closed bars (a forming bar makes signals repaint). So agents consume finalized bars only — and under Model A there is no forming bar anyway, so there is no human/agent difference at this layer. One finalized-bar stream serves both.

5.3 Server-side live rollup (LOCKED)

Because agents can't fold, live rollup is server-side too, so the candle subscription carries a timeframe (subscribe candle ACME 5m), reusing the same fold as HTTP.

5.4 Streaming rollup behavior

The ingestor keeps a forming bar per (instrument, timeframe) and folds each arriving closed 1-min candle into it. The exchange emits 1-min candles continuously (a flat candle every minute even with zero trades — see §6), so the stream never has mid-session holes.

A bucket finalizes the moment its last minute arrives — i.e. when the minute bucketStart + (timeframe − 1min) folds in. So 1m emits every minute; 5m emits when the :09/:14/… minute lands; ~0 extra lag (it never waits for the next bucket to start).
SessionClosed flush — on session close, flush all non-nil forming bars. This is the only way the final (possibly partial) 30m/60m bar of the day closes, since it has no "last minute" trigger mid-bucket. Partial final bucket is fine (matches HTTP partial-bucket behavior).
5.5 Rollup decisions (LOCKED)
R1 — fold all 4 timeframes always, regardless of who's subscribed. Keeps rollup a pure function of the candle stream, decoupled from subscription churn; cost is 4 trivial folds/min/instrument; fanout simply drops emits with no subscribers.
R2 — candle subscribe does NOT seed. A new candle subscriber loads past bars via the HTTP history endpoint, then receives live closes over WS. (Contrast: the snapshot subscribe does seed — §7/§8 — because quote/depth are change-only.)
6. Ingestor — routing (Stage 2, routing only)
6.1 Position in the pipeline
exchange public tape → [ingestor: drain + format] → { CandleStore, MarketSnapshot, FanOut }

The ingestor is a broker goroutine (non-shard), one per exchange instance, and the sole consumer of that instance's Exchange.Public <-chan emit.TapeEvent — one tape per instance carrying all that instance's instruments. When the exchange Stop()s, it closes Public; that close is the ingestor's teardown trigger. The ingestor is a producer, not a socket owner — clients never connect to it.

6.2 The five event kinds on the tape

All live data arrives as one emit.TapeEvent stream, tagged Kind, with a per-instrument Seq (ordered, no gaps):

Kind	Payload	Emission trigger (exchange)
Trade	PublicTrade{Price, Qty}	per fill, after a match. Anonymized (no order/participant). The last-traded-price source.
Quote	book.QuoteState{BestBid,BidQty,HasBid, BestAsk,AskQty,HasAsk, LastPrice,HasLastPrice}	after submit/cancel, change-only. Has* flags are load-bearing (0 is a valid price; "no bid" ≠ "bid 0").
Depth	DepthSnapshot{Bids,Asks []DepthLevel{Price,TotalQty}}	after submit/cancel, change-only. Top-10 per side, best-first, lossy. This is the "order book."
Candle	Candle{Instrument,Start,O/H/L/C,Volume,Seq}	on the per-second monitor when the minute elapses. Continuous (flat candle on empty minutes), closed candles only.
Session	SessionEvent{Kind} (SessionOpened/AuctionWindowOpened/SessionClosed)	on clock transitions, emitted on every instrument.
6.3 Routing table (Model-A correct)

The ingestor demuxes by Kind and routes to three internal destinations. Note: under Model A the candle stream is fed by closed candles, not trades — trades feed the snapshot's live price, not the bars.

Kind	→ CandleStore	→ MarketSnapshot	→ FanOut
Trade	—	set lastPrice (+ seed sessionOpen if unset)	snapshot packet
Quote	—	set quote	snapshot packet
Depth	—	set depth	snapshot packet
Candle	Insert (canonical mode only)	—	candle packet(s), via streaming rollup (§5.4)
Session	—	SessionOpened clears sessionOpen; SessionClosed flushes forming bars	snapshot packet (session info)
6.4 Two output packet types (the only things crossing ingestor → broker)

The ingestor emits exactly two packet shapes, each wrapped with {exchangeID, market, instrument, …}:

Candle packet — a finalized bar.
MarketSnapshot packet — the live market picture (§7).

exchangeID is stamped by the ingestor (it's injected with it, a funnel.ExchangeID = string identifying the instance). This tag is what lets one FanOut serve multiple ingestors/instances — the dispatcher matches on it. exchangeID is not persisted (no store column) and is not sent by WS clients (broker-internal).

6.5 Persist-then-forward — provisional

On a canonical candle the ingestor persists then forwards (synchronous Insert on the ingestor goroutine; each ingestor writes disjoint rows, pgxpool is concurrency-safe). This is flagged provisional — if the live feed ever feels slow, the insert can move to a small async writer goroutine (forward-first). Default is inline.

7. MarketSnapshot
7.1 Model (a) — one retained object per instrument (LOCKED)

A single retained snapshot object per instrument. When any one field changes, that field updates and the whole snapshot is re-emitted as a new live snapshot (majority-unchanged fields are fine — latest-wins).

7.2 Fields
Field	Set from	Purpose
lastPrice + hasLast	Trade	LTP; the has flag gates it (0 is a valid price)
quote (book.QuoteState)	Quote	best bid/ask + Has* flags; chart quote widget + BestAsk for market-buy
depth (DepthSnapshot)	Depth	top-10 ladder; chart depth widget only
sessionOpen + hasOpen	first Trade after SessionOpened	session-open-relative day-change base
7.3 Three uses of the one object
WS snapshot stream (push) — dispatched live like candles.
WS join-on-subscribe seed (sync pull) — because quote/depth are change-only, a new subscriber must be seeded with current state immediately, or it sees nothing until the next move.
Account-side synchronous read (pull) — the broker's own order path asks "what's the price now?": BestAsk(instrument) for market-buy reservation, LastPrice(instrument) for position marks. This is the funnel.MarketPriceCache interface.
7.4 It is a sibling to FanOut, not inside it

FanOut only moves events into buffers — it retains nothing. MarketSnapshot retains latest-state and supports a synchronous read. Different responsibilities, different access patterns (push vs pull), so it is its own component.

7.5 Price source — VERIFIED public-tape (not private)

The account-side price lookup (BestAsk/LastPrice) is fed by the public tape → MarketSnapshot, not by private channels. Verified across three prior threads:

tier-1 D1 — market-buy best_ask comes from the broker's market-data cache fed by the public tape.
tier-5 — that cache is read by handlePlaceOrder (market-buy) + the position-mark path, fed by the public-tape consumer.
Layer-1 — LTP and best-bid/ask are market-wide truth on the public quote; the private stream is EngineResult (own fills) only.

Why private can't do it: market-wide price moves on other participants' activity; a private, account-scoped stream is silent when your account isn't trading, yet the market price keeps moving. Market-wide LTP/best-ask is only derivable from the public tape.

Consequence: MarketSnapshot is what satisfies funnel.MarketPriceCache (today stubbed as nopMarketCache, every lookup a deliberate miss). It lights up when Phase 5/6/7 wiring replaces the stub.

8. WebSocket surface (Stage 4, transport + dispatch)
8.1 Transport — one mux, two routes

One http.ServeMux, one server, one port:

GET /candles → the history handler (§4).
GET /ws → an ordinary handler that upgrades the HTTP connection to WebSocket (hijacks it); the handler then does not return — it owns that socket for the connection's life.

Both routes coexist cleanly; this is the standard Go pattern.

8.2 Connection model

Each user/agent opens its own /ws connection. A connection is anonymous (account-free axis — no identity, no auth, consistent with "no auth/identity on internal work") and holds a set of subscriptions + one outbound buffer. Humans and agents are identical here.

8.3 Subscribe (batch)

Right after connect, the client declares what it wants:

{action:"subscribe", stream:"snapshot", instruments:[…]} — snapshot(s), no timeframe.
{action:"subscribe", stream:"candle", instruments:[…], timeframe} — candle(s) at a timeframe.

Batch (many instruments per message) is the chosen ergonomics — one parsed frame, one write-lock acquisition, one ack, versus N of each. At runtime there is zero difference (same inventory entries either way); the win is purely at connect-time.

The client sends instrument(s) + stream (+ timeframe); the broker resolves instrument → exchangeID from its own registry (clients never send exchangeID).
Validation at subscribe: unknown instrument / bad timeframe / bad stream → nack on the socket; else ack.
Snapshot ack carries the seed (current MarketSnapshot); candle ack is bare (R2 — history comes via HTTP).
8.4 Inventory (FanOut shared memory)

Subscriptions are stored keyed {exchangeID, instrument, stream, timeframe} (timeframe empty for snapshot) → a set of connections, plus a reverse index per connection for clean teardown, under one RWMutex. This is the exact tuple the ingestor's outbound packet carries, so match-back is a single map lookup — no scan.

8.5 Per-connection: two goroutines

A WS connection is full-duplex and concurrent writes to one connection are illegal (gorilla). So per connection:

read goroutine — loops reading inbound messages; handles subscribe/unsubscribe → mutates inventory (write-lock).
write goroutine — the sole writer to the socket; drains this connection's bounded buffer → writes to the socket.

The buffer exists precisely because all pushes (from N ingestors) must be serialized through one writer per connection.

8.6 Publish (ingestor goroutine, inline)

On the ingestor's own goroutine, after massaging (and persisting, for candles): take RLock, look up the packet's key, and for each subscribed connection do a non-blocking send into its buffer (drop + mark lagged if full). N ingestors Publish in parallel — RLock doesn't serialize them. The ingestor never blocks on a slow socket.

8.7 Lifecycle
unsubscribe → inventory delete for that key + connection.
switch instrument/timeframe → unsubscribe + subscribe.
disconnect (socket drop) → purge all of that connection's keys via the reverse index, close its buffer, both goroutines exit. Nothing durable to settle (pure in-memory).
slow client → buffer-full drops: a snapshot drop is harmless (next snapshot supersedes); a candle drop is a chart gap → if a connection stays lagged past a threshold, disconnect it; the client reconnects and re-fetches history via /candles, then resumes live (clean rehydrate, no WS backfill).
8.8 Concurrency, in one line

One RWMutex on the inventory. Read goroutines take the write-lock on subscribe/unsubscribe (rare); ingestors take the RLock on every Publish (hot). Two goroutine classes, one lock.

8.9 WS library

gorilla/websocket is the working assumption (most common; the read-deadline/ping-pong/close semantics referenced above are its model). Not finally locked — see §10.

9. Cross-cutting locked decisions (registry)
#	Decision
D-1	Component is public, instrument-keyed, account-free, non-shard. Separate axis from order/settlement.
D-2	Package name marketdata provisional; placement (funnel vs own pkg) is an open fork.
D-3	Raw int64 ticks/qty everywhere; no floats, no unit field on any payload.
D-4	candles schema (market, instrument, timestamp TIMESTAMPTZ, O/H/L/C, volume), PK = the only index.
D-5	Store UTC, present country-local; bucket-align in local tz; wire = naive-local ISO + timezone field.
D-6	Store-local Candle type (B), shared with the rollup engine; End derived (exclusive), never stored.
D-7	Timeframes 1m/5m/30m/60m, allowlist (reject off-list, never round).
D-8	Rollup is server-side, one impl for HTTP + live → humans/agents byte-identical.
D-9	HTTP /candles is a durable read: 200 (incl. empty) / 503 (infra/timeout) / 400 (bad params). No 404/409/504.
D-10	Paging cursor = 1-min timestamp; trailing-partial-bucket rule (drop-and-recur on full batch; complete on short batch).
D-11	Model A live candles (finalized bars + live price from snapshot). Switch to Model B only if forming wicks wanted.
D-12	R1 fold all 4 timeframes always; R2 candle subscribe does not seed (history via HTTP); snapshot subscribe does seed.
D-13	Ingestor = one broker goroutine per exchange instance, sole Exchange.Public consumer; tape close = teardown.
D-14	Ingestor emits two packet types (candle, snapshot), each tagged {exchangeID, market, instrument}.
D-15	persist-then-forward for canonical candles — provisional (may flip to async writer).
D-16	MarketSnapshot = retained model-(a) object, its own component (sibling to FanOut), three uses.
D-17	Account-side price (BestAsk/LastPrice) is public-tape-sourced via MarketSnapshot → satisfies funnel.MarketPriceCache. Verified, not private.
D-18	WS: one mux (/candles + /ws), connection per user/agent, anonymous.
D-19	Subscribe is batch; client sends instrument(s), broker resolves exchangeID.
D-20	Inventory keyed {exchangeID, instrument, stream, timeframe}; one RWMutex; two goroutines per connection (read + sole-writer).
D-21	Slow client: drop; lagged-past-threshold → disconnect → client reconnects + HTTP rehydrate.
10. Status — covered / open / parked
Covered (designed to snippet/flow level, this thread)
Storage & schema (Stage 1) — table, CandleStore operations, type B, UTC store.
Timezone model — store-UTC/present-local, local bucket-align, registry, wire format.
Rollup — timeframes, bucketing, the fold, server-side one-impl.
HTTP history surface — request/validation, read→fold→page chain, cursor rule, response envelope, status map, tz symmetry.
Live model — Model A, server-side live rollup, R1/R2.
Ingestor routing — five kinds, routing table, two output packets, exchangeID.
MarketSnapshot — model (a), fields, three uses, verified public-tape source.
WebSocket surface — transport, connection, batch subscribe, inventory, per-conn goroutines, publish, lifecycle, concurrency.
Open (decisions not yet made)
MarketSnapshot concurrency mechanism — RWMutex-map vs atomic-pointer-per-instrument (not picked).
WS library — gorilla assumed, not locked.
Package placement — funnel vs own package (D-2).
WS wire encoding + exact timestamp fields on packets — "we can decide."
Next unparked implementation piece
The ingestor assembly (loop body) — the for ev := range tape demux that ties store + streaming-rollup state + MarketSnapshot + FanOut together. Everything it routes and emits is designed; the goroutine body itself is the next step.
FanOut and MarketSnapshot struct bodies — flows are designed; method bodies + the concurrency pick remain.
Parked (deliberately deferred; rides a still-moving seam)
Ingestor ↔ exchange attachment (WireInstance / Phase 7) — how a tape actually reaches an ingestor.
MarketPriceCache wiring — how one cache injected at NewBroker resolves to the right instance's snapshot (instrument → instance).
Exchange-clock timezone — what tz emit.Candle.Start is stamped in (needed for the UTC-store conversion at insert).
Agent live transport (Stage 5) — WS reused; the exact wire parked for the L2 redesign.
Candle-table migration file — not in the current codebase; needs authoring.
market → IANA registry — designed; needs to exist as code.
Appendix — codebase grounding

Real files this design was checked against (fresh source, this thread):

exchange/emit/tape.go — the TapeEvent tagged union + payload types (PublicTrade, DepthSnapshot, Candle, SessionEvent).
exchange/emit/emitter.go, exchange/wire/handle_submit.go, handle_cancel.go, handle_tick.go — the emission triggers (change-only quote/depth; continuous closed candle; session on transition).
exchange/wire/exchange.go — Exchange.Public <-chan emit.TapeEvent (one tape per instance; closed on Stop()).
exchange/book/* — QuoteState shape.
contract/values.go — InstrumentID = string (opaque), Ticks/Qty = int64, Seq = uint64.
connectivity/db/postgres.go — *pgxpool.Pool (not database/sql).
broker/store/* (esp. trades.go) — the durable-read + pgx conventions CandleStore mirrors.
broker/funnel/mkt_cache.go — MarketPriceCache interface + nopMarketCache stub.
broker/funnel/broker.go, broker/bringup/bringup.go, bringup/country.go — NewBroker(..., mktCache), bringup (no exchange/markets created there), the Country allowlist shape.
broker/order/http.go — errorBody/writeJSON/writeError/recoverPanic handler conventions.
broker/reads/CLAUDE.md — the durable-read status rules (200 empty / 503 / no 504).
broker/exchangeseam/CLAUDE.md — scopes this component as Phase-4 Points 3–5; WireInstance = Phase 7.