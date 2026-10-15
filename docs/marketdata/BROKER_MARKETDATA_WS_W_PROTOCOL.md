Broker Market-Data Component — WebSocket Wire Protocol

Altitude. The client/agent-facing JSON contract for the live WebSocket surface — the frames both directions send, their args, and the value conventions. This is the piece the base pair listed as open ("WS wire encoding + packet timestamp fields — we can decide").

Where this sits. Continues the base pair (BROKER_MARKETDATA_DESIGN.md, BROKER_MARKETDATA_IMPLEMENTATION_DESIGN.md) and the bodies piece (BROKER_MARKETDATA_IMPL_COMPONENT_BODIES.md §3, the FanOut/transport). The HTTP history contract is already fully specified in the base DESIGN §4.4; this is its live counterpart. Humans and agents use the same contract (agent WS = human WS, locked).

0. Transport basics
One WebSocket per user/agent, opened at GET /ws, anonymous (public, account-free axis — no identity, no auth).
JSON text frames, one message each; every frame carries a type discriminator — how both directions demux.
Ping/pong is transport-level (gorilla control frames, not app JSON): the write goroutine sends periodic pings + sets write deadlines; the read goroutine services pongs. Not part of this JSON contract, noted so it isn't forgotten.
The connection is already bound to one exchange instance (one market) by L7, so the client never sends exchangeID or market — the instance is implicit.
One socket carries the whole watchlist + the open chart together (many subscriptions).
1. On connect — broker → client (immediately)

Establishes the timezone context for every naive-local timestamp that follows (mirrors the HTTP envelope's market/timezone, base DESIGN §4.4):

json
{ "type": "connected", "market": "IN", "timezone": "Asia/Kolkata" }
2. Client → broker
2.1 Subscribe (batched)

One message can carry many instruments (the batch win is at connect-time: one parsed frame, one write-lock acquisition, one ack).

json
{ "type": "subscribe",
  "subs": [
    { "stream": "candle",   "instrument": "ACME", "timeframe": "5m" },
    { "stream": "snapshot", "instrument": "ACME" },
    { "stream": "snapshot", "instrument": "BETA" }
  ] }

The per-subscription args — the whole set:

Arg	Required?	Notes
stream	yes	"candle" or "snapshot" — the two feeds.
instrument	yes	opaque instrument id; one sub names one instrument.
timeframe	candle only	one of 1m / 5m / 30m / 60m. Omitted for snapshot.

What the client does not send: no exchangeID/market (binding implies the instance); no price/full level (that old split is replaced by the candle/snapshot streams); no account/identity (anonymous).

2.2 Unsubscribe

Same subs shape, "type": "unsubscribe". Switch instrument/timeframe = unsubscribe old + subscribe new (there is no setLevel).

3. Broker → client
3.1 Ack (per-sub status)

A batch can partly fail (e.g. one bad timeframe), so the ack reports per-sub:

json
{ "type": "ack",
  "results": [
    { "stream": "candle",   "instrument": "ACME", "timeframe": "5m", "status": "ok" },
    { "stream": "snapshot", "instrument": "ACME", "status": "ok" },
    { "stream": "snapshot", "instrument": "ZZZZ", "status": "error", "error": "unknown instrument" }
  ] }

Error reasons: unknown instrument, bad timeframe (off the 1m/5m/30m/60m allowlist), bad stream. An accepted snapshot sub is immediately followed by its seed snapshot push (R2); an accepted candle sub is bare (history comes from GET /candles).

3.2 Candle push

Model A — finalized bars only (1m every minute; 5m/30m/60m on bucket close):

json
{ "type": "candle", "instrument": "ACME", "timeframe": "5m",
  "bar": { "start": "2026-10-09T09:15:00", "end": "2026-10-09T09:20:00",
           "open": 10050, "high": 10120, "low": 10030, "close": 10090, "volume": 4200 } }
3.3 Snapshot push

Model (a) — the whole current picture re-emitted on any change; session folds in via phase (decision (a): no separate session frame — a session transition re-emits snapshots with the new phase):

json
{ "type": "snapshot", "instrument": "ACME",
  "data": {
    "phase": "open",
    "last_price": 10090, "has_last": true,
    "change_pct": 1.37,
    "quote": { "best_bid": 10085, "bid_qty": 300, "has_bid": true,
               "best_ask": 10095, "ask_qty": 150, "has_ask": true },
    "depth": { "bids": [ { "price": 10085, "qty": 300 } ],
               "asks": [ { "price": 10095, "qty": 150 } ] } } }

A watchlist client reads last_price + change_pct and ignores the rest (it still carries full top-10 depth — the known tradeoff of the two-stream model; a watchlist subscribes snapshot and shows only the price).

4. Value conventions (consistent with the HTTP side)
Raw int64 ticks/qty, bare numbers, no unit field — open/high/low/close, last_price, every price/qty (decision D-3 / P6-D33). Tick→display is the client's render job.
Timestamps: naive-local ISO-8601 in the market tz named by the connected frame ("2026-10-09T09:15:00" = IST for IN). No epoch, no offset suffix — identical rule to GET /candles.
change_pct is the one float on the wire: (last_price − sessionOpen) / sessionOpen × 100. Everything else is integer.
has_* flags are load-bearing — 0 is a valid tick price, so has_bid:false ≠ a bid of 0; carried straight from book.QuoteState.
5. Mapping to the component internals
Each subs entry becomes a FanOut subKey{stream, instrument, tf} (bodies doc §3.1).
snapshot ack → FanOut.Subscribe seeds from MarketSnapshot.Get(instrument); candle ack → no seed (R2).
Candle pushes come from the streaming rollup's finalized bars; snapshot pushes from the retained MarketSnapshot re-emit (model a).
phase on snapshot is the instance-wide session phase (bodies doc §1.7 / §2.5).
Status after this doc

Closed: the full WS JSON contract (connected / subscribe / unsubscribe / ack / candle / snapshot), the subscription args, session-via-phase (decision (a)), and the value conventions. With this, the component is fully designed end to end — storage, HTTP history, rollup, the three bodies, transport, the WireInstance lifecycle, and both wire contracts (HTTP + WS).

Remaining is build-time only: WS library pick (gorilla lean), the candles migration file, the market→IANA registry as code, and config/tunables (read timeout, per-conn buffer size, lag-eviction threshold, ping interval). Parked-by-design: Model B forming bar, 1d timeframe, day high/low, true prior-close day-change, and the B5 stop-watcher feed.