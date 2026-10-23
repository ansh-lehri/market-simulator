# API Contract — `broker/marketdata` (candle history + live streaming) (v2)

**Status: refreshed 2026-10-23, re-verified against this repo's git
history up to commit `025f365`.** Supersedes `docs/api-contract/
marketdata.md` (v1, 2026-10-16). `git diff 1c436cc..HEAD` confirms **zero
changes** to `broker/marketdata/` since v1's last pass. `conductor
/conductor.go`'s WS route registration (`mux.HandleFunc(marketdata
.PathWS, conductor.handleWS)`) and `conductor/http_endpoints.go`'s
`handleWS` dispatcher are also unchanged — the one conductor file that did
change this pass, `ondemand_spinup.go`, is unrelated to WS dispatch or
`FanOut` wiring (it only touches setup-failure registry cleanup; the
`WireInstance` call that produces the `*FanOut` a session's WS connection
resolves to is untouched). **This entire document's technical content is
a verified-unchanged carry-forward.** The one addition is a reachability
note (§0) tied to the new `cmd/server/main.go` draft.

## 0. (NEW this pass) Reachability

`GET /v1/candles` is mounted inside `bringup.BringUp`'s mux (unchanged).
`GET /v1/ws` is mounted inside `conductor.MountRoutes` (unchanged, and
unaffected by this pass's one conductor-side change — see above). As of
this pass, a new, untracked, unreviewed draft, `cmd/server/main.go`, joins
both muxes onto one listener — see `README.md` §0 and `cross-cutting.md`
§7 for the full detail. Both surfaces documented below are therefore
reachable end-to-end on a real (draft) process for the first time, with
no change to either surface's own behavior.

## 1. Package responsibility

Unchanged from v1. `broker/marketdata` is the broker's public,
instrument-keyed, account-free market data axis: candle persistence,
chart history over HTTP, live price/candle streams over WebSocket, and
the `funnel.MarketPriceCache` backing seam (best-ask, last-price).

| # | Surface | Method/Path | Registered by |
|---|---|---|---|
| 1 | Candle history | `GET /v1/candles` | `broker/bringup` |
| 2 | Live stream (WS upgrade) | `GET /v1/ws` | `conductor` (L7) |

---

## 2. `GET /v1/candles` — candle history (durable read)

Unchanged from v1. Constant: `marketdata.PathCandles = "/v1/candles"`.

### 2.1 Query parameters

| Param | Required | Format | Notes |
|---|---|---|---|
| `market` | yes | string | Allowlist: **`IN`** (`Asia/Kolkata`), **`US`** (`America/New_York`). The repo's one shipped config, `configs/markets/IN.json`, populates `IN` with two instruments, `ACME`/`BETA` — see `README.md` §0a. |
| `instrument` | yes | string | Not validated against a known-instrument list here (unlike WS) — unknown instrument yields an empty page, not 400/404. |
| `timeframe` | yes | `1m`\|`5m`\|`30m`\|`60m` | Case-sensitive exact match; anything else → 400. |
| `from` | yes | naive-local `2006-01-02T15:04:05` | Parsed in the `market`'s timezone. |
| `to` | yes | same format | Must be strictly after `from`. |
| `cursor` | no | same format | From a previous response's `next_cursor`. |
| `limit` | no | positive integer | Default **100**, hard cap **500** (silently clamped); `<=0` → 400. |

### 2.2 Response envelope — `200 OK`

```json
{
  "market": "IN",
  "instrument": "ACME",
  "timeframe": "5m",
  "timezone": "Asia/Kolkata",
  "from": "2026-10-16T09:00:00",
  "to": "2026-10-16T15:30:00",
  "candles": [
    { "start": "2026-10-16T09:15:00", "end": "2026-10-16T09:20:00", "open": 10000, "high": 10080, "low": 9950, "close": 10040, "volume": 1200 }
  ],
  "next_cursor": "2026-10-16T09:20:00"
}
```
(Example instrument/price levels updated to match `configs/markets/
IN.json`'s actual `ACME` seed price of 10000 and tick size 5 — cosmetic
grounding only, not a behavior change.) `candles` is never `nil`; `from`/
`to` are echoed verbatim from the raw query string; no unit/currency field
anywhere; no float on this endpoint at all.

### 2.3 Pagination rule (trailing-partial-bucket cursor)

Unchanged — full batch drops the possibly-incomplete trailing bar and
sets `next_cursor` to its `start`; short batch keeps everything and sets
`next_cursor` to `null`; a gap-heavy short batch can still need trimming
to `limit`, in which case `next_cursor` points at the first trimmed bar.

### 2.4 Status codes

| Status | Trigger |
|---|---|
| 200 | Success, including empty `candles: []`. |
| 400 | Missing/invalid any required param, or `from >= to`. |
| 405 | Non-GET. |
| 503 | Store/infra failure or deadline exceeded. |
| 500 | Defensive-only. |

Never 404, never 504.

---

## 3. `GET /v1/ws` — live stream (WebSocket upgrade)

Unchanged from v1. Constant: `marketdata.PathWS = "/v1/ws"`.

### 3.1 Route registration

**Reachable end-to-end via `conductor`, not `broker/bringup`** —
unchanged. `broker/bringup/bringup.go` still does not register `/v1/ws`
(a dedicated regression test still asserts 404 at the bringup mux on
purpose). `conductor/conductor.go` still owns it:
`mux.HandleFunc(marketdata.PathWS, conductor.handleWS)`.

- `conductor.handleWS` rejects non-GET (405), rejects a missing
  `?session=` (401), then resolves `SessionID → Session.Market → Market
  .fanout` and delegates to `marketdata.NewWSHandler`.
- A resolve miss (unknown session, not `Active`, market gone/not `Live`)
  → 404 before any upgrade attempt.
- `conductor.wsCfg` is sourced from `MarketDataPort.WSConfig()`, the same
  `marketdata.WSConfig` `broker/bringup` builds from `BrokerConfig`.

**Net effect, re-confirmed this pass:** connect to `GET /v1/ws?session=
<sessionID>` on whichever process mounts the conductor's mux — as of this
pass, that can be `cmd/server/main.go`'s draft process, which mounts both
muxes together (see §0). The session id still comes from conductor's own
join/account endpoints, not from this package.

### 3.2 WS tunables (`WSConfig`)

Unchanged:

| Field | Meaning | Default |
|---|---|---|
| `ConnBuffer` | Per-connection outbound frame buffer depth. | 256 |
| `LagEvictionThreshold` | Consecutive drop count before eviction. | 32 |
| `PingInterval` | Server Ping cadence. | 30s |
| `ReadTimeout` | Socket read deadline, reset on Pong. | 60s |
| `WriteTimeout` | Write deadline. | 10s |

`cmd/server/main.go` leaves all six `BrokerConfig` WS/candle tunables at
zero in its literal, so a process built from that draft file gets every
one of the defaults above — it does not override any of them.

### 3.3 Connection lifecycle

Unchanged — carrier validation → FanOut resolve → gorilla upgrade
(`CheckOrigin` always `true`) → `readLoop`/`writeLoop` → `connected` frame
sent first → ping/pong keepalive → eviction or disconnect ends the
connection via `FanOut.Disconnect` (idempotent CAS).

### 3.4 Slow-client eviction

Unchanged — non-blocking enqueue, `laggedCount` reset on success,
eviction at `LagEvictionThreshold` (default 32), no server-side
backfill/replay on reconnect.

### 3.5 Inbound message framing

Unchanged:

```json
{ "type": "subscribe", "subs": [ { "stream": "snapshot", "instrument": "ACME" } ] }
```
```json
{ "type": "unsubscribe", "subs": [ { "stream": "candle", "instrument": "ACME", "timeframe": "5m" } ] }
```

| Field | Values |
|---|---|
| `type` | `"subscribe"` \| `"unsubscribe"`; anything else logged and ignored. |
| `subs[].stream` | `"snapshot"` \| `"candle"`; else nacks `"bad stream"`. |
| `subs[].instrument` | Required; must have appeared at least once on the tape, else nacks `"unknown instrument"` — real race window on a freshly-wired instance. |
| `subs[].timeframe` | Required for `"candle"`, one of `1m`/`5m`/`30m`/`60m`; ignored for `"snapshot"`. |

### 3.6 Outbound message framing

Unchanged — four frame types: `connected` (once, first), `ack` (once per
subscribe batch), `snapshot` (whole-picture re-emit on any field change,
seeds on subscribe), `candle` (finalized bars only, never seeds on
subscribe).

#### `connected`
```json
{ "type": "connected", "market": "IN", "timezone": "Asia/Kolkata" }
```

#### `ack`
```json
{
  "type": "ack",
  "results": [
    { "stream": "snapshot", "instrument": "ACME", "status": "ok" },
    { "stream": "candle", "instrument": "BOGUS", "timeframe": "5m", "status": "error", "error": "unknown instrument" }
  ]
}
```

#### `snapshot`
```json
{
  "type": "snapshot",
  "instrument": "ACME",
  "data": {
    "phase": "open",
    "last_price": 10040, "has_last": true, "change_pct": 0.4,
    "quote": { "best_bid": 10000, "bid_qty": 120, "has_bid": true, "best_ask": 10040, "ask_qty": 95, "has_ask": true },
    "depth": { "bids": [ {"price": 10000, "qty": 120} ], "asks": [ {"price": 10040, "qty": 95} ] }
  }
}
```
`change_pct` (float64) is the sole float on either surface.

#### `candle`
```json
{
  "type": "candle",
  "instrument": "ACME",
  "timeframe": "5m",
  "bar": { "start": "2026-10-16T09:15:00", "end": "2026-10-16T09:20:00", "open": 10000, "high": 10080, "low": 9950, "close": 10040, "volume": 1200 }
}
```
Identical fold (`RollUp`/`streamRollup`) produces both this and the HTTP
`candles[]` shape, so history and live streaming are byte-identical for
the same bucket. No forming-bar push exists — intra-bar movement rides
`snapshot.data.last_price` instead.

---

## 4. Shared types / enums (both surfaces)

Unchanged — `Timeframe` (`1m`/`5m`/`30m`/`60m`), `SessionPhase`
(`unknown`/`open`/`auction`/`closed`), market allowlist (`IN`→`Asia/
Kolkata`, `US`→`America/New_York`), value discipline (bare int64
ticks/qty, `change_pct` the sole float), `{"error":"<message>"}` HTTP
envelope.

---

## 5. Discrepancies / Open Questions

All eight items carried forward from v1, re-verified unchanged (the `git
diff` scope check confirms no code in this package changed since
`1c436cc`).

1. **`GET /v1/ws` registration — RESOLVED (with a twist), unchanged.**
   `conductor` owns and registers it; `broker/bringup` deliberately does
   not. As of this pass, both are joined onto one listener by the new
   `cmd/server/main.go` draft — see §0 and `cross-cutting.md` §7 — but
   the ownership split itself is unchanged.
2. **No instrument validation on `GET /v1/candles`.** Still open.
3. **WS instrument-knowledge race is real and user-visible.** Still open.
4. **No candle-stream backfill/catch-up on evict-and-reconnect.** Still
   open.
5. **No live mid-bar candle data.** Still open, deliberate tradeoff.
6. **`change_pct` can read `0.0` ambiguously** (flat day vs. no baseline
   yet). Still open.
7. **Session carrier for WS is a bare query-string id, no validation
   beyond presence.** Still open — now additionally relevant because, as
   of this pass, a session that failed on-demand setup is deleted outright
   rather than marked `Failed` (see `conductor.md` §3a/§6c): a WS client
   holding a `session_id` from a trigger that later 503'd would have
   gotten 404 at the WS gate either way (the session was never `Active`),
   so this specific change doesn't alter WS-gate behavior — noted here
   only to pre-empt the question, not because anything changed.
8. **`WSConfig`/`Config` tunables are process-wide, not per-market or
   per-tier.** Still open.
