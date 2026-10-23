# API Contract — `broker/order/` + `broker/transfer/` (v2)

**Status: refreshed 2026-10-23, re-verified against this repo's git history
up to commit `025f365`.** Supersedes `docs/api-contract/
broker-order-transfer.md` (v1, 2026-10-17 against `1c436cc`). `git diff
1c436cc..HEAD` touches only `conductor/ondemand_spinup.go` — nothing in
`broker/order/`, `broker/transfer/`, `broker/funnel/`, `exchange/match/`,
or `contract/` changed. Every file v1 cited (`broker/order/{http,place,
cancel,modify,mapping,ports,config}.go`, `broker/transfer/{http,mapping,
ports,config}.go`, `broker/funnel/{place,mkt_cache,money_config,reasons,
results,track}.go`, `exchange/match/matcher.go`, `contract/inbound.go`,
`broker/bringup/bringup.go`) was re-read for this pass and confirmed
byte-identical to v1's citations. **This entire document's technical
content is a verified-unchanged carry-forward** — every status code,
field, reason tag, and example below is re-confirmed against current
source, not assumed. The one addition is §0a, covering the new,
untracked `cmd/server/main.go` and what it means for these four routes'
reachability.

**Scope of this doc:** the four endpoints owned by `broker/order/` (place, cancel, modify) and
`broker/transfer/` (bank↔trading transfer) only.

---

## 0. Wiring status — RESOLVED as of commit `dd66a73` (unchanged since v1)

Re-reading `broker/bringup/bringup.go`'s current `buildMux` directly:

```go
mux.Handle(order.PlacePath, order.NewPlaceHandler(brokerInstance, orderCfg))
mux.Handle(order.CancelPath, order.NewCancelHandler(brokerInstance, orderCfg))
mux.Handle(order.ModifyPath, order.NewModifyHandler(brokerInstance, orderCfg))

mux.Handle(transfer.Path, transfer.NewHandler(brokerInstance, brokerStore, transferCfg))
```

All four routes are registered **unconditionally, inside `bringup.BringUp`
itself** — a caller gets all four for free the moment it calls
`bringup.BringUp(ctx, brokerCfg)`, with no extra wiring step required.
**New config fields** `BrokerConfig.OrderTimeout` / `TransferTimeout`
(default `2s`) size these four handlers' deadlines.

### 0a. (NEW this pass) Reachability via `cmd/server/main.go`

A new, untracked file, `cmd/server/main.go`, now exists and calls
`bringup.BringUp` as its first stage — which means these four routes are
reachable end-to-end on a real listener for the first time, **if** that
file is what gets built and run. It is explicitly self-described as an
unreviewed, untested draft (see `README.md` §0 and `cross-cutting.md` §7
for the full detail) — nothing about these four endpoints' own behavior
changed, only the "is there a running process serving this at all"
question, which moved from "no" to "a draft exists." The still-open half
of the old composition-root gap (whether `conductor`'s mux joins the same
listener) is also now resolved by that same file — see `cross-cutting.md`
§7.

**What is still NOT resolved:** `SlippageBufferBps` is still not plumbed
from `BrokerConfig` into `MoneyConfig` anywhere, including in `cmd/server
/main.go`'s own `BrokerConfig` literal (it has no such field to set —
`SlippageBufferBps` lives only on `MoneyConfig`, which `bringup.go`
constructs internally without exposing that field to the caller). See §2.5
and Discrepancy #4.

---

## 1. Conventions (shared with the rest of the broker HTTP surface)

Unchanged from v1.

- JSON over HTTP; `Content-Type: application/json` on every response.
- **No auth.** `account_id` is an explicit body field everywhere.
- **Money**: bare `int64` coins. **Prices**: `int64` ticks.
- **Enums on the wire**: lowercase snake_case strings, never integers.
- Two error envelope shapes:
  | Envelope | Shape | Used when |
  |---|---|---|
  | `error` | `{"error": "<message>"}` | malformed input, timeouts, internal errors |
  | `reason` | `{"reason": "<snake_case tag>"}` | business rejections on place/cancel |
  - The composed modify response has a third shape (§4).
- **Handler owns the deadline** — `cfg.OrderTimeout` / `TransferTimeout`,
  both default **2 seconds** via `bringup.BringUp`.
- **"Accepted, not executed."** Place/cancel/modify return `202`/`200`
  meaning *locally accepted*, not *executed*.
- **Panic safety.** Every `ServeHTTP` recovers and returns `500
  {"error":"internal error"}`.

---

## 2. `POST /v1/orders` — place order

Source: `broker/order/place.go`, path constant `order.PlacePath =
"/v1/orders"`. Unchanged from v1 in every respect.

### Request body

```json
{
  "account_id":   "ACC-abc",
  "instrument":   "RELIANCE",
  "side":         "buy",
  "type":         "limit",
  "tif":          "gtc",
  "qty":          100,
  "limit_price":  245000
}
```

| Field | Type | Required | Default | Enum / Notes |
|---|---|---|---|---|
| `account_id` | string | yes | — | Routing key; empty → `400` |
| `instrument` | string | yes | — | Opaque id; empty → `400` |
| `side` | string | yes | — | `"buy"` \| `"sell"`, case-sensitive; anything else → `400` |
| `type` | string | yes | — | `"limit"` \| `"market"`, case-sensitive; anything else → `400` |
| `tif` | string | yes | — | `"gtc"` \| `"ioc"` \| `"fok"`, case-sensitive; anything else → `400`. **All three parse successfully at the HTTP layer** — see §2.5 on FOK |
| `qty` | int64 | yes | `0` if omitted | `<= 0` → `422 bad_qty` (not `400`) |
| `limit_price` | int64 | conditional | `0` if omitted | Must be `> 0` iff `type == "limit"` (else `422 bad_price`); ignored for market orders |

400-vs-422 split is deliberate: the HTTP layer only rejects on wire-shape
errors; everything else lives in the funnel and surfaces as `422`.

### Success — `202 Accepted`

```json
{ "order_id": "ACC-abc:1" }
```

`order_id` has the shape `"<account_id>:<seq>"`. Treat as opaque.

### Status table

| Status | Body | Exact trigger |
|---|---|---|
| `202` | `{"order_id": "..."}` | `funnel.PlaceAccepted` |
| `400` | `{"error":"invalid json body"}` | JSON decode failure |
| `400` | `{"error":"account_id required"}` | empty `account_id` |
| `400` | `{"error":"instrument required"}` | empty `instrument` |
| `400` | `{"error":"side must be buy or sell"}` | malformed `side` |
| `400` | `{"error":"type must be limit or market"}` | malformed `type` |
| `400` | `{"error":"tif must be gtc, ioc, or fok"}` | malformed `tif` |
| `405` | `{"error":"method not allowed"}` | non-`POST` |
| `409` | `{"reason":"no_session"}` | account has no live session |
| `422` | `{"reason":"<tag>"}` | any other `PlaceRejected` reason (table below) |
| `504` | `{"error":"broker timeout"}` | shard didn't reply inside `cfg.OrderTimeout` |
| `500` | `{"error":"internal error"}` | unrecognized status, or panic |

### `422` reason-tag table (exhaustive)

| Tag | Source condition |
|---|---|
| `unknown_instrument` | instrument not in the session's allowlist |
| `bad_qty` | `order.Qty <= 0` |
| `bad_side` | defensive; HTTP layer already catches malformed strings |
| `bad_price` | limit order with `LimitPrice <= 0` |
| `bad_type` | defensive, same rationale as `bad_side` |
| `insufficient_funds` | Gate 4 rejected — cash or position insufficient, collapsed to one tag |
| `no_market_price` | market buy, `mktCache.BestAsk` missed. See §2.5 |

Check order is fixed (`knownInstrument` → `Qty>0` → `Side` → `Limit price`
→ `Type`); the first failing predicate wins.

### §2.5 Market-buy pricing — conditional, not dead-on-arrival (unchanged from v1)

`broker/marketdata.CacheRegistry` is real and `bringup.go` wires
`marketData.MarketPriceCache()` into `funnel.NewBroker` for real — **this
pass re-confirmed byte-identical to v1's citation.** Market-buy orders can
succeed when:
1. The exchange instance has been `WireInstance`'d (an L7/conductor
   responsibility — confirmed still called on every spin-up in
   `conductor/canonical_spinup.go`/`ondemand_spinup.go`, unaffected by
   this pass's registry-cleanup change to that same file).
2. The instrument has printed at least one ask-side quote.

`no_market_price` remains a real, expected rejection for any cold/unwired
instrument. Stale comments in `broker/order/CLAUDE.md` and `broker/
funnel/mkt_cache.go` (still describing this as permanently blocked) are
unchanged from v1 — still stale, still not fixed, flagged again for
completeness.

When a market buy IS priced: `requiredCash = BestAsk*Qty + SlippageBuffer`.
`SlippageBufferBps` is **still** not plumbed from `BrokerConfig` into
`MoneyConfig` — re-confirmed unchanged, and `cmd/server/main.go`'s own
`BrokerConfig` literal doesn't change this (see §0a). A zero slippage
buffer means market-buy orders reserve with no cushion for the price
moving between place and fill.

### Idempotency

None, unchanged.

---

## 3. `POST /v1/orders/cancel` — cancel order

Source: `broker/order/cancel.go`, path constant `order.CancelPath =
"/v1/orders/cancel"`. Unchanged from v1.

### Request body

```json
{ "account_id": "ACC-abc", "order_id": "ACC-abc:1" }
```

### Success — `202 Accepted`

```json
{ "order_id": "ACC-abc:1" }
```

Echoes the caller's `order_id`. **Not** "cancelled" — the exchange is
authoritative.

### Idempotency

A repeated cancel on an order whose `CancelInFlight` is already `true`
returns the same `202` without re-punching.

### Status table

| Status | Body | Exact trigger |
|---|---|---|
| `202` | `{"order_id": "..."}` | `funnel.CancelPending` |
| `400` | `{"error":"invalid json body"}` | JSON decode failure |
| `400` | `{"error":"account_id required"}` | empty `account_id` |
| `400` | `{"error":"order_id required"}` | empty `order_id` |
| `405` | `{"error":"method not allowed"}` | non-`POST` |
| `409` | `{"reason":"<tag>"}` | any `funnel.CancelFailed` reason |
| `504` | `{"error":"broker timeout"}` | shard timeout |
| `500` | `{"error":"internal error"}` | unexpected error or panic |

### `409` reason-tag table (exhaustive)

| Tag | Exact trigger |
|---|---|
| `no_session` | account has no live session |
| `unknown_order` | no track, or belongs to a different account — deliberately indistinguishable |
| `not_cancellable` | order's status is terminal |

**No release on cancel.** The reservation is released only when the
exchange's echoed `Cancelled` outcome lands at settlement.

---

## 4. `POST /v1/orders/modify` — modify order

Source: `broker/order/modify.go`, path constant `order.ModifyPath =
"/v1/orders/modify"`. Unchanged from v1. **Not a primitive** — composed
cancel-then-conditionally-place under one shared timeout.

### Request body

```json
{
  "account_id": "ACC-abc",
  "order_id":   "ACC-abc:1",
  "instrument": "RELIANCE",
  "side":       "buy",
  "is_market":  false,
  "new_qty":    150,
  "new_price":  246000
}
```

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `account_id` | string | yes | — | empty → `400` |
| `order_id` | string | yes | — | the OLD order to cancel; empty → `400` |
| `instrument` | string | yes | — | empty → `400` |
| `side` | string | yes | — | `"buy"` \| `"sell"`; anything else → `400` |
| `is_market` | bool | yes | `false` | `true` → Market+IOC; `false` → Limit+GTC. **No `tif` field — modify cannot request FOK.** |
| `new_qty` | int64 | yes | `0` | funnel-level validation, same 422-not-400 split |
| `new_price` | int64 | conditional | `0` | required `> 0` iff `is_market == false` |

### Response shape (used on both `200` and `409`)

```json
{
  "modify_status": "ok" | "partial" | "rejected",
  "cancel":  { "status": "pending" | "failed", "order_id": "...", "reason": "..." },
  "replace": { "status": "accepted" | "rejected", "order_id": "...", "reason": "..." } | null,
  "warning": "..."
}
```

### Three outcomes

**`200`, `"ok"`** — both legs locally accepted.
**`200`, `"partial"`** — cancel accepted, replacement rejected:
```json
{
  "modify_status": "partial",
  "cancel":  { "status": "pending",  "order_id": "ACC-abc:1" },
  "replace": { "status": "rejected", "reason":   "insufficient_funds" },
  "warning": "original order is being cancelled; replacement was rejected — you may be left with no order"
}
```
Never rolled back automatically.
**`409`, `"rejected"`** — cancel locally failed; place never attempted.

### Status table

| Status | Body | Exact trigger |
|---|---|---|
| `200` | composed body | `"ok"` or `"partial"` |
| `400` | `{"error":"invalid json body"}` | JSON decode failure |
| `400` | field-specific | missing `account_id`/`order_id`/`instrument`, malformed `side` |
| `405` | `{"error":"method not allowed"}` | non-`POST` |
| `409` | composed body, `"rejected"` | cancel leg gate-rejected |
| `504` | `{"error":"broker timeout (cancel leg)"}` / `"(place leg)"` | leg timeout — **one shared ctx covers both legs** |
| `500` | `{"error":"internal error"}` | unexpected error or panic |

### Idempotency

None — unchanged.

### No pre-cancel funds check

Unchanged — the cancel is punched before any affordability check on the
replacement.

---

## 5. `POST /v1/account/transfer` — bank ↔ trading transfer

Source: `broker/transfer/http.go`, path constant `transfer.Path =
"/v1/account/transfer"`. Unchanged from v1. **Mode-blind**: response shape
identical whether served live or durable.

### Request body

```json
{ "account_id": "ACC-abc", "direction": "to_bank", "amount": 50000, "unit": "" }
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `account_id` | string | yes | empty → `400` |
| `direction` | string | yes | `"to_bank"` \| `"to_trading"`; anything else → `400` |
| `amount` | int64 | yes | must be `> 0` (checked at the HTTP layer — this one IS a `400`) |
| `unit` | string | no | parsed, then completely ignored |

### Success — `200 OK`

```json
{ "available": 123456, "bank": 7890 }
```

### Status table

| Status | Body | Exact trigger |
|---|---|---|
| `200` | `{"available":...,"bank":...}` | live or durable success |
| `400` | field-specific | malformed input |
| `404` | `{"error":"account not found"}` | durable branch only, `store.ErrNoAccount` |
| `405` | `{"error":"method not allowed"}` | non-`POST` |
| `422` | `{"error":"insufficient funds"}` | either branch, identical text |
| `503` | `{"error":"store unavailable"}` | durable branch, `store.ErrLoadFailed` |
| `504` | `{"error":"broker timeout"}` | live-shard timeout |
| `500` | `{"error":"internal error"}` | any other error or panic |

**`409` is never emitted by this endpoint** — explicitly tested
(`TestHTTP_No409_EverEmitted`). Route-first, authoritative, never
client-declared.

### Idempotency

None — each call moves real money.

---

## 6. Order lifecycle state machine

Unchanged from v1. `OrderTrack.Status` (`broker/funnel/track.go`):

```
                    place
                     │
                     ▼
               OrderStatusWorking ──────────────┐
                 │        │                     │
     (partial    │        │ (full fill,         │ (cancel echo,
      fill)      │        │  no partials)       │  CancelledQty==RemainingQty)
                 ▼        ▼                     ▼
   OrderStatusPartiallyFilled ──(fill rest)──► OrderStatusFilled   OrderStatusCancelled
                 │                                                      ▲
                 └──(cancel echo mid-way)──────────────────────────────┘

   OrderStatusRejected  ◄── admission-time exchange reject (no Working state ever reached)
```

Terminal states delete the `OrderTrack` from the shard's tracks map.
`/v1/account/orders` only ever lists non-terminal orders by construction.

Cancel's `202` sets `CancelInFlight = true` but does not change `Status`
until the exchange's async echo lands.

---

## 7. FOK time-in-force — STILL not honored downstream (unresolved, re-confirmed unchanged)

Checked `exchange/match/matcher.go` (`assembleResult`) directly against
the current HEAD — **byte-identical to v1's citation, confirmed by the
`git diff 1c436cc..HEAD` scope check at the top of this file.** There is
still no `case contract.FOK` anywhere in the matcher, the book, or any
other `exchange/` source file. A `fok` order is matched exactly like
`ioc` — partial fills allowed, leftover cancels with the shared
`ioc_leftover` reason, no FOK-specific all-or-nothing semantic.

Practical implication: do not build any UI/logic that depends on
`tif: "fok"` behaving as fill-or-kill.

---

## 8. Numeric encoding notes

Unchanged from v1 — all quantities and prices are `int64`, never float;
transfer's `amount` is `int64` coins; malformed/out-of-range JSON numbers
fail to decode and surface as `400 invalid json body`; omitted fields
decode to Go zero values.

---

## Discrepancies / Open Questions

All eight items carried forward from v1, re-verified unchanged (confirmed
by the `git diff` scope check — none of the files these findings reference
changed since `1c436cc`).

1. **Market-buy price cache — RESOLVED (partially), unchanged.** See §2.5.
2. **FOK time-in-force — UNRESOLVED, confirmed unchanged.** See §7.
3. **`broker/order` not wired into any composition root — RESOLVED
   2026-10-17, and now additionally reachable via the new `cmd/server
   /main.go` draft as of this pass.** See §0a.
4. **`SlippageBufferBps` not plumbed from bringup into `MoneyConfig` —
   still unresolved, re-confirmed this pass**, including against the new
   `cmd/server/main.go`'s own `BrokerConfig` literal, which has no field
   for it. Whether `0` is the deliberate interim default remains an open
   question for the implementer/doc owner, not asserted as a bug.
5. **`BROKER_HTTP_API.md` is itself stale on the market-data section.**
   Unchanged — not re-checked this pass (out of this doc's direct scope),
   carried forward as a known, previously-flagged item.
6. **`OrderTrack` durability / crash-resume of the per-shard `OrderID`
   counter.** Carried, unresolved.
7. **`exitAt = time.Now()` on trade conversion.** Carried, unresolved.
8. **Modify has no pre-cancel funds check and is not idempotent.**
   Confirmed still true. See §4.
