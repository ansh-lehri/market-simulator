# Broker HTTP API

**Audience:** frontend / client developers consuming the broker's HTTP surface.
**Status:** every endpoint listed here is implemented and shipping. Deferred/known limitations are called out inline.

The broker exposes 12 JSON-over-HTTP endpoints covering four areas:

- **Orders** — place, cancel, modify.
- **Account money** — bank ↔ trading transfer.
- **Live account reads** — balance, positions, open orders, composite observation.
- **Durable account reads** — closed trades, P&L summary, cost breakdowns.

---

## Table of contents

1. [Conventions](#1-conventions)
2. [Orders](#2-orders)
   - [2.1 `POST /v1/orders` — place](#21-post-v1orders--place-order)
   - [2.2 `POST /v1/orders/cancel` — cancel](#22-post-v1orderscancel--cancel-order)
   - [2.3 `POST /v1/orders/modify` — modify](#23-post-v1ordersmodify--modify-order)
3. [Account money](#3-account-money)
   - [3.1 `POST /v1/account/transfer` — bank ↔ trading](#31-post-v1accounttransfer--bank--trading-transfer)
4. [Live account reads](#4-live-account-reads)
   - [4.1 `GET /v1/account/balance`](#41-get-v1accountbalance)
   - [4.2 `GET /v1/account/positions`](#42-get-v1accountpositions)
   - [4.3 `GET /v1/account/orders`](#43-get-v1accountorders)
   - [4.4 `GET /v1/account/observation`](#44-get-v1accountobservation)
5. [Durable account reads](#5-durable-account-reads)
   - [5.1 `GET /v1/account/trades`](#51-get-v1accounttrades)
   - [5.2 `GET /v1/account/pnl`](#52-get-v1accountpnl)
   - [5.3 `GET /v1/account/costs`](#53-get-v1accountcosts)
   - [5.4 `GET /v1/account/costs/summary`](#54-get-v1accountcostssummary)
6. [Status-code dictionary](#6-status-code-dictionary)
7. [Client behaviors you MUST know](#7-client-behaviors-you-must-know)

---

## 1. Conventions

### Transport
- All requests/responses are JSON.
- `Content-Type: application/json` on every request body **and** response body.
- Request bodies on `POST` endpoints; query strings on `GET` endpoints.

### Identity
- **No auth.** `account_id` is the identity param — passed in the query string on `GET`, in the request body on `POST`. The server trusts it. (Fine for the demo surface; a real deploy would gate this.)

### Values
- **Money**: every monetary field is a bare `int64` in minor units (paise for IN). There is NO `unit` / `currency` field anywhere.
- **Prices**: integer ticks, `int64`.
- **Timestamps**: RFC3339 strings (e.g. `"2026-10-09T12:34:56Z"`).
- **Enums on the wire** are stable lowercase snake_case strings (`"buy"`, `"working"`, `"to_bank"`, …). Not integers.

### Error envelopes

Two shapes depending on the kind of failure:

| Envelope | Shape | Used when |
|---|---|---|
| `error` | `{"error": "<human message>"}` | Infrastructure, validation, timeouts, auth-ish failures |
| `reason` | `{"reason": "<snake_case tag>"}` | **Business rejections** on place/cancel/modify — client dispatches on `reason` |

The composed modify response has a third shape with embedded `cancel` and `replace` sub-objects (see §2.3).

### Timeouts
- Every handler owns its own request deadline.
- A live-path round-trip that exceeds the deadline → `504 Gateway Timeout`.
- A durable-only read in the same condition → `503 Service Unavailable` (durable reads **never** return `504`).

### "Accepted, not executed"
Order endpoints (`POST /v1/orders`, `POST /v1/orders/cancel`) return `202` meaning *locally accepted and sent to the exchange*. The real terminal outcome (filled, cancelled, rejected) arrives asynchronously. The client must poll `/v1/account/orders` and `/v1/account/trades` to see terminal state.

### Live vs durable semantics
- **Live reads** go through the shard that owns the account; `409 Conflict` means "no live session." They cannot tell a brand-new account from a logged-out one.
- **Durable reads** go straight to Postgres; an unknown account returns `200` with empty data — never `404`, never `409`.

---

## 2. Orders

### 2.1 `POST /v1/orders` — place order

Submit a new order. Reserves funds / units, mints an `OrderID`, punches to the exchange.

#### Request body
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

| Field | Type | Required | Notes |
|---|---|---|---|
| `account_id` | string | yes | Routing key — hashes to the owning shard |
| `instrument` | string | yes | Opaque instrument id |
| `side` | enum | yes | `"buy"` or `"sell"` |
| `type` | enum | yes | `"limit"` or `"market"` |
| `tif` | enum | yes | `"gtc"`, `"ioc"`, or `"fok"` (FOK is accepted on the wire but rejected by the exchange today) |
| `qty` | int64 | yes | Whole units; must be `> 0` |
| `limit_price` | int64 | conditional | Ticks. Required `> 0` iff `type == "limit"`; ignored for market orders |

#### Success — `202 Accepted`
```json
{ "order_id": "ACC-abc:1" }
```

`order_id` has the shape `"<account_id>:<seq>"`. Treat it as opaque and echo it back on cancel/modify.

#### Errors

| Status | Body | When |
|---|---|---|
| `400 Bad Request` | `{"error":"..."}` | Malformed JSON; missing `account_id`/`instrument`; `side`/`type`/`tif` not in the allowed sets |
| `405 Method Not Allowed` | `{"error":"method not allowed"}` | Non-`POST` request |
| `409 Conflict` | `{"reason":"no_session"}` | Account has no live session |
| `422 Unprocessable Entity` | `{"reason":"<tag>"}` | Business reject (see below) |
| `504 Gateway Timeout` | `{"error":"broker timeout"}` | Shard did not reply before the deadline |
| `500 Internal Server Error` | `{"error":"internal error"}` | Unexpected broker error, or a recovered panic |

**`422` reason tags**:

| Tag | Meaning |
|---|---|
| `unknown_instrument` | Instrument not in the session's allowlist |
| `bad_qty` | `qty <= 0` |
| `bad_side` | Side not Buy/Sell (defensive — `400` catches most on the wire) |
| `bad_price` | Limit order with `limit_price <= 0` |
| `bad_type` | Type not Limit/Market (defensive) |
| `insufficient_funds` | Gate 4 reservation reject. **Collapses "insufficient cash" and "insufficient position units" into one tag** — the server does not split funds-vs-units at the HTTP surface |
| `no_market_price` | Market buy with no cached best-ask. See [known limitations](#known-limitations) |

---

### 2.2 `POST /v1/orders/cancel` — cancel order

Pulls a working or partially-filled order. POST-with-body (not `DELETE /orders/{id}`) by design — the no-auth demo keeps `account_id` an explicit field everywhere.

#### Request body
```json
{
  "account_id": "ACC-abc",
  "order_id":   "ACC-abc:1"
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `account_id` | string | yes | |
| `order_id` | string | yes | The order to cancel (must belong to `account_id`) |

#### Success — `202 Accepted`
```json
{ "order_id": "ACC-abc:1" }
```

`202` means *the cancel was accepted locally and punched to the exchange*. **It does NOT mean "cancelled"** — the exchange is authoritative. The real outcome (`cancelled` or `cancel_rejected`) arrives async; a fill may have beaten the cancel.

#### Idempotency
A repeated cancel while one is already in flight returns the same `202 CancelPending` without re-punching to the exchange.

#### Errors

| Status | Body | When |
|---|---|---|
| `400` | `{"error":"..."}` | Malformed JSON; missing `account_id` or `order_id` |
| `405` | `{"error":"method not allowed"}` | Non-`POST` |
| `409 Conflict` | `{"reason":"<tag>"}` | Any CancelFailed reason (**uniform** — unlike place, cancel never emits 422) |
| `504` | `{"error":"broker timeout"}` | Timeout |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

**`409` reason tags**:

| Tag | Meaning |
|---|---|
| `no_session` | No live session for the account |
| `unknown_order` | Order not found OR found but belongs to a different account. **The two are indistinguishable on purpose** (don't leak the existence of other users' orders) |
| `not_cancellable` | Order is in a terminal state (`filled` / `cancelled` / `rejected`) |

---

### 2.3 `POST /v1/orders/modify` — modify order

Modify is **not a primitive** — it is composed as "cancel the old order, if the cancel is locally accepted then place a replacement." Gated on local acceptance of the cancel: a locally-failed cancel never leaks a naked new order.

#### Request body
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

| Field | Type | Required | Notes |
|---|---|---|---|
| `account_id` | string | yes | |
| `order_id` | string | yes | The OLD order to cancel |
| `instrument` | string | yes | |
| `side` | enum | yes | `"buy"` or `"sell"` |
| `is_market` | bool | yes | `true` → `IOC` market replacement; `false` → `GTC` limit replacement |
| `new_qty` | int64 | yes | |
| `new_price` | int64 | conditional | Required `> 0` iff `is_market == false`; ignored for market |

Note the shape **differences from `POST /v1/orders`**: `is_market` bool instead of `type` enum string; `new_price` instead of `limit_price`; no `tif` field (derived from `is_market`). Modify does not support FOK.

#### Response shape
```json
{
  "modify_status": "ok" | "partial" | "rejected",
  "cancel":  { "status": "...", "order_id": "...", "reason": "..." },
  "replace": { "status": "...", "order_id": "...", "reason": "..." } | null,
  "warning": "..."
}
```

#### Three outcomes

**`200 OK` — modify_status: `"ok"`** (both legs locally accepted)
```json
{
  "modify_status": "ok",
  "cancel":  { "status": "pending",  "order_id": "ACC-abc:1" },
  "replace": { "status": "accepted", "order_id": "ACC-abc:2" }
}
```

**`200 OK` — modify_status: `"partial"`** (cancel accepted; Gate 4 rejected the replacement)
```json
{
  "modify_status": "partial",
  "cancel":  { "status": "pending",  "order_id": "ACC-abc:1" },
  "replace": { "status": "rejected", "reason":   "insufficient_funds" },
  "warning": "original order is being cancelled; replacement was rejected — you may be left with no order"
}
```
**Partial is reported, never rolled back** — the client MUST surface the warning. The cancel is in flight (likely to succeed) but the replacement never went out. The user may end up with no order at all.

**`409 Conflict` — modify_status: `"rejected"`** (cancel locally failed → no place attempted)
```json
{
  "modify_status": "rejected",
  "cancel":  { "status": "failed", "reason": "unknown_order" },
  "replace": null
}
```

#### Errors

| Status | Body | When |
|---|---|---|
| `200` | composed body | `modify_status: "ok"` or `"partial"` |
| `400` | `{"error":"..."}` | Malformed body; missing `account_id`/`order_id`/`instrument`; bad `side` |
| `405` | `{"error":"method not allowed"}` | Non-`POST` |
| `409` | composed body | `modify_status: "rejected"` (cancel locally failed) |
| `504` | `{"error":"broker timeout (cancel leg)"}` or `"...(place leg)"` | Timeout on either leg (one ctx bounds both) |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

---

## 3. Account money

### 3.1 `POST /v1/account/transfer` — bank ↔ trading transfer

Routes to the live session if the account is live on a shard (in-memory `MoveCash` on the owner goroutine); otherwise falls through to a guarded Postgres `UPDATE`. Response shape is **mode-blind** — the client cannot tell live from durable.

#### Request body
```json
{
  "account_id": "ACC-abc",
  "direction":  "to_bank",
  "amount":     50000,
  "unit":       ""
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `account_id` | string | yes | |
| `direction` | enum | yes | `"to_bank"` (trading→bank) or `"to_trading"` (bank→trading) |
| `amount` | int64 | yes | Integer coins; must be `> 0` |
| `unit` | string | no | **Parsed but IGNORED** (reserved for future unit vocabulary) |

#### Success — `200 OK`
```json
{ "available": 123456, "bank": 7890 }
```
Post-move balances. `available` is trading-cash available; `bank` is the bank bucket. **Same shape whether the money moved on the live shard or the durable row.**

#### Errors

| Status | Body | When |
|---|---|---|
| `400` | `{"error":"..."}` | Malformed JSON; missing `account_id`; `direction` not in `{to_bank, to_trading}`; `amount <= 0` |
| `404 Not Found` | `{"error":"account not found"}` | **Durable branch only** — account row does not exist |
| `405` | `{"error":"method not allowed"}` | Non-`POST` |
| `422` | `{"error":"insufficient funds"}` | Source bucket lacks `amount`. **Same body text whether live or durable** (mode-blind) |
| `503 Service Unavailable` | `{"error":"store unavailable"}` | Postgres unavailable on the durable branch |
| `504` | `{"error":"broker timeout"}` | Live-shard round-trip timed out |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

The server **never** emits `409` on this endpoint.

---

## 4. Live account reads

All four live reads share the same request shape:
- Method: `GET`.
- Required query param: `account_id`.
- `409 Conflict` means **no live session on any shard**; it is NOT "account doesn't exist."
- `504` means the shard did not reply before `ReadTimeout`.

### 4.1 `GET /v1/account/balance`

Returns the four `Cash` buckets of the live account.

#### Query
```
?account_id=<id>   (required)
```

#### Success — `200 OK`
```json
{
  "available": 100000,
  "reserved":  25000,
  "settled":   0,
  "bank":      500000
}
```

| Field | Type | Notes |
|---|---|---|
| `available` | int64 | Trading cash available to reserve. **May be negative** (debt is legal in the ledger) |
| `reserved` | int64 | Cash locked behind resting buy orders |
| `settled` | int64 | Session-only bucket tracking Reserved→position conversions |
| `bank` | int64 | Bank-side cash |

#### Errors

| Status | Body | When |
|---|---|---|
| `400` | `{"error":"account_id required"}` | Missing `account_id` |
| `405` | `{"error":"method not allowed"}` | Non-`GET` |
| `409` | `{"error":"no live session for account"}` | No live session |
| `504` | `{"error":"balance read timed out"}` | Shard timeout |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

---

### 4.2 `GET /v1/account/positions`

#### Query
```
?account_id=<id>              (required)
?instrument=<id>              (optional — single-instrument filter)
```

Empty/absent `instrument` returns every held instrument. A specific instrument the account doesn't hold returns `200` with an empty slice (flat on an instrument is not an error).

#### Success — `200 OK`
```json
{
  "positions": [
    {
      "instrument":          "RELIANCE",
      "qty":                 100,
      "reserved_qty":        20,
      "weighted_avg_entry":  245000,
      "mark_available":      false,
      "ltp":                 0,
      "gross_unrealized":    0
    }
  ]
}
```

| Field | Type | Notes |
|---|---|---|
| `instrument` | string | |
| `qty` | int64 | Free-to-sell units (`Position.Available`) |
| `reserved_qty` | int64 | Units locked behind resting sell orders |
| `weighted_avg_entry` | int64 | Qty-weighted avg entry in ticks. `0` iff flat |
| `mark_available` | bool | `false` whenever the market-data cache misses. **Currently always `false`** in production until the tape relay (Phase 7) lands |
| `ltp` | int64 | Last-traded price. `0` when `mark_available == false` |
| `gross_unrealized` | int64 | Signed P&L mark. `0` when `mark_available == false` |

A `net_unrealized` field is intentionally **not** present (deferred — needs an L5 write-path change).

#### Errors — same table as [`/balance`](#41-get-v1accountbalance).

---

### 4.3 `GET /v1/account/orders`

Returns the **working set only** — orders in `working` or `partially_filled` state. Terminal orders are removed from the shard at settlement; historical executions live under [`/v1/account/trades`](#51-get-v1accounttrades).

#### Query
```
?account_id=<id>   (required)
```

#### Success — `200 OK`
```json
{
  "orders": [
    {
      "order_id":         "ACC-abc:1",
      "instrument":       "RELIANCE",
      "side":             "buy",
      "is_market":        false,
      "limit_price":      245000,
      "orig_qty":         100,
      "remaining_qty":    60,
      "filled_qty":       40,
      "reserved_cash":    14700000,
      "reserved_units":   0,
      "status":           "partially_filled",
      "cancel_in_flight": false
    }
  ]
}
```

| Field | Type | Notes |
|---|---|---|
| `order_id` | string | |
| `instrument` | string | |
| `side` | enum | `"buy"` or `"sell"` |
| `is_market` | bool | |
| `limit_price` | int64 | Ticks. `0` for market orders |
| `orig_qty` | int64 | Qty when the order was accepted |
| `remaining_qty` | int64 | Qty still live on the exchange |
| `filled_qty` | int64 | Derived: `orig_qty - remaining_qty` |
| `reserved_cash` | int64 | Cash still reserved for this order (`0` for sells) |
| `reserved_units` | int64 | Units still reserved for this order (`0` for buys) |
| `status` | enum | `"working"` or `"partially_filled"` (terminals filtered out) |
| `cancel_in_flight` | bool | A cancel has been punched; echo not yet landed |

#### Errors — same table as [`/balance`](#41-get-v1accountbalance).

---

### 4.4 `GET /v1/account/observation`

**The preferred dashboard endpoint.** Fans balance + positions + open orders atomically to the owning shard in one visit (so the three sub-payloads cannot interleave with a fill), **plus** a concurrent durable read for the 20 most recent closed trades.

#### Query
```
?account_id=<id>   (required)
```

#### Success — `200 OK`
```json
{
  "balance":   { "available": 100000, "reserved": 25000, "settled": 0, "bank": 500000 },
  "positions": [ /* same shape as /positions */ ],
  "orders":    [ /* same shape as /orders    */ ],
  "recent_trades": [ /* up to 20 rows, newest-first; same shape as /trades */ ],
  "trades_degraded": false
}
```

- `trades_degraded: true` means the durable half failed. `recent_trades` will be `[]` but the live half is still correct. **Response is still `200`.** The frontend should surface this honestly (e.g. grey out the "recent trades" strip).

#### Errors

Only the **live** leg drives error codes. A durable failure degrades to `200` with `trades_degraded: true`.

| Status | Body | When |
|---|---|---|
| `400` | `{"error":"account_id required"}` | Missing `account_id` |
| `405` | `{"error":"method not allowed"}` | Non-`GET` |
| `409` | `{"error":"no live session for account"}` | No live session |
| `504` | `{"error":"observation read timed out"}` | Shard timeout |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

---

## 5. Durable account reads

All four durable reads are Postgres-only. No live session concept. Unknown or brand-new account → `200` with empty data. `503` means store unavailable. Durable reads **never** return `504`.

### 5.1 `GET /v1/account/trades`

Keyset-paginated list of closed trades, newest-first by `exit_at`.

#### Query
```
?account_id=<id>                   (required)
?limit=<int>                        (optional, default 50; must be > 0 if present)
?before=<RFC3339 timestamp>         (optional; default = newest page)
```

Walking older pages: pass the previous response's `next_before` back as `before`. The walk ends when the response returns a short/empty page (`next_before: null`).

#### Success — `200 OK`
```json
{
  "trades": [
    {
      "trade_id":    "ACC-abc:1:1",
      "instrument":  "RELIANCE",
      "direction":   "long",
      "quantity":    40,
      "entry_price": 244000,
      "exit_price":  246000,
      "entry_at":    "2026-10-09T10:00:00Z",
      "exit_at":     "2026-10-09T10:14:00Z",
      "gross_pnl":   80000,
      "total_costs": 350,
      "net_pnl":     79650,
      "pnl_pct":     1.0082,
      "exit_reason": "fifo_match"
    }
  ],
  "next_before": "2026-10-09T10:14:00Z"
}
```

| Field | Type | Notes |
|---|---|---|
| `trade_id` | string | |
| `instrument` | string | |
| `direction` | enum | `"long"` or `"short"` |
| `quantity` | int64 | |
| `entry_price` | int64 | Ticks |
| `exit_price` | int64 | Ticks |
| `entry_at` | RFC3339 | |
| `exit_at` | RFC3339 | |
| `gross_pnl` | int64 | Signed |
| `total_costs` | int64 | ≥ 0 |
| `net_pnl` | int64 | Signed |
| `pnl_pct` | float32 | Matches the DB `REAL` column precision |
| `exit_reason` | string | |
| `next_before` | RFC3339 \| null | Cursor for next page, or `null` meaning "last page" |

#### Errors

| Status | Body | When |
|---|---|---|
| `200` | normal body with `"trades":[]`, `"next_before":null` | Account has no trades (**empty is not an error**) |
| `400` | `{"error":"..."}` | Missing `account_id`; `limit` zero/negative/unparseable; `before` not RFC3339 |
| `405` | `{"error":"method not allowed"}` | Non-`GET` |
| `503` | `{"error":"store unavailable"}` | Postgres unavailable / ctx cancelled |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

No `504` on this endpoint.

---

### 5.2 `GET /v1/account/pnl`

Account-wide P&L aggregate. Separate from `/trades` so the frontend doesn't recompute the summary while paging.

#### Query
```
?account_id=<id>   (required)
```

#### Success — `200 OK`
```json
{
  "trade_count": 42,
  "gross_pnl":   128000,
  "total_costs": 3500,
  "net_pnl":     124500,
  "win_count":   28,
  "loss_count":  10
}
```

| Field | Type | Notes |
|---|---|---|
| `trade_count` | int | Total closed trades |
| `gross_pnl` | int64 | Signed sum |
| `total_costs` | int64 | ≥ 0 sum |
| `net_pnl` | int64 | Signed sum |
| `win_count` | int | Rows with `net_pnl > 0` |
| `loss_count` | int | Rows with `net_pnl < 0` |

**Breakeven trades** (`net_pnl == 0`) are counted in neither `win_count` nor `loss_count`. So `trade_count - win_count - loss_count = breakeven_count`.

#### Errors — same table as [`/trades`](#51-get-v1accounttrades). Empty-account returns a zero-filled `200`.

---

### 5.3 `GET /v1/account/costs`

Per-trade cost breakdown. **Country-dispatched.**

#### Query
```
?account_id=<id>                      (required)
?country=<code>                       (required — CASE-SENSITIVE, two-letter uppercase)
?limit=<int>                           (optional, default 50)
?before=<RFC3339 timestamp>            (optional; newest page by default)
```

#### Country handling

| `country` value | Behavior |
|---|---|
| `IN` | Routes to India breakdown (STT, exchange, SEBI, stamp, GST, autosquare_fee) |
| `US` | `501 Not Implemented` (slot defined, no implementation yet) |
| anything else, missing, lowercase `in` | `400` |

#### Success — `200 OK`
```json
{
  "trades": [
    {
      "trade_id": "ACC-abc:1:1",
      "lines": [
        { "name": "stt",            "amount": 150 },
        { "name": "exchange",       "amount": 20  },
        { "name": "sebi",           "amount": 1   },
        { "name": "stamp",          "amount": 36  },
        { "name": "gst",            "amount": 65  }
      ],
      "total": 272
    }
  ],
  "next_before": "2026-10-09T10:14:00Z"
}
```

- `lines` includes **only non-zero lines** — a line that was zero on this trade is omitted entirely (e.g. a buy-side trade won't carry an `stt` row because STT is sell-only).
- `brokerage` is NOT in the per-trade breakdown (it is charged at the order level, always `0` per trade).
- `total` is the stored total, never recomputed.
- Line names are **country-specific**. IN emits: `stt`, `exchange`, `sebi`, `stamp`, `gst`, `autosquare_fee`.

#### Errors

| Status | Body | When |
|---|---|---|
| `200` | normal body with `"trades":[]` | Empty is not an error |
| `400` | `{"error":"..."}` | Missing `account_id`/`country`; unknown country; bad `limit`/`before` |
| `405` | `{"error":"method not allowed"}` | Non-`GET` |
| `501 Not Implemented` | `{"error":"country not available"}` | `country=US` |
| `503` | `{"error":"store unavailable"}` | Postgres unavailable |
| `500` | `{"error":"internal error"}` | Unexpected / panic |

---

### 5.4 `GET /v1/account/costs/summary`

Account-wide cost aggregate. Separate from `/costs` for the same reason `/pnl` is split out — the frontend shouldn't recompute the aggregate while paging.

#### Query
```
?account_id=<id>   (required)
?country=<code>    (required — same rules as /costs)
```

#### Success — `200 OK`
```json
{
  "trade_count": 42,
  "lines": [
    { "name": "stt",            "amount": 4200 },
    { "name": "exchange",       "amount": 520  },
    { "name": "sebi",           "amount": 15   },
    { "name": "stamp",          "amount": 980  },
    { "name": "gst",            "amount": 1850 }
  ],
  "total": 7565
}
```

Same quarantine rules as `/costs`: only non-zero lines surface; `total` is projected from the stored column.

#### Errors — same table as [`/costs`](#53-get-v1accountcosts).

---

## 6. Status-code dictionary

| Code | Meaning in this API |
|---|---|
| `200 OK` | Success. Durable reads use this even for empty results |
| `202 Accepted` | Order action locally accepted and sent to the exchange; terminal outcome arrives async |
| `400 Bad Request` | Client bug — malformed JSON, missing field, unparseable enum, bad `limit`/`before`, unknown country |
| `404 Not Found` | **Only on `POST /v1/account/transfer` durable branch** — account row does not exist. Live reads never emit 404 |
| `405 Method Not Allowed` | Wrong HTTP method. The `Allow` header carries the right one |
| `409 Conflict` | *Live reads*: no live session for the account. *Place*: `no_session`. *Cancel*: any CancelFailed reason. *Modify*: cancel leg locally failed. **Never emitted by `/transfer`** |
| `422 Unprocessable Entity` | Business reject. *Place*: validation / insufficient / no_market_price. *Transfer*: insufficient funds (live or durable) |
| `500 Internal Server Error` | Unexpected server error or recovered panic. Retry sparingly / escalate |
| `501 Not Implemented` | `country=US` on cost endpoints. Slot defined, no implementation yet — don't retry |
| `503 Service Unavailable` | Durable store unavailable (any failure on the Postgres path, including ctx cancel). Short retry is appropriate |
| `504 Gateway Timeout` | Live-shard round-trip exceeded the handler's deadline. **The command may still have taken effect** on the broker — reconcile via a pull-based read; do NOT blindly retry a `POST` |

---

## 7. Client behaviors you MUST know

1. **`202` is not "done".** The exchange's authoritative outcome arrives async. For a definitive terminal state (filled / cancelled / rejected), poll `/v1/account/orders` and `/v1/account/trades`. A placed order can still be rejected by the exchange and show up as `rejected` later.

2. **`504` does not mean "nothing happened".** The shard may still have processed the command — the caller just didn't stay to see the reply. **Always reconcile with a read before retrying a `POST`.**

3. **Modify's `partial`** can leave the user with no order at all. Surface the warning string verbatim.

4. **`mark_available: false`** is the current steady state on positions until the market-data cache lands. Don't show "N/A" angrily; show the position without an unrealized number.

5. **`trades_degraded: true`** on observation means the durable half failed; the live half is still trustworthy.

6. **`unknown_order` on cancel** can mean "doesn't exist" OR "belongs to someone else" — the server merges them deliberately. Don't tell the user "that order belongs to another account."

7. **`insufficient_funds` on place** merges "insufficient cash" and "insufficient position units" into one reason. Message should be generic.

8. **Observation is the preferred dashboard read** — one HTTP call, atomic snapshot. **Avoid issuing `/balance + /positions + /orders` as three separate requests** (they can interleave with a fill).

9. **Pagination cursor** on `/trades` and `/costs`: pass the previous response's `next_before` as `?before=`. Walk ends when `next_before: null`.

10. **No auth**: every request carries `account_id` explicitly. The server trusts it.

---

## Known limitations

These are shipping behaviors the frontend should know about today:

- **Market buys reject with `no_market_price`** until the market-data cache (tape relay, Phase 7) is wired.
- **Position marks degrade with `mark_available: false`** — same reason as above. The position itself, reserved qty, and weighted-avg entry still ship correctly.
- **`net_unrealized` field is absent** on position rows (deferred; needs an L5 write-path change for per-position incurred-entry-costs).
- **FOK (`"tif": "fok"`)** is accepted on the wire but rejected downstream by the exchange.
- **Only `country=IN` is implemented** on cost endpoints; `country=US` honestly returns `501`.

---

## Not yet exposed — market data (Phase 7)

**The broker currently exposes NO market-data endpoints.** Specifically, there is no:

- `GET /v1/market/depth` / order book snapshot
- `GET /v1/market/quote` (best-bid/best-ask/last-price)
- `GET /v1/market/trades` (public trade prints — distinct from the account-scoped `/v1/account/trades`)
- `GET /v1/market/candles` (OHLCV)
- Any WebSocket/SSE streaming endpoint

This is **by design, not an oversight**. The exchange's `Public` channel carries all of this (trade prints, quote/depth snapshots, OHLCV candles) — the broker is specified as the relay to end-users (`docs/broker/Exchange_Broker_Contract.md`), but the broker-side components that would consume `Public` and expose it are collectively deferred to **Phase 7** (`BROKER_PHASE4_IMPLEMENTATION_DESIGN.md §6`):

| Component | Role | Status |
|---|---|---|
| **Ingestor** | One goroutine per exchange; drains `exchange.Public`, stamps `ExchangeID`, feeds the cache + CandleStore + FanOut | Not built |
| **MarketPriceCache** | `BestBid` / `BestAsk` / `LastPrice` per `(ExchangeID, InstrumentID)` | Interface stub only (`BestAsk` + `LastPrice`); writer not built → wired to a no-op cache that always misses |
| **CandleStore** | 1-min OHLCV `Insert` + `RangeRead1m` on a durable `candles` table | Not built |
| **Streaming FanOut** | Push live quotes / depth / trades / candles to connected clients | Not built — the design implies push (WS/SSE), not GET |

**What this means for the frontend today:**
- No way to render an order book / depth ladder.
- No way to render a candlestick chart.
- No last-trade ticker.
- `/v1/account/positions` cannot show a live unrealized P&L mark (`mark_available: false` always).
- Market-buy orders fail (`no_market_price`).

**What the frontend should plan for in Phase 7:**
- The design points at a **streaming** market-data surface (FanOut), not polled REST. Plan the client around a WebSocket/SSE subscription for quotes/depth/trades/candles, not a request-response loop.
- Exact endpoint shapes are **not yet specified** in any doc — the FanOut is listed in the Phase-7 scope but no API contract has been drafted for it. When it is, this doc will be updated.
