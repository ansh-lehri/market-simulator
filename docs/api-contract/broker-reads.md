# API Contract — `broker/reads/` (Tier-4 read surfaces) (v2)

**Status: refreshed 2026-10-23, re-verified against this repo's git
history up to commit `025f365`.** Supersedes `docs/api-contract/
broker-reads.md` (v1, 2026-10-17 against `1c436cc`). `git diff
1c436cc..HEAD` confirms **zero changes** to `broker/reads/` or any package
it depends on (`broker/funnel`, `broker/store`) since v1's last pass. Every
file v1 cited (`balance.go`, `positions.go`, `orders.go`, `observation.go`,
`trades.go`, `costs.go`, `account_cash.go`, `common.go`, `shaping.go`,
`ports.go`, `config.go`) was re-read for this pass and confirmed
byte-identical. **This entire document's technical content is a
verified-unchanged carry-forward.** The one addition is a note on
reachability via the new `cmd/server/main.go` draft (§0).

Covers balance, positions, open orders, composite observation, closed
trades, P&L, cost/tax breakdowns, and the pre-session cash view.

## 0. (NEW this pass) Reachability

All nine endpoints below are mounted inside `bringup.BringUp`'s mux
(unchanged — see `broker-order-transfer.md` §0), and as of this pass that
mux is joined to `conductor`'s mux by the new, untracked, unreviewed
`cmd/server/main.go` draft. See `README.md` §0 and `cross-cutting.md` §7
for the full detail. No endpoint behavior below is affected — this is
purely a "is anything actually listening" update.

## 1. Standing rules (apply to every endpoint below)

Unchanged from v1.

- **No auth.** `account_id` is a required, trusted query parameter on
  every endpoint. No ownership check.
- **All methods are GET.** Non-GET → `405`, `Allow: GET`.
- **Error envelope**: `{ "error": "<message>" }` on every non-2xx.
- **Money/ticks:** bare `int64`, no `unit` field anywhere (deliberate
  divergence from the Tier-4 design doc — see Discrepancy #1).
- **Two independent read paths:**
  - **Live reads** (balance, positions, open orders, observation's live
    half): `funnel.ErrBrokerTimeout` → 504; other broker error → 500;
    `Present:false` → 409 `{"error":"no live session for account"}`.
  - **Durable reads** (trades, pnl, costs, costs/summary, observation's
    trades half, cash): `store.ErrLoadFailed` → 503; other store error →
    500; **no 409 and no 404** on any durable-only endpoint **except**
    `/v1/account/cash` (§9).
- **Context/timeout:** `context.WithTimeout(ctx, cfg.ReadTimeout)`,
  operator-set, no package default.

## 1. `GET /v1/account/balance`

Unchanged from v1. Source: `balance.go`. Route constant
`reads.PathBalance`.

| Param | Required | Notes |
|---|---|---|
| `account_id` | yes | Missing/empty → `400`. |

| Status | Trigger |
|---|---|
| 200 | Live session found. |
| 400 | `account_id` missing. |
| 405 | Non-GET. |
| 409 | No live session. |
| 500 | Non-timeout broker error. |
| 504 | `funnel.ErrBrokerTimeout`. |

```json
{ "available": 125000, "reserved": 30000, "settled": 0, "bank": 500000 }
```
`available` may be negative (debt is legal, L5 Option-4). No derived
fields.

## 2. `GET /v1/account/positions`

Unchanged from v1. Source: `positions.go`. Route constant
`reads.PathPositions`.

| Param | Required | Notes |
|---|---|---|
| `account_id` | yes | Missing/empty → 400. |
| `instrument` | no | Exact-match filter; unheld instrument → `200 {"positions":[]}`, not an error. |

| Status | Trigger |
|---|---|
| 200 | Live session found (may be `[]`). |
| 400 | `account_id` missing. |
| 405 | Non-GET. |
| 409 | No live session. |
| 500 | Non-timeout broker error. |
| 504 | `funnel.ErrBrokerTimeout`. |

Each row (`funnel.PositionView`):

| Field | Type | Semantics |
|---|---|---|
| `instrument` | string | |
| `qty` | int64 | free-to-sell units |
| `reserved_qty` | int64 | locked behind resting sells |
| `weighted_avg_entry` | int64 | display-only, FIFO remains the realization basis |
| `mark_available` | bool | see note below — conditional, not permanently false |
| `ltp` | int64 | last-traded tick the mark is computed against; `0` when unavailable |
| `gross_unrealized` | int64 | gross unrealized P&L; `0` when unavailable. No net-unrealized field, intentionally. |

A fully flat position is skipped entirely, never a zero row.

### `mark_available` — conditional, not unconditionally false (unchanged from v1)

`broker/bringup/bringup.go` passes the **real** `*marketdata.CacheRegistry`
into `funnel.NewBroker` — re-confirmed byte-identical to v1's citation.
`mark_available` is `true` once (a) `marketData.WireInstance` has run for
the instrument's exchange instance (confirmed still called on every
spin-up in `conductor/canonical_spinup.go`/`ondemand_spinup.go` — note
`ondemand_spinup.go` changed this pass, but only in its registry-cleanup
logic on setup failure, not in its `WireInstance` call, which is
unaffected) and (b) the instrument has printed at least one trade. **Not**
permanently `false`.

```json
{ "positions": [ { "instrument": "ACME", "qty": 100, "reserved_qty": 0, "weighted_avg_entry": 15000, "mark_available": false, "ltp": 0, "gross_unrealized": 0 } ] }
```
(Example instrument updated to `ACME` to match the one market config this
repo now ships, `configs/markets/IN.json` — see `README.md` §0a. Any
instrument id works identically; this is cosmetic, not a behavior note.)

## 3. `GET /v1/account/orders`

Unchanged from v1. Source: `orders.go`. Route constant `reads.PathOrders`.

| Param | Required | Notes |
|---|---|---|
| `account_id` | yes | Missing/empty → 400. |

**Still no `instrument` filter**, despite the Tier-4 doc specifying one.
Same four-way status mapping as balance/positions.

Each row (`funnel.OpenOrderView`):

| Field | Type | Semantics |
|---|---|---|
| `order_id` | string | `"<AccountID>:<seq>"` |
| `instrument` | string | |
| `side` | string enum | `"buy"` \| `"sell"` \| `"unknown"` |
| `is_market` | bool | |
| `limit_price` | int64 | ticks; `0` for market orders — **confirmed still populated**, the Tier-4 doc's "not retained" claim is still false |
| `orig_qty` | int64 | |
| `remaining_qty` | int64 | |
| `filled_qty` | int64 | derived at the HTTP edge: `orig_qty - remaining_qty` |
| `reserved_cash` | int64 | `0` for sell reservations |
| `reserved_units` | int64 | `0` for buy reservations |
| `status` | string enum | see table below |
| `cancel_in_flight` | bool | |

`status` enum, int-backed:

| Go const | int | JSON string |
|---|---|---|
| `OrderStatusUnknown` | 0 | `"unknown"` |
| `OrderStatusWorking` | 1 | `"working"` |
| `OrderStatusPartiallyFilled` | 2 | `"partially_filled"` |
| `OrderStatusFilled` | 3 | `"filled"` |
| `OrderStatusCancelled` | 4 | `"cancelled"` |
| `OrderStatusRejected` | 5 | `"rejected"` |

Only `Working`/`PartiallyFilled` can appear — terminal orders are deleted
from the shard's track map at settlement.

## 4. `GET /v1/account/observation`

Unchanged from v1. Source: `observation.go`. Route constant
`reads.PathObservation`.

| Param | Required | Notes |
|---|---|---|
| `account_id` | yes | Missing/empty → 400. |

**Still no `recent_n` param** — hardcodes `observationRecentTradesLimit =
20`.

Fans out two concurrent reads on one shared deadline: `broker
.ReadObservation` (live) and `durableStore.RecentTrades(ctx, accountID,
20)` (durable). Only the live leg drives HTTP status.

| Status | Trigger |
|---|---|
| 200 | Live leg succeeded. Trades leg may have degraded — see below. |
| 400 | `account_id` missing. |
| 405 | Non-GET. |
| 409 | Live leg not present. |
| 500 | Live leg non-timeout error. |
| 504 | Live leg `funnel.ErrBrokerTimeout`. |

```json
{
  "balance": { "available": 125000, "reserved": 0, "settled": 0, "bank": 500000 },
  "positions": [],
  "orders": [],
  "recent_trades": [],
  "trades_degraded": false
}
```
`trades_degraded` is `true` iff `RecentTrades` returned any error. No
top-level `present` field in the success body (unchanged divergence from
the Tier-4 doc).

## 5. `GET /v1/account/trades`

Unchanged from v1. Source: `trades.go`. Route constant `reads.PathTrades`.
Pure durable read.

| Param | Required | Default | Validation |
|---|---|---|---|
| `account_id` | yes | — | missing/empty → 400 |
| `limit` | no | **50** | must be a positive integer; no client-visible ceiling — silently degrades to the store's internal `maxRecent=500` |
| `before` | no | absent → newest page | RFC3339 timestamp; unparseable → 400 |

Keyset pagination on `exit_at`, newest-first. `next_before` is the last
row's `exit_at` on a full page, `null` on a short/empty page.

| Status | Trigger |
|---|---|
| 200 | Always, including an empty/new-account result. |
| 400 | missing `account_id`, bad `limit`, bad `before`. |
| 405 | non-GET. |
| 500 | non-`ErrLoadFailed` store error. |
| 503 | `store.ErrLoadFailed`. |

No 409/504 — this endpoint never consults the funnel.

Each row (`store.TradeRow`):

| Field | Type | Notes |
|---|---|---|
| `trade_id` | string | |
| `instrument` | string | |
| `direction` | string | `"long"` \| `"short"`, free-form at this seam |
| `quantity` | int64 | |
| `entry_price` / `exit_price` | int64 | ticks |
| `entry_at` / `exit_at` | RFC3339 string | |
| `gross_pnl` | int64 | signed |
| `total_costs` | int64 | `>= 0` |
| `net_pnl` | int64 | signed |
| `pnl_pct` | **float32** | the one float field in this surface, mirrors a Postgres `REAL` column deliberately |
| `exit_reason` | string | free-form, not a Go enum at this seam |

## 6. `GET /v1/account/pnl`

Unchanged from v1. Source: `trades.go` (`pnlHandler`). Route constant
`reads.PathPnL`. Pure durable read, **a separate endpoint from
`/v1/account/trades`**, not a `summary=` flag.

| Status | Trigger |
|---|---|
| 200 | Always on success. |
| 400 | missing `account_id`. |
| 405 | non-GET. |
| 500 | non-`ErrLoadFailed` store error. |
| 503 | `store.ErrLoadFailed`. |

```json
{ "trade_count": 42, "gross_pnl": 183456, "total_costs": 9123, "net_pnl": 174333, "win_count": 27, "loss_count": 14 }
```
Breakevens (`net_pnl == 0`) counted in neither `win_count` nor
`loss_count`. New/trade-less account → all-zero response, never
`pgx.ErrNoRows`.

## 7. `GET /v1/account/costs`

Unchanged from v1. Source: `costs.go` (`costsHandler`). Route constant
`reads.PathCosts`. Pure durable read, country-dispatched.

| Param | Required | Notes |
|---|---|---|
| `account_id` | yes | missing/empty → 400 |
| `country` | yes | **Case-sensitive, two-letter.** `"IN"` live; `"US"` defined-but-unbuilt → 501; anything else → 400 |
| `limit` | no | default **50**, same cap rules as trades |
| `before` | no | RFC3339 cursor, keyed on the parent trade's `exit_at` |

Case sensitivity confirmed still deliberate — `parseCountry`'s doc comment
is unchanged.

| Status | Trigger |
|---|---|
| 200 | successful read for `country=IN`. |
| 400 | missing account_id/country, bad country, bad limit/before. |
| 405 | non-GET. |
| 500 | non-`ErrLoadFailed` store error. |
| 501 | `country=US`. |
| 503 | `store.ErrLoadFailed`. |

```json
{ "trades": [ { "trade_id": "t-901", "lines": [ {"name": "stt", "amount": 150}, {"name": "exchange", "amount": 20} ], "total": 368 } ], "next_before": "2026-10-15T09:31:04Z" }
```
Key is `"trades"`, not the Tier-4 doc's `"per_trade"`. Only non-zero lines
are present. India `Line.Name` set, unchanged: `stt`, `exchange`, `sebi`,
`stamp`, `gst`, `autosquare_fee` (`brokerage` never selected per-trade).

## 8. `GET /v1/account/costs/summary`

Unchanged from v1. Source: `costs.go` (`costsSummaryHandler`). Route
constant `reads.PathCostsSummary`. A separate endpoint from §7, not a
combined response.

```json
{ "trade_count": 42, "lines": [ {"name": "stt", "amount": 6300}, {"name": "exchange", "amount": 840}, {"name": "sebi", "amount": 42}, {"name": "stamp", "amount": 1050}, {"name": "gst", "amount": 1134} ], "total": 9123 }
```
Zero-sum lines dropped. Trade-less account →
`{"trade_count":0,"lines":[],"total":0}`.

---

## 9. `GET /v1/account/cash` — pre-session cash view

Unchanged from v1. Source: `account_cash.go` + `broker/store
/account_cash.go`. Route constant `reads.PathAccountCash =
"/v1/account/cash"`. Pure durable read off the `accounts` table — no
funnel, no shard, no live-session concept.

| Param | Required | Notes |
|---|---|---|
| `account_id` | yes | 400 if missing/empty |

### Status codes — the one `broker/reads` endpoint that uses 404

| Status | Condition | Body |
|---|---|---|
| 200 | account row found | `{"trading_cash": ..., "bank_cash": ...}` |
| 400 | `account_id` missing/empty | `{"error":"account_id required"}` |
| 404 | `store.ErrNoAccount` | `{"error":"account not found"}` |
| 405 | non-GET | `{"error":"method not allowed"}` |
| 500 | unexpected store error | `{"error":"internal error"}` |
| 503 | `store.ErrLoadFailed` | `{"error":"store unavailable"}` |

```json
{ "trading_cash": 1000000, "bank_cash": 0 }
```
`trading_cash` may be negative (debt is legal, same as `balance
.available`); `bank_cash` is non-negative by a DB `CHECK` constraint.
**Not the same shape as `/v1/account/balance`** — `cash` is the durable,
at-rest view (use before joining a market); `balance` is the live,
in-memory view (use once a session exists). `cash` is not updated live
while a session is active.

---

## 10. Shared types reference

Unchanged from v1 — `funnel.AccountID`/`InstrumentID`/`OrderID` (plain
string aliases), `store.AccountID`, the two special-cased sentinels
(`funnel.ErrBrokerTimeout`, `store.ErrLoadFailed`), the `brokerPort`/
`storePort` narrow interfaces, and `reads.Config{ReadTimeout}`.

---

## Discrepancies / Open Questions

All sixteen items carried forward from v1, re-verified unchanged (the
`git diff` scope check at the top of this file confirms no code in this
package or its dependencies changed since `1c436cc`).

1. **No `unit` field anywhere.** Still open.
2. **No 404 anywhere except `/v1/account/cash`.** Still the rule, still
   one exception.
3. **`/v1/account/orders` has no `instrument` filter.** Still open.
4. **Resting limit price IS retained.** Still resolved (code ahead of the
   Tier-4 doc).
5. **`/v1/account/observation` has no `recent_n` param.** Still open.
6. **Observation's degrade flag is inverted and renamed vs. the Tier-4
   doc.** Still open.
7. **Observation's 200 body has no top-level `present` field.** Still
   open, low-impact.
8. **Trades pagination defaults/caps differ from the Tier-4 doc.** Still
   open.
9. **Trades vs. P&L are separate endpoints.** Still open.
10. **`country` param is case-sensitive**, deliberately. Still open.
11. **Costs vs. costs-summary are separate endpoints**, with a different
    key name (`"trades"` not `"per_trade"`). Still open.
12. **`mark_available` is conditional, not permanently false.**
    Unchanged correction from v1, still accurate — see §2.
13. **No account-existence check on durable endpoints (except `cash`).**
    Matches the Tier-4 doc's own stated decision; not a divergence.
14. **`pnl_pct` is the one `float32` field in this surface.** Still a
    deliberate, documented exception to "never float."
15. **`direction`/`exit_reason` are untyped strings at this seam.**
    Still an open question for a future pass reading `economy/realize`
    directly.
16. **No upper bound surfaced to the client on `limit`.** Still a UX gap,
    not a bug per the store's documented design.
