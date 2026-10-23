# Market Simulator — Frontend/UI API Contract (v2)

**Status: refreshed 2026-10-23 against this repo's current state (tracked
history up to commit `025f365`, plus two untracked-but-present trees:
`cmd/server/` and `configs/`).** This supersedes `docs/api-contract/` (the
"v1" pass, last updated 2026-10-17 against commit `1c436cc`). Re-verification
method: `git log`/`git diff 1c436cc..HEAD` was read first to scope exactly
what changed in tracked code (one behavioral commit pair,
`5de1d37`/`025f365`, touching only `conductor/ondemand_spinup.go` and its
test — see [§0b](#0b-what-changed-since-v1-the-full-list)), then every
package's source was re-read in full rather than assumed unchanged, and the
two previously-nonexistent, untracked directories (`cmd/`, `configs/`) were
read for the first time. Code wins over docs; every disagreement is recorded
below, never silently resolved.

Read [§0](#0-read-this-first--deployment-reality) before anything else — the
deployment picture changed more than the endpoint behavior did this pass.

| File | Covers |
|---|---|
| [`conductor.md`](./conductor.md) | Signup/login, market catalog, instrument list, joining a canonical market, triggering/tearing down an on-demand market, session status/recovery, WS upgrade gate |
| [`broker-order-transfer.md`](./broker-order-transfer.md) | Place / cancel / modify order, bank↔trading transfer |
| [`broker-reads.md`](./broker-reads.md) | Balance, positions, open orders, composite "observation", closed trades, P&L, cost/tax breakdowns |
| [`marketdata.md`](./marketdata.md) | Candle history (HTTP) and live candle/snapshot streaming (WebSocket) |
| [`cross-cutting.md`](./cross-cutting.md) | Auth, numeric encoding, timestamps, error envelope, seam-type leakage, session phases, route wiring, composition root, versioning, CORS — full evidence trail for everything summarized below |

---

## 0. Read this first — deployment reality

**The single biggest change since v1: a composition root now exists.**
`cmd/server/main.go` is a new, previously-absent file that calls
`bringup.BringUp` and `conductor.NewConductor`/`cond.Start()`/
`cond.MountRoutes` on **one shared `*http.ServeMux`**, then runs one
`http.Server` with an ordered graceful-shutdown chain (HTTP → conductor →
broker runtime → conductor's own OHLCV pool). v1 stated flatly that
"nothing in this repository ever actually binds a TCP listener outside of
tests" — **that is no longer true.**

Two caveats, both important for anyone deciding whether to build against
this:

1. **It's git-untracked and self-described as a draft.** `cmd/server/`,
   `configs/`, and this very `docs/api-contract-v2/` directory are all
   untracked in `git status` as of this pass (so was v1's
   `docs/api-contract/`, for what it's worth — these contract docs have
   never been committed). `main.go`'s own header comment states: *"It is
   NOT wired into any build target, NOT tested, and NOT reviewed... Treat
   every line below as a sketch to discuss, not production code."* There is
   no test file (`cmd/server` → `[no test files]`), and nothing has
   exercised this file's actual listener end-to-end.
2. **The file's own comments are already stale relative to the code they
   sit next to.** The header says *"broker/order and broker/transfer are
   explicitly left unmounted — out of scope for this draft"* and a later
   comment claims the mux ends up with *"18 paths total."* Both are wrong,
   verified directly against `broker/bringup/bringup.go`'s current
   `buildMux` (unchanged since v1 — see `broker-order-transfer.md` §0):
   `bringup.BringUp` already mounts **all 14** broker-owned routes
   unconditionally (9 `broker/reads` + 3 `broker/order` + 1
   `broker/transfer` + `GET /v1/candles`), so `order`/`transfer` are **not**
   excluded — main.go gets them "for free" the moment it calls
   `bringup.BringUp`, whether its own comment says so or not.
   `conductor.MountRoutes` adds **10** more routes (counting the liveness
   probe), none overlapping. **The real total is 24 distinct paths, not
   18.** See `cross-cutting.md` §7 for the full count and the corrected
   route table.

**Action for the frontend team:** if this file (or something built the same
way) is what gets deployed, you now get the *entire* documented surface —
sessions, markets/instruments, account-open, `/v1/ws`, orders, transfers,
reads, and candles — all on one listener, no extra glue required. But
nothing has run this file against a real request yet; treat it as "the
composition recipe now exists and compiles," not "this exact file is what's
running in any environment." Confirm with backend whether `cmd/server/
main.go` (or an equivalent) is actually what gets built and deployed before
committing to a sprint plan around it.

### 0a. Deployment knobs, as hardcoded in the draft

`main.go` has no flag parsing and reads no env vars itself (env is read only
inside `connectivity/config.Load`, called twice — once inside
`bringup.BringUp`, once directly by `main` to size a second, separate
Postgres pool `conductor.Config.OHLCVPool` requires). The operationally
relevant literals, as of this pass:

| Knob | Value | Source |
|---|---|---|
| HTTP listen address | `:8000` | `main.go` const |
| Market config directory | `./configs/markets` | `main.go` const — currently contains exactly one file, `IN.json` (see below) |
| `BrokerConfig.ShardCount` | 4 | `main.go` literal |
| `BrokerConfig.StartingCapital` | 1,000,000 coins | `main.go` literal, labeled "illustrative" |
| `BrokerConfig.StartingBank` / `OnDemandMinimum` | 0 / 0 | `main.go` literal |
| Conductor's own OHLCV pool size | 4 connections | `main.go` const, independent of the broker Store's own pool sizing |
| Graceful shutdown budget | 30s | `main.go`, wraps `httpServer.Shutdown` |

`configs/markets/IN.json` — the one market config a fresh checkout ships
with — defines market tag `IN`, timezone `Asia/Kolkata`, a 09:15/15:30/15:40
continuous→closing_auction→closed session shape, a 60‑minute/2‑minute
on‑demand duration/warmup default, and exactly two instruments, `ACME`
(tick size 5) and `BETA` (tick size 10) — **not** `RELIANCE`/`TCS`/`INFY`,
which appear throughout this contract (and v1's) only as illustrative
example payloads. A frontend pointed at an out-of-the-box checkout of this
repo will see `ACME`/`BETA` on `GET /v1/instruments?market=IN`, not the
illustrative tickers used in request/response examples.

### 0b. What changed since v1 — the full list

Re-verified via `git diff 1c436cc..HEAD` (the exact commit v1 was last
checked against): **exactly one behavioral change landed in tracked code**,
spanning two commits (`5de1d37`, `025f365`), touching only
`conductor/ondemand_spinup.go` and its test file. Everything else in every
package — `exchange/`, `economy/`, `contract/`, `connectivity/`, all of
`broker/` — is byte-for-byte unchanged since v1's last pass; this was
confirmed by diff, not re-assumed.

1. **(behavior change) On-demand setup failure now fully wipes the
   registry, instead of marking the session `Failed` and retaining it.**
   Previously, any failure during `StartOnDemand`'s setup pipeline (agent
   populate, schedule build, instrument-spec build, or the post-`Start`
   broker-seal step) left the `Session` in the registry with
   `State: Failed` and kept the user's `byUser` slot occupied — the stated
   intent being that a future reaper would clear it on a TTL. That reaper
   was never built, so in practice **a user whose first on-demand trigger
   failed was permanently stuck**: every retry hit the per-user XOR gate
   (`ErrAlreadyInSession`, a `409`) with no way out short of a process
   restart. `failOnDemand` now deletes the session, frees the `byUser`
   slot, and drops the market record on **every** setup-failure branch
   (`populate_failed`, `schedule_invalid`, `specs_failed`,
   `install_failed`) — the same shape pre-setup rollback already used, plus
   the post-`Start` exchange-stop step. See `conductor.md` §3 and §4 for the
   full, re-derived behavior and its knock-on effects (summarized in
   item 2 below).
2. **(new, consequence of #1) The `Failed` session-status label is now
   unreachable in the current build.** Grepped every non-test assignment
   to `Session.State` across `conductor/`: nothing sets it to `Failed`
   anymore (confirmed — the only call site was the one rewritten in #1).
   `GET /session/status` and `POST /session/current` both still carry a
   `Failed`/`"failed"` branch in their status-label switch, but it is dead
   code against the current registry-mutation paths — a session that fails
   on-demand setup is deleted outright, so a status read against its
   `session_id` afterward gets **404 `{"error":"unknown session"}`**, not
   `200 {"status":"failed","reason":"..."}` as v1 documented. **The failure
   reason is now visible to the client in exactly one place: the original
   `POST /session/ondemand` call's `503` response body
   (`onDemandFailedBody`).** If that response is lost (client crash,
   network drop after the up-to-90s `SetupTimeout` fired), the reason is
   gone — there is no durable or registry-backed way to recover it anymore.
3. **(new, consequence of #1) Same-user retry after a failed on-demand
   trigger now works immediately**, with no backoff, no manual
   intervention, and no restart — proven end-to-end by the new
   `TestStartOnDemand_RetryAfterSetupFailureUnblocked` (two sub-cases:
   pre-construct populate failure, post-Start install failure). This is a
   genuine bug fix a frontend integrator should be glad about: a "try
   again" button on a failed on-demand trigger can now simply re-POST
   `/session/ondemand` with the same user identity and market tag.
4. **(new, composition) `cmd/server/main.go` exists.** See §0 above and
   `cross-cutting.md` §7 for the full detail — this is new *code*, not a
   behavior change to any existing endpoint, and it ships as an explicitly
   unreviewed draft.
5. **(new, config) `configs/markets/IN.json` exists** — the first concrete,
   checked-in market config in the repo (previously `docs/api-contract/`
   described `<tag>.json` purely abstractly). See §0a above.
6. **No endpoint was added, removed, or had its request/response shape
   changed this pass.** Every route, status code, field, and reason tag
   documented in v1's `conductor.md` §1–§2, §7–§10,
   `broker-order-transfer.md`, `broker-reads.md`, and `marketdata.md` is
   re-verified unchanged in this pass's sibling files — those files are
   reproduced here with updated verification dates/commit references, not
   because nothing was checked, but because nothing there had, in fact,
   changed.

---

## 1. Global conventions (true across every package, verified independently in each)

- **No authentication, anywhere.** Unchanged from v1 — no API key, JWT,
  cookie, or bearer token exists in this codebase. Every endpoint trusts a
  plaintext identifier taken on faith:
  - `account_id` — body field (order/transfer) or query param (reads).
  - `user.user_id` — JSON body field (conductor signup/join/current).
  - `?session=<id>` — query param (conductor WS gate, status, exit; also
    the sole "auth" signal `conductor.handleWS` uses to admit a `/v1/ws`
    connection).
  See [`cross-cutting.md §1`](./cross-cutting.md#1-authentication--authorization).
- **Numeric encoding — strict integer discipline.** Unchanged. All money is
  `int64` coins; all prices are `int64` ticks; quantities are `int64`.
  **Never a float**, with exactly two narrow, named exceptions on the
  entire wire surface: `change_pct` (WS market-data snapshot) and
  `pnl_pct` (trades/costs rows, `float32`, mirroring a Postgres `REAL`
  column) — both derived display percentages, never a money/price value.
  Lot size / minimum order quantity is still not exposed anywhere.
- **Error envelope — one consistent shape, with one addition on
  place/cancel/modify.** Unchanged:
  ```json
  { "error": "<message string>" }
  ```
  `broker/order`'s place/cancel endpoints additionally use a **business
  rejection** shape on specific status codes:
  ```json
  { "reason": "<stable_snake_case_tag>" }
  ```
  `modify` has its own composed response (see `broker-order-transfer.md`
  §4). Only `reason` and the HTTP status code are safe to branch logic on.
- **Timestamps — at least four incompatible formats across packages.**
  Unchanged from v1:
  | Format | Where | Example |
  |---|---|---|
  | RFC3339 (second precision) | `conductor`'s `/v1/markets`; `broker/reads`'s `?before=` cursor and response rows | `2026-10-16T03:45:00Z` |
  | RFC3339Nano | `conductor`'s session-lifecycle fields (`warmup_ends_at`, `session_ends_at`, both on the trigger response and `/session/current`) | `2026-10-15T12:05:00.000000000Z` |
  | Naive-local, no offset/zone suffix | `broker/marketdata` candle `start`/`end`/`next_cursor` and every WS timestamp — in the market's own IANA zone, named separately on the envelope | `2026-10-09T09:15:00` |
  | Date-only `2006-01-02` | `conductor/canonical_spinup.go`'s session-date key (internal, not wire-exposed, lower severity) | `2026-10-16` |
  **A frontend cannot use one shared date parser across all five
  packages.** See [`cross-cutting.md §3`](./cross-cutting.md#3-timestamp-formats).
- **Versioning.** Unchanged — every route is `/v1/`-prefixed, **except**
  `conductor`'s own session-lifecycle routes (`/session/canonical`,
  `/session/ondemand`, `/session/status`, `/session/current`,
  `/session/exit`), which have no version prefix.
- **CORS / WS origin — wide open.** Unchanged. No CORS headers exist
  anywhere; the WebSocket upgrader's `CheckOrigin` unconditionally returns
  `true`.
- **Panic safety.** Unchanged — every handler in every package recovers
  from a panic and converts it to `500 {"error":"internal error"}`.
- **No idempotency keys anywhere**, with the same single exception as
  before: order **cancel** is idempotent. Place, modify, and transfer all
  mint/move fresh state on every call. **A `504` never means "nothing
  happened."**
- **No webhooks / push notifications for order or transfer outcomes.**
  Unchanged.
- **Pagination.** Unchanged — only `broker/reads`'s `/v1/account/trades`
  and `/v1/account/costs`, and `broker/marketdata`'s `/v1/candles`,
  paginate, all via an opaque/timestamp keyset cursor, and the two cursor
  formats are **not** mutually compatible.

---

## 2. Full endpoint index

No additions, removals, or shape changes this pass (see §0b item 6).
Reproduced here for completeness against the current commit.

### conductor (session/market orchestration — see `conductor.md`)
| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/_conductor/live` | Liveness probe |
| POST | `/v1/account/open` | Signup/login handshake → `account_id` |
| GET | `/v1/markets` | Market catalog, liveness, session window |
| GET | `/v1/instruments?market=` | Static instrument list + tick size |
| POST | `/session/canonical` | Join a live canonical market |
| POST | `/session/exit?session=` | Leave a session — mode-branched: canonical detach-keep, or on-demand full teardown |
| POST | `/session/ondemand?market=&duration=` | Trigger an on-demand market (slow, ≤90s). **Setup failure is now a clean, fully-reversible no-op on the registry — see §0b.** |
| GET | `/session/status?session=` | Recovery status read. **No longer ever reports `"failed"` for an on-demand session in the current build — see §0b.** |
| POST | `/session/current` | Resolve current session by user identity |
| GET | `/v1/ws?session=` | WebSocket upgrade gate — delegates framing to `broker/marketdata` |

### broker/order + broker/transfer (writes — see `broker-order-transfer.md`)
Mounted inside `bringup.BringUp`; confirmed unchanged and, as of this pass,
also reachable via `cmd/server/main.go`'s composition (see §0).
| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/orders` | Place order |
| POST | `/v1/orders/cancel` | Cancel order |
| POST | `/v1/orders/modify` | Modify order (composed cancel+replace) |
| POST | `/v1/account/transfer` | Bank ↔ trading cash transfer |

### broker/reads (account-scoped queries — see `broker-reads.md`)
| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/account/balance` | 4-bucket cash split (requires a live session — 409 otherwise) |
| GET | `/v1/account/cash` | Pre-session durable cash view (`trading_cash`/`bank_cash`) — works with no live session; the one `broker/reads` endpoint that returns 404 |
| GET | `/v1/account/positions` | Live open positions |
| GET | `/v1/account/orders` | Live working-set orders only |
| GET | `/v1/account/observation` | Composite balance+positions+orders+recent trades |
| GET | `/v1/account/trades` | Paginated closed-trade history |
| GET | `/v1/account/pnl` | Account-wide P&L aggregate |
| GET | `/v1/account/costs?country=` | Paginated per-trade cost/tax breakdown |
| GET | `/v1/account/costs/summary?country=` | Account-wide cost/tax aggregate |

### broker/marketdata (public, anonymous — see `marketdata.md`)
| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/candles` | Paginated OHLCV candle history |
| GET (WS) | `/v1/ws` | Live candle closes + instrument snapshot push — reachable via `conductor` |

---

## 3. Consolidated critical gaps (confirm with backend before building against these)

Ranked by integration impact. Items marked **(v2, new)** are new this pass;
everything else is carried forward from v1, re-verified unchanged.

1. **(v2, new) The on-demand setup-failure reason is now a one-shot,
   non-durable value.** It is delivered exactly once, on the triggering
   `POST /session/ondemand` call's `503` body. There is no follow-up
   endpoint that can recover it after the fact — `GET /session/status` on
   that `session_id` now 404s instead of echoing the reason. A frontend
   that doesn't reliably capture/display the `503` body at the moment it
   arrives loses that information permanently. See §0b item 2.
2. **(v2, resolved) The composition-root gap is substantively closed as a
   recipe, but still unreviewed/untested.** `cmd/server/main.go` now joins
   `bringup.BringUp` and `conductor` on one mux with ordered shutdown. It
   is explicitly self-described as a draft with no test coverage. See §0.
3. **(v2, new, cosmetic but worth knowing) `cmd/server/main.go`'s own
   header/body comments undercount the mounted routes and incorrectly
   claim `broker/order`/`broker/transfer` are excluded.** They are not —
   `bringup.BringUp` mounts them unconditionally, unchanged since
   `dd66a73`. See §0 and `cross-cutting.md` §7.
4. **No authentication anywhere.** Unchanged. See §1 and
   `cross-cutting.md §1`.
5. **At least four incompatible timestamp formats** across packages.
   Unchanged. See §1.
6. **FOK time-in-force is still accepted on the wire but not honored** —
   re-confirmed unchanged by direct re-read of `exchange/match/matcher.go`
   (this file is untouched since v1; no FOK branch exists anywhere in
   `exchange/`). Don't offer it as a selectable option.
7. **Market-buy pricing and position-mark availability remain
   conditional, not unconditional** — both require the exchange instance
   to have been `WireInstance`'d and the instrument to have printed at
   least one quote/trade. Unchanged; re-confirmed against
   `broker/bringup/bringup.go` and `broker/marketdata/registry.go`, both
   byte-identical to v1's last pass.
8. **`SlippageBufferBps` is still not plumbed from `BrokerConfig` into
   `MoneyConfig` in `bringup.go`** — re-confirmed unchanged; market-buy
   orders still reserve with a zero slippage cushion. `cmd/server/main.go`
   doesn't set it either (not a field on `BrokerConfig` at all — it lives
   only on `MoneyConfig`, which `bringup.go` builds without exposing that
   field to the caller).
9. **Tick size is discoverable (`GET /v1/instruments`); lot size / minimum
   quantity is not exposed anywhere in the codebase.** Unchanged.
10. **No webhook/push for order or transfer terminal outcomes.** Unchanged.
11. **Modify is two non-atomic legs and can leave the account with no
    order at all** (`modify_status: "partial"`). Unchanged.
12. **`broker/reads` response shapes still diverge from
    `BROKER_READ_SURFACES_tier4.md`** in the same ways v1 found (no `unit`
    field, split trades/pnl and costs/costs-summary endpoints, no
    `?recent_n=`/`?instrument=` filters, different observation field
    names, 503 instead of 504, no 404 except on `/v1/account/cash`,
    case-sensitive `country`). Re-confirmed unchanged this pass.
13. **Two independently-maintained session-phase vocabularies** (really
    three related enums: `exchange/clock.Phase`, `exchange/emit
    .SessionEventKind`, `broker/marketdata.SessionPhase`), bridged by one
    hand-written mapping function. Unchanged.
14. **(v2, sharpened) `conductor`'s "what's my session doing" endpoints
    now diverge even further in practice.** `/session/status` and
    `/session/current` already used two different status-label
    vocabularies for the same states (v1 finding, unchanged); as of §0b,
    `/session/status`'s `Failed`/`"failed"` branch is also now dead code
    for on-demand sessions specifically — a failed on-demand session
    doesn't surface as `"failed"` on this endpoint at all anymore, it
    surfaces as `404 unknown session`. Canonical sessions never reached
    `Failed` to begin with (confirmed: nothing in `canonical_join.go`/
    `canonical_spinup.go` sets it), so this branch was already partially
    dead and is now effectively dead everywhere in the current build.
15. **No instrument validation on `GET /v1/candles`.** Unchanged.
16. **WS slow-client eviction has no backfill/catch-up mechanism.**
    Unchanged.
17. **`GET /v1/account/cash` remains the one `broker/reads` exception to
    "never 404."** Unchanged — see `broker-reads.md` §9.

---

## 4. Glossary

| Term | Meaning |
|---|---|
| **Tick** | The exchange's atomic price unit. All prices on the wire are a raw integer count of ticks; `tick_size` (from `GET /v1/instruments`) is how many ticks apart two legal order prices must be. |
| **Coin** | The atomic money unit. All balances/amounts on the wire are a raw integer count of coins; there is no sub-coin fraction and no currency code anywhere in the API. |
| **Canonical market** | A standing, scheduled market (e.g. "IN" during its normal session window) that any user can join while it's live. Contrast with **on-demand**. |
| **On-demand market** | A short-lived, per-user-triggered market instance with a warm-up window and a fixed total duration (5–120 min); fully torn down (ephemeral account discarded) on both normal exit *and* setup failure (see §0b) rather than persisted like a canonical session. |
| **Session (conductor sense)** | A user's membership in one live market (canonical or on-demand), identified by `session_id`. Not an HTTP/auth session — purely a market-membership record, and as of this pass, one that **never survives a setup failure** — a session either becomes `Active` or ceases to exist; there is no longer a visible `Failed` resting state. |
| **Participant** | The broker-side handle (`participant_id`) returned alongside a conductor `session_id` on join; used by order-entry calls. |
| **Account** | The broker's money/position-owning entity (`account_id`), created once via `POST /v1/account/open` and reused across every market session the same user joins. |
| **Working set** | An account's currently resting (not yet terminal) orders — what `GET /v1/account/orders` returns. |
| **Punch** | Internal term for the broker handing an order action to the exchange for admission. "Locally accepted and punched" ≠ "executed." |
| **Reservation** | Money or position units locked against a resting order so they can't be double-spent; released on fill or cancel. |
| **Gate 4 / funnel** | Internal broker subsystems that validate and reserve funds before an order reaches the exchange. |
| **`WireInstance`** | The per-exchange-instance call (made by `conductor` on every market spin-up) that binds `broker/marketdata`'s price/snapshot cache to that instance — the gate that makes both market-buy pricing *and* position-mark availability go from "always missing" to "available once traded." |
| **Snapshot (market-data sense)** | The WS push carrying an instrument's whole current picture (last price, quote, depth) — always a full re-emit, never a delta. |
| **Candle / bar** | One OHLCV bucket for a timeframe (`1m`/`5m`/`30m`/`60m`). Only finalized (closed) bars are ever returned or pushed. |
| **Durable / live** (in `broker/reads`) | "Live" reads hit the in-memory broker shard for the account (require an active session, else `409`); "durable" reads hit Postgres directly and work for any account, active or not. |
| **Cash vs. balance** | `/v1/account/cash` is a *durable* read (works with no session, 404s on an unknown account). `/v1/account/balance` is a *live* read (needs a session, 409s without one). Not interchangeable; `cash` does not update while a session is live. |
| **Mode-blind** (transfer) | A design property of the transfer endpoint: the response is identical whether the money moved through the live in-memory path or the durable Postgres fallback. |
| **Composition root** (v2, new) | The one process entry point that actually calls `http.ListenAndServe`. As of this pass, `cmd/server/main.go` is the first (and only) such file, and it is an explicitly unreviewed draft — see §0. |
