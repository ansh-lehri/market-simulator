# Conductor (Layer 7) — API Contract (v2)

**Status: refreshed 2026-10-23, re-verified against this repo's git history
up to commit `025f365`.** This supersedes `docs/api-contract/conductor.md`
(v1, verified against `1c436cc`). `git diff 1c436cc..HEAD` was read first to
scope what changed: exactly two commits, `5de1d37` and `025f365`, both
touching only `conductor/ondemand_spinup.go` and its test file. Every other
`.go`/`_test.go` file in `conductor/` was re-read in full for this pass and
is confirmed unchanged. The behavioral change is a real one — it alters the
status tables in §3 and §4 below relative to v1 — so this file is not a
rubber-stamp carry-forward; §3, §4, and §5 have updated content, and a new
§6c covers the fix in detail. Sections 1, 2, 7, 8, 9, 10 are reproduced
verbatim from v1 (re-verified, unchanged).

This document is the frontend/UI integration contract for the `conductor/`
package — the Layer‑7 HTTP/WebSocket orchestration surface that sits above
the broker and the exchange. Scope: every HTTP endpoint and the one
WebSocket dispatch route the conductor mounts via `MountRoutes`. The
conductor is **money‑blind** and **market‑data‑blind**. There is **no
authentication**: every request carries a trusted identity packet
`{ user: { email, user_id } }`; `user_id` is the identity key, `email` is
attribution only.

## Conventions used throughout

- **Base paths.** Session/account endpoints are flat, unversioned paths
  (`/session/...`); catalog/instrument/account endpoints are versioned
  (`/v1/...`). Unchanged.
- **Identity packet** (`joinRequestUser`):
  ```json
  { "email": "string, attribution only, not validated", "user_id": "string, required, trusted verbatim" }
  ```
- **Error envelope** (`errorBody`), used by every non‑2xx response except
  the on‑demand trigger's 503 (distinct shape, see §3):
  ```json
  { "error": "<human-readable message, often the Go error's .Error() string>" }
  ```
- **Session carrier.** Three endpoints (`GET /v1/ws`, `GET /session/status`,
  `POST /session/exit`) read the session id from the query string
  `?session=<sid>`. `POST /session/current` resolves the session from the
  identity packet instead.
- **Panic safety, content type, method enforcement, handler-owned
  timeouts** — all unchanged from v1; see that doc's Conventions section
  for the full detail (re-verified identical this pass).

| Config field | Default constant | Default value | Governs |
|---|---|---|---|
| `BringUpTimeout` | `DefaultBringUpTimeout` | 30s | the synchronous `AgentOrchestrator.BringUp` call inside spin‑up |
| `JoinTimeout` | `DefaultJoinTimeout` | 5s | `POST /session/canonical`, and (reused) `POST /session/exit` |
| `AccountOpenTimeout` | `DefaultAccountOpenTimeout` | 3s | `POST /v1/account/open` |
| `SetupTimeout` | `DefaultSetupTimeout` | 90s | `POST /session/ondemand` |
| `BadConfigRetryDelay` | `DefaultBadConfigRetryDelay` | 1h | internal per‑market timer retry backoff |
| `CloseTimeout` | `DefaultCloseTimeout` | 60s | internal scheduled‑close / on‑demand‑reaper barrier |

Unchanged from v1; all overridable via `Config`.

---

## 1. `POST /v1/account/open` — signup / login handshake

Unchanged from v1. File: `conductor/http_account_open.go`. Handler:
`handleAccountOpen`. One call into the broker's `EnsureAccount`. No registry
touch. Idempotent across signup and login.

- **Auth:** none. Trusted identity packet.
- **Request body:**
  ```json
  { "user": { "email": "a@b.com", "user_id": "u-123" } }
  ```
- **Timeout:** `AccountOpenTimeout` (default 3s).

| Status | Trigger | Body |
|---|---|---|
| 200 | `EnsureAccount` succeeded (new or existing account) | `{"account_id":"<opaque broker handle>"}` |
| 400 | missing/empty JSON body, or malformed JSON | `{"error":"invalid JSON body"}` |
| 400 | `user.user_id` empty | `{"error":"user_id required"}` |
| 400 | `lifecycle.ErrBadOwnerID` | `{"error":"<err>"}` |
| 405 | method != POST | `{"error":"method not allowed"}` |
| 503 | `store.ErrNoAccount` or `lifecycle.ErrCreateFailed` | `{"error":"<err>"}` |
| 504 | `AccountOpenTimeout` fired | `{"error":"context deadline exceeded"}` |
| 500 | `lifecycle.ErrBadOwnerType`, negative-capital sentinels, any unmapped error, or a panic | `{"error":"<err>"}` or `{"error":"internal error"}` |

```json
POST /v1/account/open
{ "user": { "email": "trader@example.com", "user_id": "u-42" } }
```
```json
{ "account_id": "acct_8f2c1e" }
```

---

## 2. `POST /session/canonical` — join a live canonical market

Unchanged from v1. File: `conductor/http_endpoints.go` (handler) +
`conductor/canonical_join.go` (flow `JoinCanonical`).

- **Auth:** none.
- **Request body:**
  ```json
  { "user": { "email": "a@b.com", "user_id": "u-123" }, "market": "IN" }
  ```
- **Timeout:** `JoinTimeout` (default 5s).
- **Idempotency / reconnect:** an existing **Active** session on the
  **same** market tag returns the existing `{session_id, participant_id}`
  verbatim with zero broker I/O.

| Status | Trigger |
|---|---|
| 200 | success — fresh admission or same‑market reconnect |
| 400 | malformed JSON body, or `user.user_id`/`market` empty |
| 405 | method != POST |
| 409 | `ErrMarketNotLive` / `ErrLiveElsewhere` / `funnel.ErrSessionAlreadyLive` |
| 504 | `funnel.ErrBrokerTimeout` |
| 503 | `lifecycle.ErrNoAccount`, `ErrLoadFailed`, `ErrSnapshotWriteFailed` |
| 500 | `EnsureAccount` failures, conductor‑bug seams, any other error, or a panic |

```json
{ "session_id": "<uuid>", "participant_id": "<uuid>", "status": "active" }
```

**Confirmed this pass: `canonical_join.go` never assigns `Session.State =
Failed`** — the only state transitions it performs are `Starting` (on
reservation) and `Active` (on success); a failed join rolls back by
deleting the reservation outright (`rollbackDeleteOnDemand`'s canonical
counterpart), the same "no resting Failed record" shape §6c below describes
for on‑demand. Canonical sessions have never been able to reach `Failed` —
this isn't a change, but it's now directly relevant background for §4's
corrected status table.

---

## 3. `POST /session/ondemand?market=<tag>&duration=<minutes>` — spin up a one‑user instance

File: `conductor/http_ondemand.go` (handler) + `conductor/ondemand_spinup.go`
(flow `StartOnDemand`). **Request/response shapes and query-param
validation are unchanged from v1.** What changed this pass is what happens
to the registry *after* a setup failure — see the new §3a below, which
replaces v1's treatment of the `503` branch.

This is a **held** (synchronous, long‑running) request — the handler does
not return until the entire setup pipeline (gate check → agent populate →
exchange construct/wire/start → broker seal) either completes or fails.

- **Auth:** none. Trusted identity packet (body).
- **Query params:**
  - `market` — required; 400 if missing or unknown tag.
  - `duration` — optional integer minutes, default **60**, legal range
    **[5, 120]**; malformed/out-of-range → 400.
- **Request body:** `{ "user": { "email": "...", "user_id": "u-123" } }`.
- **Timeout:** `SetupTimeout` (default 90s).

### Admission gate

After the per‑user reservation succeeds, the flow calls `EnsureAccount` then
`QualifyOnDemand` (a broker‑side money‑floor check). A gate failure is a
first‑class `200` result (`OnDemandInsufficientFunds`), not an error; no
setup is attempted.

### Responses

| Status | Trigger | Body shape |
|---|---|---|
| 200 | setup completed; warm‑up window is now running | `onDemandWarmingUpBody` |
| 200 | gate rejected the account (insufficient trading cash) | `onDemandInsufficientFundsBody` |
| 400 | malformed body / missing or bad `market`/`duration`/`user.user_id` / unknown market tag | `errorBody` |
| 405 | method != POST | `errorBody` |
| 409 | `ErrAlreadyInSession` or `funnel.ErrSessionAlreadyLive` | `errorBody` |
| 503 | `ErrOnDemandSetupFailed` (wraps agent populate / schedule / specs / exchange construct / `StartSession` seal failure) or `context.DeadlineExceeded` | **`onDemandFailedBody`** — `{"status":"failed","reason":"<raw cause string>"}` |
| 500 | `funnel.ErrBadBinding`/`ErrBadParticipant`, any other error, or a panic | `errorBody` |

Success body — warming up:
```json
{
  "status": "warming_up",
  "session_id": "<uuid>",
  "warmup_ends_at": "2026-03-15T10:05:00.000000000Z",
  "session_ends_at": "2026-03-15T11:05:00.000000000Z"
}
```
Success body — insufficient funds:
```json
{ "status": "insufficient_funds", "message": "insufficient funds for on-demand session; move money and retry" }
```
Setup‑failed body (503):
```json
{ "status": "failed", "reason": "install_failed: <underlying broker error>" }
```

Timestamp format note: unchanged — `warmup_ends_at`/`session_ends_at` use
`time.RFC3339Nano`; `GET /v1/markets`' session‑window timestamps use plain
RFC3339.

### 3a. What happens after a 503 — CHANGED this pass (see §6c for the full trace)

**v1 said:** a setup failure marks the `Session` record `Failed` in the
registry and keeps the user's `byUser` slot occupied, so `GET
/session/status` could later report `{"status":"failed","reason":...}`,
and a planned-but-unbuilt reaper would eventually free the slot.

**That is no longer accurate.** `failOnDemand` (the function every one of
the four setup-failure branches calls) now:
1. Deletes the `Session` record from the registry outright.
2. Frees the user's `byUser` slot (only if it still points at this exact
   session — a defensive check against a race with a second, unrelated
   session for the same user).
3. Deletes the `Market` record.
4. If the exchange had already been `Start`ed (the post-`Start` failure
   branch only), calls `exchange.Stop()` first, before the registry wipe.

**Consequence for the frontend:**
- The `503` response body (`onDemandFailedBody`, carrying `reason`
  verbatim) is now the **only** place this failure's cause is ever
  surfaced. There is no follow-up read that recovers it.
- **A same-user retry (same or different market) can be issued
  immediately** — the previous XOR-gate deadlock (`ErrAlreadyInSession` on
  every subsequent attempt) is gone. This is a genuine bug fix: see the new
  test `TestStartOnDemand_RetryAfterSetupFailureUnblocked`.
- `GET /session/status?session=<the failed session's id>`, called any time
  after the `503` returns, now gets **404 `{"error":"unknown session"}`**,
  not a `200` with a `"failed"` status label. See §4 below — this is a
  direct, user-visible change to that endpoint's behavior for this
  scenario, even though `handleSessionStatus`'s own code is untouched (the
  change is entirely in what the registry now contains).

---

## 4. `GET /session/status?session=<sid>` — fallback/recovery status read

File: `conductor/http_ondemand.go`. Handler: `handleSessionStatus`. **Code
unchanged from v1** — but the registry mutation this pass changed (§3a)
means one of this handler's own branches is now unreachable for on-demand
sessions, so its *effective* behavior table has changed. Documented in
full below, including the dead branch, so a reader doesn't have to
reconstruct this from the code alone.

- **Auth:** none.
- **Query:** `session` — required session id. 400 if absent.

| Status | Trigger | Body |
|---|---|---|
| 200 | session id found in the registry | `sessionStatusBody` (shape varies by state, see below) |
| 400 | `session` query param missing | `errorBody` |
| 404 | session id not in the registry — **now the outcome for any on-demand session that failed setup, immediately after its `503` response, with no window where a `"failed"` status is readable** | `errorBody` `{"error":"unknown session"}` |
| 405 | method != GET | `errorBody` |

`sessionStatusBody` field population by `Session.State`:

| `Session.State` | `status` label | Timers | `reason` | **Reachable in the current build?** |
|---|---|---|---|---|
| `Starting` | `"warming_up"` | included only if the `Market` record already exists | omitted | yes |
| `Active` | `"ready"` | always included | omitted | yes |
| `Failed` | `"failed"` | omitted | included | **no — dead code.** Grepped every non‑test `Session.State` assignment in `conductor/`; nothing sets it to `Failed` anymore. The only call site that used to (`failOnDemand`) was rewritten this pass to delete the session instead (§3a). |
| `Stopping` or `Ended` | `"ended"` | omitted | omitted | yes |

**Practical takeaway for a frontend:** do not build a "poll `/session/
status` to discover why my on-demand trigger failed" flow — it will never
see `"failed"` in the current build. The `reason` string is only ever
available in the triggering `POST /session/ondemand` call's own `503` body,
at the moment it arrives.

### Example

```
GET /session/status?session=3fae...-uuid
```
200 (ready):
```json
{ "status": "ready", "warmup_ends_at": "2026-03-15T10:05:00Z", "session_ends_at": "2026-03-15T11:05:00Z" }
```
404 (a session that failed setup, at any point after its `503`):
```json
{ "error": "conductor: session not found" }
```

---

## 5. `POST /session/current` — reconnect‑resolve by user identity

File: `conductor/session_current.go`. Handler: `handleSessionCurrent`.
**Code and request/response shape unchanged from v1.** The same dead-branch
caveat as §4 applies: `sessionCurrentResponseBody.status` can emit
`"failed"` per its own `SessionState.String()` vocabulary, but nothing in
the current build will ever populate a `byUser` entry pointing at a
`Failed` session — a user whose on-demand trigger failed has **no**
`byUser` entry at all afterward (§3a step 2), so this endpoint returns
**404 `{"error":"no session for user"}`** for that user immediately after
the failure, not a `200` with `status:"failed"`.

- **Auth:** none.
- **Request body:** `{ "user": { "user_id": "u-123" } }`.
- No timeout wrapper — pure in‑memory registry read.

| Status | Trigger | Body |
|---|---|---|
| 200 | user has an entry in `byUser` | `sessionCurrentResponseBody` |
| 400 | malformed JSON body, or `user.user_id` empty | `errorBody` |
| 404 | user has no session (`byUser` miss) — **now the outcome immediately after any on-demand setup failure for that user, not just for a user who never triggered anything** | `errorBody` `{"error":"no session for user"}` |
| 405 | method != POST | `errorBody` |

```json
{
  "session_id": "<uuid>",
  "mode": "canonical" | "ondemand",
  "market": "<tag>",
  "status": "starting" | "active" | "stopping" | "ended" | "failed",
  "warmup_ends_at": "RFC3339Nano, omitted for canonical sessions",
  "session_ends_at": "RFC3339Nano, omitted for canonical sessions"
}
```
`status` here uses the raw `SessionState.String()` vocabulary — a
**different** vocabulary from `/session/status`'s labels, unchanged
inconsistency from v1 (see Discrepancies §1). The `"failed"` value in this
enum is, as of this pass, unreachable for the same reason given above.

---

## 6. `POST /session/exit?session=<sid>` — leave a session (mode‑branched)

Unchanged from v1 in every respect except the renumbering below (v1's §6a/
§6b are reproduced verbatim as §6a/§6b; a new §6c is added to give the
on-demand setup-failure fix its own fully-traced home, cross-referenced
from §3a).

File: `conductor/session_leave.go`. Handler: `handleSessionExit`.
Mode‑branched: looks up the session's market under one microsecond
registry lock to decide which of two independent teardown bodies to run.

- **Auth:** none.
- **Query:** `session` — required. 400 if absent.
- **Timeout:** reuses `JoinTimeout` (default 5s) for both branches.
- **Response body on success (both branches):** `{"status":"ended"}`.

### 6a. Canonical branch — `leaveCanonicalSession` (detach‑keep)

Unchanged. Uninstalls the broker binding and frees the per‑account XOR, but
**keeps** the account's `LoadedAccount` resident (parked in a `detached`
map) so a later rejoin can re‑install over carried positions.

| Status | Trigger |
|---|---|
| 200 | detach + reap succeeded, or broker reported `funnel.ErrNoLiveSession` (idempotent success) |
| 404 | `ErrSessionNotFound` |
| 409 | `ErrSessionNotActive` |
| 504 | `funnel.ErrBrokerTimeout` |
| 503 | `lifecycle.ErrLoadFailed`, `ErrSnapshotWriteFailed`, `ErrNoAccount` |
| 500 | conductor‑bug seams, registry drift, any unknown error, or a panic |

On any error after the session was flipped to `Stopping`, it is reverted
back to `Active`. `broker.FinalizeMarket` is never called from leave.

### 6b. On‑demand branch — `exitOnDemandSession` → `tearDownOnDemandInstance` (full teardown)

Unchanged — this is the **normal-exit** teardown path (user-initiated or
the automatic end-of-session reaper), distinct from the **setup-failure**
path covered in §3a/§6c. Both ultimately produce the same end state (no
session, no market, no `byUser` slot) but via different trigger points and
different functions (`tearDownOnDemandInstance` here vs. `failOnDemand` in
§6c) — do not conflate them when reading the source.

1. **Preflight** (handler): resolve `session → market → mode`. If already
   gone, falls through to `ErrSessionNotFound` → 404.
2. `exitOnDemandSession`: re‑resolves under its own lock; vanished-between
   → 404 again. Otherwise calls `tearDownOnDemandInstance`.
3. `tearDownOnDemandInstance` (shared with the automatic reaper):
   - Guard: missing/non‑`Live` market → no‑op. Flips `Live → TearingDown`.
   - Off‑lock, forward‑progress on every error: `broker.EndSession
     (Mode=OnDemand)` (errors logged and ignored), `agents.WindDown`,
     `exchange.Stop()`.
   - Reap: deletes the session, market record, and `byUser` slot.
   - `broker.FinalizeMarket` is never called on this path.

| Status | Trigger | Body |
|---|---|---|
| 200 | teardown ran regardless of internal `EndSession` error | `{"status":"ended"}` |
| 404 | session unknown at preflight or resolve (already reaped by a prior exit, the automatic reaper, **or — new, see §6c — a prior setup failure**) | `errorBody` |
| 500 | unreachable in the current body | `errorBody` |
| 405 | method != POST | `errorBody` |
| 400 | `session` query param missing | `errorBody` |

**Important status‑table deviation, unchanged from v1:** an on‑demand
session already torn down by the automatic reaper returns **404**, not an
idempotent 200 — the preflight resolve honestly reports "no such session."
Treat 404 as success for on‑demand exit specifically.

The automatic reaper (`spawnOnDemandReaper`) is spawned once per on‑demand
instance at `StartOnDemand` finalize — i.e. only once a session has
actually reached `Active`. A session that fails during setup never gets a
reaper spawned for it at all (§6c covers that path's own cleanup
mechanism, which runs synchronously inside the failing call, not on a
timer).

### 6c. (NEW) Setup‑failure cleanup — `failOnDemand`, the fix this pass made

This is the function §3a describes from the HTTP contract's point of view;
this subsection gives the full mechanical trace for anyone reading this
alongside the source.

**Before this pass:** `failOnDemand(sessionID, marketID, reason,
exchangeHandle)` set `reservedSession.State = Failed` and
`reservedSession.FailReason = reason`, deleted only the `Market` record,
and left both the `Session` record and the `byUser[userID]` slot in place.
The stated rationale (now-removed doc comment, quoted in the source diff):
*"byUser is deliberately NOT cleared: the Failed Session stays reachable so
a fallback status read surfaces 'failed' + reason, and a future recovery
pass has something to reconcile."* No such recovery pass — reaper or
otherwise — was ever built for the `Failed` state specifically (the reaper
that *does* exist, `spawnOnDemandReaper`, only arms for sessions that
reached `Active`, per §6b). The result in practice: **a user whose first
on-demand trigger failed was permanently stuck** — every subsequent
`POST /session/ondemand` for that user hit the per-user XOR gate
(`ErrAlreadyInSession`, 409) with no way out short of restarting the
process (which wipes the whole in-memory registry).

**After this pass:** `failOnDemand(sessionID, userID, marketID, reason,
exchangeHandle)` (note the new `userID` parameter) does, unconditionally,
on every one of the four setup-failure call sites (`populate_failed`,
`schedule_invalid`, `specs_failed`, `install_failed`):
1. `exchangeHandle != nil` (post-`Start` failure only) → `exchangeHandle
   .Stop()` first, logged.
2. Under the registry lock: `delete(sessions, sessionID)`; if
   `byUser[userID] == reservedSession`, `delete(byUser, userID)`; `delete
   (markets, marketID)`.
3. Logs the failure at `slog.Error`, now including `user` alongside
   `session_id`/`market_id`/`reason`/`exchange_stopped`.

This is the exact same "leave no residue" shape the **pre-setup** rollback
path (`rollbackDeleteOnDemand`, used when the very first per-user
reservation step itself fails, before any `Session`/`Market` record is
even created) already used — the fix brings the **post-reservation**
setup-failure path into alignment with it, rather than introducing a new
pattern.

**Test coverage added this pass** (`conductor/ondemand_spinup_test.go`):
- `TestStartOnDemand_PopulateFailedBeforeConstruct` — pre-construct
  failure (agent `BringUp` itself fails): asserts zero registry residue
  (`sessions`, `byUser`, `markets` all empty) and that no exchange was
  ever constructed.
- `TestStartOnDemand_InstallFailedAfterStart` — post-`Start` failure
  (`StartSession` fails after the exchange is already running): asserts
  the same zero-residue outcome **and** that `exchange.Stop()` was called
  (no goroutine leak from the started-but-never-sealed exchange).
- `TestStartOnDemand_RetryAfterSetupFailureUnblocked` — the end-to-end
  proof, two sub-cases (`populate_failed`, `install_failed`): triggers a
  failure, asserts the registry is clean, then immediately retries
  `StartOnDemand` for the **same user** and asserts the retry does **not**
  hit `ErrAlreadyInSession`. This is the regression test that would catch
  a reversion of this fix.

---

## 7. `GET /v1/markets` — catalog + liveness + session window

Unchanged from v1. File: `conductor/http_markets.go`. Handler:
`handleMarkets`.

- **Auth:** none. **Query:** none.
- A market with a bad config or unschedulable window is logged and
  skipped, not surfaced as an error.

| Status | Trigger |
|---|---|
| 200 | always, even for an empty catalog |
| 405 | method != GET |
| 500 | `listConfiguredMarkets` (the `CfgDir` read) failed |

```json
{
  "markets": [
    {
      "tag": "IN",
      "active": true,
      "session": {
        "opens_at": "2026-03-16T03:45:00Z",
        "closes_at": "2026-03-16T10:00:00Z",
        "duration": "6h15m0s",
        "wall_clock": "09:15–15:30 IST"
      }
    }
  ]
}
```

**Grounded against the one config file this repo now ships
(`configs/markets/IN.json`, see `README.md` §0a):** this example matches
that file's shape exactly — `IN`, `Asia/Kolkata`, a 09:15–15:30 session
window with a 15:40 close for the closing auction. A fresh checkout's
`GET /v1/markets?` response will have `tag: "IN"` as its only entry.

---

## 8. `GET /v1/instruments?market=<tag>` — static instrument list

Unchanged from v1. File: `conductor/http_instruments.go`. Handler:
`handleInstruments`.

- **Auth:** none. **Query:** `market` — required.

| Status | Trigger |
|---|---|
| 200 | config loaded for the tag |
| 400 | `market` missing/empty |
| 404 | no `<tag>.json` exists |
| 405 | method != GET |
| 500 | config present but malformed |

```json
{
  "market": "IN",
  "instruments": [
    { "id": "RELIANCE", "tick_size": 5 }
  ]
}
```

**Against the actual checked-in `configs/markets/IN.json`**, a fresh
checkout's real response is:
```json
{
  "market": "IN",
  "instruments": [
    { "id": "ACME", "tick_size": 5 },
    { "id": "BETA", "tick_size": 10 }
  ]
}
```
Only `id` and `tick_size` are returned — `band_bps`, `seed_price`, and
liquidity config stay internal.

---

## 9. `GET /v1/ws?session=<sid>` — market‑data WebSocket dispatcher

Unchanged from v1. File: `conductor/http_endpoints.go`. Handler:
`handleWS`. Validates the session carrier, resolves `sessionID → Session
.Market → Market.fanout`, and hands off to `marketdata.NewWSHandler`. Wire
protocol out of scope here — see `marketdata.md`.

- **Auth:** none; carrier is `?session=<sid>` only.
- **Path constant:** `marketdata.PathWS = "/v1/ws"`.

| Status | Trigger |
|---|---|
| (upgrade) | carrier present, resolves to an `Active` session on a `Live` market |
| 401 | `?session=` absent/empty |
| 404 | session doesn't exist, isn't `Active`, or its market is gone/not `Live` |
| 405 | method != GET |

---

## 10. `GET /v1/_conductor/live` — liveness probe

Unchanged from v1. File: `conductor/conductor.go`. `GET` → `200 "ok"`
(plain text); any other method → `405 "method not allowed"` (also plain
text, not JSON).

---

## What changed since v1

See `README.md` §0b for the cross-package summary. Within this package
specifically: `conductor/ondemand_spinup.go`'s `failOnDemand` now fully
wipes the registry on setup failure instead of marking the session
`Failed` and retaining it (§3a, §6c). No HTTP route, request shape, or
response shape changed — the change is entirely in registry-state
side-effects, which surfaces to a frontend only as "a session that failed
setup is no longer visible as `'failed'` on `/session/status` or
`/session/current` — it's simply gone, and the user's slot is free to
retry immediately."

---

## Discrepancies / Open Questions

Items 1–8 are reproduced from v1 (re-verified, unchanged). Item 9 is new
this pass.

1. **Status‑label vocabulary mismatch between `/session/current` and
   `/session/status`.** Unchanged — see §5.
2. **On‑demand exit's 404‑vs‑200 asymmetry is intentional but easy to
   miss.** Unchanged — see §6b.
3. **`/v1/account/open` and `/session/canonical` map the same lifecycle
   sentinel to different HTTP statuses.** Unchanged.
4. **Path‑scheme inconsistency** (`/session/...` unversioned vs. `/v1/...`
   for everything else). Unchanged.
5. **No rate limiting / no auth anywhere on this surface.** Unchanged.
6. **`GET /v1/_conductor/live` uses a different error‑envelope
   convention** than every other route. Unchanged.
7. **Un‑exercised 500 branch in `onDemandExitStatusFor`.** Unchanged.
8. **WebSocket message/payload shapes are explicitly out of scope here.**
   Unchanged.
9. **(NEW) The `Failed` `SessionState` is now dead code in practice.**
   `Session.go`'s enum still declares it, `registry.go`'s `FailReason`
   field comment still references it, and both `handleSessionStatus` and
   `handleSessionCurrent` still carry live switch-cases for it — but
   nothing in the current non-test codebase ever assigns `Session.State =
   Failed` (confirmed by grepping every assignment site in `conductor/`;
   see §6c). This is not a bug — the behavior it enables (a resting,
   inspectable failure record) was deliberately removed because nothing
   ever cleaned it up — but a reader of `registry.go` in isolation would
   reasonably expect `Failed` to be reachable, and it currently is not.
   Flagging so a future change that re-introduces a `Failed`-setting call
   site does so deliberately, with the retry-deadlock risk this pass fixed
   explicitly in mind, rather than by accident.
