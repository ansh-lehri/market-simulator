# Cross-Cutting API Contract Concerns (v2)

**Status: refreshed 2026-10-23.** Supersedes `docs/api-contract/
cross-cutting.md` (v1, 2026-10-17 against `1c436cc`). `git diff
1c436cc..HEAD` confirms the only tracked-code change since v1 is the
`conductor/ondemand_spinup.go` registry-cleanup fix (fully covered in
`conductor.md` §3a/§6c — it doesn't touch any cross-cutting concern in this
file, since it's a pure internal-state change with no new status code,
field, or envelope shape). The substantive update in this file is §7
(server composition), driven by the new, untracked `cmd/server/main.go` —
everything else below is re-verified directly against the current source
and found unchanged.

Scope: concerns that apply **globally** across conductor/, broker/order,
broker/transfer, broker/reads, broker/marketdata. Code wins over docs;
disagreements are flagged explicitly, never silently resolved.

---

## 1. Authentication / Authorization

**Unchanged. There is no authentication anywhere in the HTTP surface.**
Re-confirmed by re-grepping `broker/`, `conductor/`, `connectivity/` for
`auth|apikey|jwt|bearer|middleware` against the current tree — same hits
as v1:

- `broker/order/cancel.go:18` and `broker/reads/common.go:99` document the
  convention as a **"no-auth demo surface."**
- `conductor/http_endpoints.go` (`authFromRequest`) is a session-id lookup
  key, not authentication.
- `conductor/http_account_open.go` notes the conductor assumes an auth
  layer exists *upstream* of it.
- No package imports a JWT/OAuth library; no `Authorization` header is
  read anywhere; no API-key table exists.

`cmd/server/main.go`, the new composition-root draft, does not add any
auth layer either — it is purely a mux/lifecycle wire-up and introduces no
new middleware of any kind. **This finding is unaffected by this pass's
changes.**

---

## 2. Numeric Encoding (money, prices, quantities)

**Unchanged.** All money is `int64` coins, all prices `int64` ticks, never
float, with the same two narrow exceptions (`change_pct`, `pnl_pct`).
Re-verified against `contract/values.go`, `broker/order/place.go`,
`broker/transfer/http.go`, `broker/marketdata/http_candles.go`,
`broker/funnel/money_config.go` — all byte-identical to v1's last pass.
Same soft risk noted, unmitigated: large `int64` values are plain JSON
numbers, so a JavaScript client's IEEE-754 doubles can silently lose
precision above 2^53.

---

## 3. Timestamp Formats

**Unchanged — still four incompatible formats, re-verified against the
same files v1 cited, all unmodified since:**

1. **RFC3339** — `broker/reads/common.go`'s `?before=` cursor;
   `conductor/http_markets.go`'s `opens_at`/`closes_at`.
2. **RFC3339Nano** — `conductor/http_ondemand.go` and `conductor/
   session_current.go`'s `warmup_ends_at`/`session_ends_at`.
3. **Naive-local, no offset** (`"2006-01-02T15:04:05"`) —
   `broker/marketdata/http_candles.go` and `fanout.go`'s candle
   `start`/`end`/`next_cursor` and every WS timestamp.
4. **Date-only** (`"2006-01-02"`) — `conductor/canonical_spinup.go`'s
   internal session-date key, not wire-exposed.

No fifth format was introduced by `cmd/server/main.go` (it formats nothing
itself — it only wires existing handlers together) or by the
`conductor/ondemand_spinup.go` change (which touches registry-mutation
logic, not any timestamp-formatting code path). **A client still cannot
use one shared date parser across all five packages.**

---

## 4. Shared Seam / Contract Types

**Unchanged.** `contract/` inventory re-verified identical to v1:
`values.go`, `inbound.go`, `match.go`, `reject.go`, `result.go`, `doc.go`.
The two distinct, non-interchangeable `RejectReason` enums
(`contract.RejectReason`, 5 values; `funnel.RejectReason`, 11 values)
remain deliberately separate, as noted in v1.

---

## 5. Error Envelope Consistency

**Unchanged.** Confirmed consistent `{"error": "<message>"}` shape across
every HTTP package, each with its own independently-declared struct — no
shared `contract.ErrorBody` type exists. The one partial exception remains
the WS protocol's own `"status":"ok"|"error"` ack field, a different (and
reasonable) convention for a different transport.

---

## 6. Session / Market Lifecycle Phases

**Unchanged in the exchange/marketdata layer.** Still three related-but-
distinct enumerations (`exchange/clock.Phase`, `exchange/emit
.SessionEventKind`, `broker/marketdata.SessionPhase`), bridged by one
hand-written mapping function with no compiler-enforced correspondence —
re-verified against the same files v1 cited, all unmodified.

**New, conductor-side nuance this pass (cross-referenced from
`conductor.md` §4/§9):** `conductor`'s own `Session.State` enum (distinct
from all three enums above — this one tracks session *membership*
lifecycle, not market *trading* phase) has had one of its five values,
`Failed`, become unreachable in the current build as a side effect of the
`ondemand_spinup.go` fix (nothing assigns it anymore). This doesn't change
the three-enum market-phase picture above, but it is a fourth,
conductor-local enum worth distinguishing from the market-phase trio when
reasoning about "what does 'state' mean on this wire" across the whole
surface — `Session.State` is about *whether a user is in a market*,
`exchange/clock.Phase`/`emit.SessionEventKind`/`marketdata.SessionPhase`
are about *whether a market is trading*. They are easy to conflate by name
alone.

---

## 7. Server Composition / Wiring (the "composition root" question)

**UPDATED 2026-10-23 — this is the section with the real news this pass.**
v1 left this as: *"bringup's mux and conductor's mux are still two separate
`*http.ServeMux` values with nothing in the repo combining them, and there
is still no `main.go` anywhere that starts an actual listener."* That is no
longer true, with caveats.

### What changed

`cmd/server/main.go` is a new file (untracked in git, like this entire
`docs/api-contract-v2/` tree and `configs/`) that:

1. Calls `bringup.BringUp(ctx, brokerCfg)`, getting back a `*bringup
   .Runtime` whose `.Mux` already carries all 14 broker-owned routes
   (confirmed unchanged since v1 — see `broker-order-transfer.md` §0): the
   9 `broker/reads` routes, the 3 `broker/order` routes, `broker/
   transfer`'s route, and `GET /v1/candles`.
2. Opens a **second**, independent Postgres pool specifically for
   `conductor.Config.OHLCVPool` (a field `conductor.NewConductor`
   requires non-nil — documented in-code as deliberately separate from the
   broker Store's own pool, "so chart-history reads at market-open don't
   compete with session-load traffic"). This means `main.go` reads
   `DATABASE_URL`/`REDIS_URL` from the environment **twice** — once inside
   `bringup.BringUp`, once directly via `connectivity/config.Load` — which
   is redundant but harmless (`config.Load` is pure parsing, no connection
   side effect); the two resulting pools are legitimately separate by
   design.
3. Constructs a `conductor.Conductor` wired to `runtime.Broker` /
   `runtime.Market` (both already satisfy conductor's `BrokerPort`/
   `MarketDataPort` interfaces structurally — no adapter needed), calls
   `cond.Start()` (spawns one per-market timer per `<tag>.json` under
   `CfgDir`), then `cond.MountRoutes(runtime.Mux)` — **the same mux**
   `bringup.BringUp` already populated.
4. Starts exactly one `http.Server{Addr: ":8000", Handler: runtime.Mux}`
   and runs a signal-driven (`SIGINT`/`SIGTERM`) graceful shutdown: HTTP
   server shutdown first (30s budget), then `cond.Stop()`, then `runtime
   .Shutdown()`, then the OHLCV pool close — strict LIFO via `defer`,
   deliberately ordered so new HTTP work stops before conductor's
   background timers stop, which stop before the broker's shard owners
   exit, which exit before their DB/Redis handles close.

**Net: the composition root this doc previously said didn't exist now
does, as a single file, and it resolves the "which mux" question — there
is now exactly one mux, assembled from the two previously-separate pieces,
with both packages' routes live on it.**

### What's still open

- **It is explicitly a draft.** The file's own header comment: *"NOT wired
  into any build target, NOT tested, and NOT reviewed... Treat every line
  below as a sketch to discuss, not production code."* `cmd/server` has
  `[no test files]`. No CI target, `Makefile` entry, or build script in
  this repo references `cmd/server` (grepped — none found).
- **It is untracked by git**, same status as this entire `docs/
  api-contract-v2/` tree, `configs/`, and v1's own `docs/api-contract/` —
  none of these have ever been committed. A `git clone` of this repository
  at its current committed state would **not** include `cmd/server/
  main.go` at all.
- **No reverse proxy / TLS / rate-limiting layer exists around it** — it's
  a bare `http.Server` on a bare port.

### Corrected route count (the file's own comments are wrong)

`main.go`'s inline comments make two claims that don't match the code they
sit next to, re-verified directly against `broker/bringup/bringup.go` (its
`buildMux`, unchanged since `dd66a73`) and `conductor/conductor.go` (its
`MountRoutes`, unchanged since v1):

| Claim in `main.go`'s comments | Actual, verified against source |
|---|---|
| *"rt.Mux comes back pre-populated with the 8 broker/reads routes + GET /v1/candles"* | `broker/reads` has **9** routes as of `d37ed62` (the 8 original plus `PathAccountCash`, added before v1's own last pass — this predates and is unrelated to anything in this pass). The comment undercounts by one. |
| *"broker/order and broker/transfer are explicitly left unmounted — out of scope for this draft"* (header) and *"broker/order and broker/transfer deliberately excluded from this draft, per instruction"* (body) | **False as written.** `bringup.BringUp`'s `buildMux` mounts `order.PlacePath`/`CancelPath`/`ModifyPath` and `transfer.Path` **unconditionally**, inside `BringUp` itself, since commit `dd66a73` (well before this file was written). `main.go` gets all four routes automatically the moment it calls `bringup.BringUp` — there is no "leave them unmounted" option at all with the current `bringup` API. The comment describes a world that predates `dd66a73`; it was likely copy-drifted from an earlier version of `bringup` or an earlier draft of this same file. |
| *"runtime.Mux now has 18 paths total registered on it"* | Recomputed directly: `bringup.BringUp` → 9 reads + 3 order + 1 transfer + 1 candles = **14**. `cond.MountRoutes` → liveness + 5 session-lifecycle + markets + instruments + account-open + `/v1/ws` = **10**. No path collisions between the two (verified — no shared literal path string). **Actual total: 24**, not 18. |

This is flagged as a doc-vs-code (well, comment-vs-code) disagreement per
this repo's own `CLAUDE.md` instruction to surface such gaps rather than
silently fix them — the comment is wrong, the code is what ships; a reader
should trust the route list in `README.md` §2, not this file's inline
prose.

---

## 8. API Versioning

**Unchanged.** Same inconsistency as v1: every route is `/v1/`-prefixed
except `conductor`'s five flat `/session/...` routes. `cmd/server
/main.go` doesn't rewrite or re-prefix any path — it mounts both muxes'
handlers verbatim, so this inconsistency is now live on the one real
listener exactly as it existed in the two separate ones.

---

## 9. CORS and WebSocket Origin Checks

**Unchanged.** No CORS headers are ever set anywhere; `broker/marketdata/
ws.go`'s `CheckOrigin` unconditionally returns `true`. `cmd/server
/main.go` adds no CORS middleware, no `http.Handler` wrapping of `runtime
.Mux` of any kind — the mux is handed to `http.Server` as-is.

---

## Discrepancies / Open Questions

Items 1–11 are reproduced from v1 (re-verified, unchanged except where
explicitly updated per §7 above). Items 12–13 are new this pass.

1. **"No production main.go wires conductor + broker/order +
   broker/transfer + broker/reads + broker/marketdata onto one
   listener."** — **RESOLVED this pass, with caveats.** See §7. A file now
   exists that does exactly this. It is untracked, untested, and
   explicitly self-described as an unreviewed draft — "resolved" means
   "the recipe exists and compiles," not "this is deployed and verified
   end-to-end."
2. **"`buildMux` in `broker/bringup/bringup.go` excludes `broker/transfer`
   and `/v1/ws` to avoid an import cycle."** — Unchanged from v1: the
   `broker/transfer` half was resolved before v1's own last pass (commit
   `dd66a73`); the `/v1/ws` half remains unchanged by design (`conductor`,
   not `bringup`, owns that route).
3. **"No authentication anywhere."** — **STILL TRUE.** See §1.
4. **"Error envelope is `{"error":"..."}` consistently."** — **STILL
   TRUE.** See §5.
5. **"Multiple incompatible timestamp formats."** — **STILL TRUE**, same
   four formats as v1. See §3.
6. **"Two [really three] independently-maintained session-phase enums."**
   — **STILL TRUE.** See §6, plus the new conductor-local `Session.State`
   nuance noted there.
7. **CORS/WS origin checks.** — **STILL TRUE**, unchanged. See §9.
8. **API versioning ad hoc / inconsistent within `conductor` itself.** —
   **STILL TRUE**, unchanged. See §8.
9. **`conductor/conductor.go`'s `MountRoutes` doc-comment staleness.** —
   Re-checked this pass: the function's doc comment is unchanged from v1
   (still describes a smaller scaffold than the 10 routes it actually
   registers). **Still stale**, carried forward as-is.
10. **Two distinct, same-named `RejectReason` types.** — Unchanged. See
    §4.
11. **Large `int64` values as JSON numbers, not strings.** — Unchanged.
    See §2.
12. **(NEW) `cmd/server/main.go`'s own inline comments undercount the
    mounted route total and incorrectly claim `broker/order`/`broker/
    transfer` are excluded from it.** See §7's corrected table. This is
    the kind of staleness that compounds quickly in a fast-moving repo —
    a comment written against one commit of `bringup` silently describing
    a different, later commit's actual behavior — and is flagged here
    rather than fixed, per this repo's CLAUDE.md instruction to surface
    code/doc (here, code/comment) disagreements rather than silently
    resolve them.
13. **(NEW) `conductor`'s `Session.State.Failed` value is dead code as of
    this pass.** Not a cross-cutting wire-format or envelope concern
    directly, but flagged here because it's the kind of "silently
    unreachable enum branch" finding this file exists to catch — see
    `conductor.md` §4 and its own Discrepancy #9 for the full detail; not
    repeated at length here to avoid duplicating that file's content.
