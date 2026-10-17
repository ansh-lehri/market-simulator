# layer7/ondemand — On-Demand Session Spin-Up + Join (L7, Tier-2)

<!-- Provisional path; orientation's call. READ ORDER:
     1) /CLAUDE.md  2) layer7/CLAUDE.md (L7 domain + scaffold)  3) THIS file
     4) docs/LAYER7_ONDEMAND_SPINUP_IMPL.md (the spec)
     Companions it REUSES (do not re-derive):
       LAYER7_CANONICAL_SPINUP_IMPL.md, LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md -->

## Spec & authority
`LAYER7_ONDEMAND_SPINUP_IMPL.md` is the spec (decisions OD.1–OD.13). On-demand = the **canonical spine
wrapped in a money gate + fused spin-up + warm-up**; it states only the deltas over the two canonical
docs and reuses them wholesale. **Code wins over doc** — the shipped broker/marketdata/lifecycle surfaces
and the canonical Conductor/Registry/dispatcher are verified; bind to them exactly.

## What it IS (one line)
One held HTTP call drives the whole thing: **reserve(XOR) → EnsureAccount → gate(QualifyOnDemand) →
[held: BRING_UP → construct → wire both axes → Start → StartSession(seal)] → finalize Live**, returning
**once** at warm-up-start with absolute `warmup_ends_at`/`session_ends_at`; the UI self-times the 2-min
book warm-up and connects `/v1/ws` at `warmup_ends_at`. Spin-up **and** join, fused. Stops at connect;
leave/reap is Tier 3.

## REUSE, DON'T REBUILD (the whole point of this tier)
All shipped — extend/bind, never fork:
- **Broker surface (all shipped):** `EnsureAccount`, `WireInstance(exchangeID, private)`,
  `QualifyOnDemand(ctx, accountID) (bool,err)`, `StartSession(ctx, SessionStartCmd{Mode:OnDemand})`.
  `WireInstance` was the *canonical* prereq — it already exists now; **do not rebuild it.**
- **`/v1/ws` dispatcher (join doc §7): reused UNCHANGED.** Resolves `sessionID → Session.Market
  (od:<sid>) → Market.fanout`, delegates to `marketdata.NewWSHandler`. On-demand adds **zero** WS code
  (OD.11). If you're editing the dispatcher, stop.
- **Registry reserve/finalize/rollback pattern + `StartSession`/`SessionStartCmd`:** from the join doc —
  mirror it. The only new broker call in the gate path is `QualifyOnDemand`.
- **`marketData.WireInstance(..., ModeOnDemand)`**, `NewExchange`/`Start`/`Stop` + edges,
  `buildInstrumentSpecs`/`readReference`: shipped from canonical spin-up.

## NEW in L7 (what this phase actually builds)
1. **Registry deltas (do first — everything compiles against them):** `Session` gains `Failed`
   `SessionState` + `FailReason string`; `Market` gains `warmupEndsAt time.Time` (`closeAt` already =
   session end). (§4.3)
2. **`Conductor.StartOnDemand(ctx, userID, tag, durMin)`** — the fused reserve→gate→held-setup→finalize
   (§4.2), bounded by the handler's `setupTimeout` ctx (~90s, OD.9).
3. **`handleStartOnDemand`** — `POST /session/ondemand?market=&duration=` (§4.1/§4.2), status map §4.4.
4. **`GET /session/{id}/status`** — pure registry read, **fallback/recovery only** (OD.12), not the
   happy path.
5. **Failure helpers:** `rollbackDelete` (pre-setup / gate-fail) and `failOnDemand` (post-setup, real
   teardown).

## Load-bearing rules (from the spec)
- **One return, UI-timed warm-up (OD.1):** hold the request through the **deterministic setup only**
  (gate→populate→construct→wire→Start→seal); return at warm-up-start. Warm-up is **neither held nor
  polled** — UI counts down to the server's absolute `warmup_ends_at`. Revises HTTP_SURFACE H.2 (polling
  demoted to fallback).
- **Schedule (OD.4), `T0 = now at construct`:** `continuous @ T0`, `closed @ T0 + warmupLen +
  minutes(durMin)`. `warmup_ends_at = T0 + warmupLen`; `session_ends_at = T0 + warmupLen + duration`.
  The exchange is continuous the **whole** span — "warm-up" is just its first `warmupLen` (agents trade,
  user not admitted); there is **no warm-up phase in the exchange**. Duration genuinely starts after
  warm-up because the closed boundary is `warmup_ends_at + duration`.
- **`BRING_UP` before construct (OD.9):** so `T0` equals the `Start` instant and the window is tight.
  `NewExchange` starts no goroutines → a populate failure aborts **before anything is built**.
- **duration (OD.2):** integer minutes, query param, `min 5 / default 60 / max 120`; out-of-range →
  **400**. warm-up length = 2 min, configurable `ondemand.warmup` (OD.3).
- **IDs / mode (OD.6):** `MarketID = ExchangeID = "od:<sessionID>"`; `numShards = 1`;
  `marketdata.ModeOnDemand` (throwaway population, candles not persisted). `tickInterval = time.Second`.
- **Reference (OD.5):** prior-close — reuse canonical `readReference` keyed on the market **tag**.
- **The seal (OD.7):** `StartSession(Mode = lifecycle.OnDemand)` → `OndemandSessionLoad`,
  `PersistOnClose=false` (ephemeral non-persisting wallet; balance read at load).
- **NO seed (OD.8):** on-demand has **no `SeedLiquidity`** — the book forms from agents + the warm-up
  window. Don't call the liquidity operator.
- **XOR reserve:** `reg.byUser[userID] != nil → ErrAlreadyInSession` (strict — canonical **or**
  on-demand; a user is in at most one).
- **THE RULE (three locked regions, nothing else):** lock held **only** for (1) the reserve, (2) the
  `Market`-create, (3) the finalize — **never** across `BRING_UP` / construct / wire / `Start` /
  `StartSession`.

## Failure choreography (the real delta vs canonical)
Canonical seeds-before-wire, so abort = delete the Provisioning record, nothing spawned. On-demand has
**no seed** and its fallible `StartSession` runs **after** `Start`, so teardown is real (OD.10):
- `rollbackDelete(sid,userID)` — gate-fail / pre-setup: nothing built; delete `sessions[sid]` + `byUser`.
- `failOnDemand(sid, marketID, reason)` — setup failure: **if the exchange was Started, `exch.Stop()`**
  (closes edges → settlement consumer + ingestor exit; ingestor does `CacheRegistry.Remove`); then mark
  `sess.State=Failed` + `FailReason`, delete `markets[marketID]`. **Session stays `Failed`** so a
  fallback status read / recovery sees it. Full teardown/reap + clearing the Failed session = **Tier 3**.

## HTTP status map (§4.4)
`warming_up`→200 · `insufficient_funds`→200 · XOR conflict→**409** · bad body / duration ∉ [5,120]→**400**
· setup fail or `setupTimeout` exceeded→**503** `{failed,reason}` · `ErrBadBinding`/`ErrBadParticipant`
(= L7 bug) / other→**500**.

## Deps & seam
L7-0 scaffold (Conductor/Registry/config/ports/THE RULE); the **shipped** canonical spin-up + join code
(dispatcher, `StartSession`, registry patterns); shipped broker `QualifyOnDemand`/`EnsureAccount`/
`WireInstance`/`StartSession`, `marketData.WireInstance(ModeOnDemand)`, `lifecycle.OnDemand` +
`OndemandSessionLoad`; `simulator/exchange/wire`; the **stub** `AgentOrchestrator.BringUp` (SYNC,
bounded) — no real agents until L2.

## Do NOT
- rebuild `funnel.WireInstance` or **any** broker surface — shipped; bind exactly;
- touch the `/v1/ws` dispatcher or registry resolution — reused unchanged (OD.11);
- call `SeedLiquidity` / any liquidity operator — on-demand has no seed (OD.8);
- hold the registry lock across `BRING_UP`/construct/wire/`Start`/`StartSession` (THE RULE);
- let a bad schedule reach `NewExchange` (validate terminal `PhaseClosed`, sorted — the clock **panics**);
- persist anything on close — `PersistOnClose=false` is the whole point of `OnDemand` mode;
- treat the status endpoint as the happy path — fallback/recovery only (OD.12);
- forget teardown after `Start` — a post-`Start` failure must `exch.Stop()` + `CacheRegistry.Remove`.

## Flags (orientation)
- **Registry deltas first** (Session `Failed`/`FailReason`, Market `warmupEndsAt`) — the compile seam.
- Setup-hold length rides on `BRING_UP` (L2, undesigned); bounded by `setupTimeout` (OD.9); exceed →
  `failed`. Fallback = flip this one request to poll — **not** the default.
- `BRING_UP` is a stub; signature will change (same placeholder port as canonical).
- On-demand liquidity (OD.8) — no operator; revisit if the thin book bites.
- Reconnect token (P6) — `AuthFromRequest` carrier is the `sessionID` on connect; longer-lived token
  parked. Instrument allowlist — `SessionStartCmd` has no field; installs permissive (full set).
- Full failure teardown/reap + Failed-session clearing = **Tier 3**, out of scope.

## Tests (objective; no goldens/auditor)
- `StartOnDemand` with a **fake ExchangeFactory + stub ports + fake broker** (`QualifyOnDemand` toggle;
  `EnsureAccount`/`StartSession`/`WireInstance` fakes):
  - **gate NO** → `InsufficientFunds`, 200, `rollbackDelete` leaves **no** session/byUser residue;
  - **XOR** → second user-session → `ErrAlreadyInSession` → 409;
  - **gate YES happy** → `warmupEndsAt`/`closeAt` off injected `T0` (`=T0+warmup`, `=T0+warmup+dur`);
    `State` flips `Active`/`Live`; handles set at finalize;
  - **populate_failed** (BringUp err, pre-construct) → `failOnDemand`, **no** `exch.Stop` (nothing
    started), `Session=Failed`+reason, market gone;
  - **install_failed** (`StartSession` err, post-`Start`) → `failOnDemand` **calls `exch.Stop()`** +
    market removed, `Session=Failed`+reason retained;
  - THE RULE — assert no lock across call-outs (structural review).
- duration bounds: `<5`/`>120` → 400, `0`/absent → default 60; bad body → 400.
- status endpoint: `Starting→warming_up`(+ends if Market up) / `Active→ready`(+ends) / `Failed→failed`+
  reason / unknown → 404 (pure read, no side effects).
- WS: assert on-demand adds **no** dispatcher code — a resolution test (`od:<sid>` → the instance's
  `FanOut`, one subscriber) exercises the **shipped** path only.
- `go test ./... -race`.