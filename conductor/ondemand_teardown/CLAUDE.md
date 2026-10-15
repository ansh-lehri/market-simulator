# layer7/teardown — On-Demand Teardown & Reconnect, Shape C (L7, Tier-3)

<!-- Provisional path; orientation's call. This is the ON-DEMAND HALF of Tier-3 teardown —
     SAME package as the canonical half (shared handleExit / shapeC / registry states), not a new dir.
     READ ORDER:
       1) /CLAUDE.md  2) layer7/CLAUDE.md (L7 domain + scaffold)  3) THIS file
       4) docs/LAYER7_ONDEMAND_TEARDOWN_IMPL.md (the spec)
     SIBLING it REUSES HEAVILY (read first — defines the shared pieces):
       LAYER7_CANONICAL_TEARDOWN_IMPL.md  (EndSession seam, /session/current, handleExit skeleton,
                                           TearingDown, THE RULE)
     Also reuses: LAYER7_ONDEMAND_SPINUP_IMPL.md (m.closeAt, m.warmupEndsAt, ModeOnDemand, od:<sid>),
                  LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md (/v1/ws dispatcher §7). -->

## Spec & authority
`LAYER7_ONDEMAND_TEARDOWN_IMPL.md` is the spec (decisions OT.1–OT.7). This is the **on-demand half** of
Tier 3; the **canonical half** (`LAYER7_CANONICAL_TEARDOWN_IMPL.md`) is the sibling that defines the
shared pieces this doc **reuses**: the `EndSession` seam (here, its `Mode=OnDemand` = **discard**
branch), `POST /session/current` (reconnect resolve), the `handleExit` skeleton, `TearingDown`, THE
RULE, the registry states. **Code wins over doc** — the shipped `exch.Stop()`, the `/v1/ws` dispatcher,
and the already-built on-demand spin-up (`m.closeAt`/`m.warmupEndsAt`/`od:<sid>`) are verified; bind
exactly.

## What it IS (one line)
Shape C — a **private single-user `od:<sid>` instance winds down**: **flip `TearingDown` →
`EndSession(OnDemand=discard)` → `WindDown` → `exch.Stop()` → reap + free `byUser`** — fired by either
an **explicit exit** or a **per-session reap timer at `session_ends_at`** (the two race, resolved by the
flip). Accidental nav is **not** teardown → reconnect. No detach-keep, no square, no `FinalizeMarket`,
no rejoin — once gone, the user starts a **new** on-demand.

## REUSE, DON'T REBUILD (this phase is mostly glue over the sibling)
- **`EndSession` seam — defined and both branches now built.** The canonical arm shipped with the
  canonical-teardown stage; the on-demand (`Mode=OnDemand`) discard arm shipped alongside this
  phase in `broker/funnel/ondemand_session_discard.go` — routes a `DiscardSession` command to the
  owning shard (`hash(AccountID)`), uninstalls the binding, drops the ephemeral `LoadedAccount`
  from `sessions` + the per-exchange index, and calls `lifecycle.SessionDiscard` (Redis snapshot
  delete only; **never writes Postgres** — the on-demand account carries `PersistOnClose=false`
  from load). Contrast canonical = detach-keep. The conductor-side teardown binds the port's
  `EndSession` method and treats a wrapped `ErrDiscardFailed` as a log+proceed error (the shard
  maps are cleaned regardless; a stale Redis key is TTL-cleaned).
- **`handleExit` skeleton — shipped from the canonical doc.** It branches on `m.Mode`; this phase fills
  the **on-demand branch** = a straight `shapeC_teardown`. Don't fork the handler.
- **`POST /session/current` — shipped from the canonical doc** (§6.2). Reused **unchanged** for
  reconnect; it already returns `warmup_ends_at`/`session_ends_at` for on-demand (OT.7). Zero new code.
- **`TearingDown` / `Stopping` registry states, THE RULE — from the canonical doc.** Reuse.
- **`/v1/ws` dispatcher** (join §7) — reused unchanged; reconnect resolves `od:<sid>` → the single-
  subscriber FanOut.

## NEW in L7 (what this phase actually builds — small)
1. **`shapeC_teardown(marketID)`** (§3) — the on-demand teardown body. `marketID = "od:<sid>"`.
2. **`spawnOnDemandReap(marketID, closeAt)`** (§2.1) — one short-lived goroutine per on-demand session,
   armed to `m.closeAt`; fires `shapeC_teardown` at duration-end, or exits on `conductorQuit`.
3. **The on-demand branch of `handleExit`** (§5) — resolve mode, call `shapeC_teardown`, 200.
4. **CROSS-DOC EDIT:** the on-demand spin-up **finalize** gains the `spawnOnDemandReap(marketID,
   m.closeAt)` call (it computed `m.closeAt` but spawned no reaper — Tier 3 adds it). Update
   `LAYER7_ONDEMAND_SPINUP_IMPL.md` when implementing (OT.4 / §8).

## Load-bearing rules (from the spec)
- **Exit-vs-nav is automatic — no flag (OT.2):** the test is purely "does `byUser[user]` still exist?"
  Nav → session survives → `/session/current` returns it → reconnect works. Exit/timeout → Shape C
  reaped it + freed `byUser` → `/session/current` **404** → "session ended, start a new one." The
  teardown reaping the session is what removes the rejoin option.
- **Three triggers (OT.1):** explicit exit → Shape C now; duration timeout → reap timer fires Shape C at
  `session_ends_at`; accidental nav/drop → **no** teardown, instance stays live (agents trading, timer
  running), `Session` `Active`, `byUser` holds → reconnect.
- **No detach-keep (OT.3):** the market is the user's, so exit = **full teardown**, not a detach. There
  is **no fresh-rejoin** for on-demand — gone means start-new.
- **Two triggers converge, idempotent by the flip (OT.4/§2):** exit and the reap timer both call
  `shapeC_teardown` and can race. `Live → TearingDown` flip resolves it: first lock-taker runs it, the
  other sees `TearingDown`/gone and **no-ops**.
- **Reap timer (OT.4/§2.1):** per-session goroutine (T3.14 Option 1 — **not** a shared reaper), armed to
  **`m.closeAt`** (`= session_ends_at = T0 + warmupLen + duration`), **not** "duration from now" — the
  warm-up window is counted. Spawned at spin-up finalize. The exchange clock also hits `PhaseClosed` at
  `m.closeAt` but that **does not stop the instance** — L7's reap timer is what tears it down via
  `exch.Stop()`.
- **Shape C body (OT.5/§3):** `EndSession(OnDemand=discard)` (**not** `FinalizeMarket`); **N=1** barrier
  (single discard ack, bounded, **log + proceed** on error — forward-progress, T3.9); `WindDown(tag,
  OnDemand)` L2 fire-and-forget (discard throwaway population); `exch.Stop()` (tape closes →
  market-data self-cleans; nothing persisted); reap `sessions[sid]` + `markets[marketID]` + free
  `byUser`. **No** pre-close square, **no** `FinalizeMarket`/market→accounts sweep, **no** operator
  (on-demand was never operator-seeded, T3.11).
- **Reconnect (OT.7/§6):** nothing torn down; `/session/current` → the on-demand timers → UI countdown
  resumes → re-open `/v1/ws` → `od:<sid>` single-subscriber FanOut. While the socket is dropped the
  FanOut has **zero** subscribers — agent publishes drop harmlessly; shipped `Disconnect` had cleaned
  the old sub; browser re-subscribes on the shipped in-band path (join §8).
- **THE RULE:** lock held **only** for the two µs regions (the `TearingDown` flip + member read; the
  reap) — **never** across `EndSession` / `WindDown` / `exch.Stop()`.

## Exit status table (§5, on-demand) — all roads reach `Stop()`, intent always honored
`success (torn down)`→**200** · unknown session→**404** · already torn down (timer won the race)→**200**
(idempotent, flip no-ops) · `EndSession` discard err/timeout→**200** + logged (forward progress reaches
`Stop()`; the user's intent to end it is honored regardless).

## Do NOT
- redefine or build the `EndSession` seam — it's the canonical doc's (B-1); here bind + exercise the
  **discard** branch only (OT.6);
- fork `handleExit` or `/session/current` — shipped from the sibling; fill the on-demand branch / reuse;
- `FinalizeMarket`, pre-close square, or market→accounts sweep — none apply to a single ephemeral
  account (OT.5); it's discarded wholesale;
- persist anything — `PersistOnClose=false`; `session_discard` never writes Postgres;
- arm the reap timer off "duration from now" — arm off **`m.closeAt`** so warm-up is counted (OT.4);
- use a shared reaper — one goroutine **per session** (T3.14 Option 1);
- tear down on accidental nav / socket drop — that's reconnect; instance stays live (OT.1);
- treat an `EndSession` discard error as a failure — **log + proceed**, still 200 (forward progress);
- hold the registry lock across `EndSession`/`WindDown`/`exch.Stop()` (THE RULE);
- treat the "already torn down (reaper won)" HTTP status as 200 — the preflight resolves no
  session, so it honestly returns 404 (the user's intent to end was already honored by the reaper).

## Deps & seam
L7-0 scaffold; the **shipped** canonical teardown (`handleExit` skeleton, `/session/current`,
`TearingDown`/`Stopping`, THE RULE, the `BrokerPort.EndSession` contract); the **shipped** on-demand
spin-up (`m.closeAt`/`m.warmupEndsAt`/`od:<sid>`/`ModeOnDemand`, the finalize to extend); the `/v1/ws`
dispatcher; `simulator/exchange/wire` `exch.Stop()`; `lifecycle.OnDemand` + `PersistOnClose=false`; the
**stub** `AgentOrchestrator.WindDown` (L2); `conductorQuit` (process-shutdown signal for the reaper).

## Flags (orientation)
- **`EndSession` seam** (dual-mode, shard delivery) is broker-owned. Both branches are now built —
  the canonical arm from the earlier canonical-teardown stage, the on-demand discard arm alongside
  this phase in `broker/funnel/ondemand_session_discard.go` (`DiscardSession` command + handler;
  calls `lifecycle.SessionDiscard`; shard-side maps cleaned before the Redis delete so forward
  progress holds on a `DeleteSnapshot` failure, surfaced as a wrapped `ErrDiscardFailed`).
- `WindDown`/`BringUp` signatures **will** change with L2 (placeholder ports).
- Server-crash recovery — a crash loses the ephemeral instance entirely (nothing persisted, by design);
  the user starts a new on-demand. **No recovery path** (nor wanted). (§8)
- **Spawn-point cross-doc edit done.** `spawnOnDemandReaper(marketID, m.closeAt)` is now called at
  the on-demand spin-up finalize (after the Live-state commit); the sibling spin-up doc has been
  updated to pin the call site.

## Tests (objective; no goldens/auditor)
Against a **fake broker** (`EndSession` with an `OnDemand`/discard path + injectable err), **stub
`WindDown`**, injected `now`/`closeAt`, fake exchange handle:
- **`shapeC_teardown`:** happy → `EndSession(Mode=OnDemand)` called, `WindDown`, `exch.Stop()` once,
  `sessions`/`markets`/`byUser` all gone; **run-once guard** (`!Live` → return, no double-reap);
  `EndSession` err → **proceeds** to `Stop()`+reap (forward-progress); assert **no** `FinalizeMarket`/
  square/operator call; THE RULE (no lock across call-outs) — structural.
- **exit-vs-timeout race (OT.4):** two `shapeC_teardown` calls on the same market → exactly one runs the
  body, the second no-ops on the flip; both leave the registry clean.
- **reap timer:** armed off injected `closeAt` (= `session_ends_at`, **not** now+duration — assert the
  warm-up is counted); fires `shapeC` at `closeAt`; `conductorQuit` → timer stopped, no teardown; a
  timer firing **after** an exit already reaped → safe no-op.
- **`handleExit` on-demand branch:** unknown → 404; success → 200 + reaped; already-torn (timer won) →
  **200** idempotent; `EndSession` err → **200** + logged.
- **reconnect (OT.2/OT.7):** `byUser` present (nav, no teardown) → `/session/current` → session + mode
  `OnDemand` + both timers; after teardown → `byUser` freed → **404**; reconnect-just-before vs
  just-after `session_ends_at` both resolve cleanly (gets session then socket closes / 404).
- **spin-up finalize edit:** finalize now calls `spawnOnDemandReap(marketID, m.closeAt)` (assert the
  reaper is armed off `m.closeAt`).
- `go test ./... -race`.