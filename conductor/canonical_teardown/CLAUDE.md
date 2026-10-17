# layer7/teardown — Canonical Teardown, Leave & Rejoin (L7, Tier-3)

<!-- Provisional path; orientation's call. READ ORDER:
     1) /CLAUDE.md  2) layer7/CLAUDE.md (L7 domain + scaffold)  3) THIS file
     4) docs/LAYER7_CANONICAL_TEARDOWN_IMPL.md (the spec)
     Companions it REUSES (do not re-derive):
       LAYER7_CANONICAL_SPINUP_IMPL.md, LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md
     Sibling (NOT here): LAYER7_ONDEMAND_TEARDOWN_IMPL.md (Shape C) -->

## Spec & authority
`LAYER7_CANONICAL_TEARDOWN_IMPL.md` is the spec (decisions CT.1–CT.11). It builds on
`LAYER7_TIER3_TEARDOWN.md` (the three shapes, THE RULE, the timers) **but corrects it** — see the next
section; the Tier-3 doc predates the Model-A lock and the on-demand doc. **Code wins over doc** — the
shipped broker (`StartSession`/`InstallSession`, shard routing, XOR), `exch.Stop()` semantics, and the
already-built L7 spin-up/join (`runCanonicalTimer`, `findLiveCanonical`, the `/v1/ws` dispatcher,
registry patterns) are verified; bind to them exactly.

## What it IS (one line)
Three winding-down flows for a **canonical** market: **(1) explicit Leave = detach-keep**, **(2) one
idempotent Join/Rejoin door + a `/session/current` reconnect resolve**, **(3) Shape B scheduled close**
(timer at `closeAt` → one `FinalizeMarket` barrier → L2 wind-down → `exch.Stop()` → reap). On-demand
teardown (Shape C) is the sibling doc; this one does not re-derive bring-up/join.

## ⚠ THIS DOC CORRECTS THE TIER-3 DOC (do not "fix" code back toward it)
All four agreed this session, LOCKED:
1. **Shape A is NOT a teardown.** Canonical leave = **detach-keep**, not "drop + free" — and it **DOES
   signal the broker** (Tier-3 said "no broker signal on leave"; that breaks rejoin). (CT.2)
2. **Accidental nav ≠ leave.** A dropped socket / back-button tears **nothing** down (reconnect). Only
   the explicit Leave button is detach-keep. (CT.1)
3. **Shape B uses ONE `FinalizeMarket` + one barrier**, not N per-session `StopSession(save)` + N acks —
   detached accounts are off L7's books, so only the broker can sweep them. (CT.7)
4. **No L7 market-data unwire anywhere** (Model A — the browser's socket close + shipped `Disconnect`
   handle the feed). (CT.3)

## BROKER SEAMS (jointly owned — propose + bind; the broker confirms struct/shard, same as `WireInstance`)
The broker ships `StartSession`/`InstallSession` (shard-routed `hash(AccountID)`, per-account XOR →
`ErrSessionAlreadyLive`). It ships **no** session-end/save today (`FireStop` = stop-orders; "SessionSave"
is only a forward comment in `install_session.go`). These are the broker's Tier 3 — L7 specifies the
facing contract + required behavior; **struct shape + shard delivery are the broker's to confirm.** Do
**not** treat them as shipped, and do **not** build their bodies in L7:
- **B-1 `EndSession(ctx, SessionEndCmd{AccountID, SessionID, Mode})`** — dual-mode. **Canonical =
  detach-keep:** route to owning shard (`hash(AccountID)`, command+reply like `InstallSession`);
  uninstall the session binding → **free per-account XOR**; **keep** the `LoadedAccount` (positions carry
  to EOD); no persist/square/unload. Synchronous return = ack. (OnDemand = discard → sibling.)
- **B-2** loaded-account-without-session + **market→accounts index**: each `LoadedAccount` stays tagged
  with its `ExchangeID` (from `StartSession`) so it survives detach and the close sweep can enumerate
  **detached** accounts L7 no longer tracks.
- **B-3** pre-close **auto-square is broker-owned**, attachment-independent, self-triggered before close.
  **L7 never triggers / sequences / waits on it.** Broker learns close time **at wire** (flag: pin this
  wire-time detail).
- **B-4 `FinalizeMarket(ctx, ExchangeID)`** — called **once** by L7 at `closeAt`; persists (honoring
  `PersistOnClose` — canonical = `Durable` = true) + uninstalls + unloads **all** accounts tagged
  `exchangeID` (attached **and** detached). Positions already flat (B-3) → pure save+cleanup, no exchange
  needed. Return = **the single per-market barrier** L7 waits on.
- **B-5** `StartSession` must **re-install over carried positions** on a loaded, session-less account
  (XOR freed in B-1 → no `ErrSessionAlreadyLive`) — this is the fresh-rejoin path.
- **B-6** `EndSession` + `FinalizeMarket` are **synchronous** (error = ack).

## NEW in L7 (what this phase builds)
1. **Registry states (do first — the compile seam):** `Session.State` gains `Stopping` (the **only**
   teardown/leave guard; `Ended` stays **vestigial** — sessions are **deleted** on reap/leave, never
   tombstoned). `MarketState` gains `TearingDown` (then deleted on reap). (§2, CT.10)
2. **`BrokerPort` grows** `EndSession` + `FinalizeMarket`; **`AgentOrchestrator` grows** `WindDown(tag,
   mode)` (fire-and-forget; signature **will** change with L2).
3. **Routes:** `POST /session/{sessionID}/exit` → `handleExit` (mode-branched — canonical here); `POST
   /session/current` → `handleCurrent` (reconnect resolve, shared with on-demand); `POST
   /session/canonical` already exists — §6 makes it **idempotent** for the unified Join/Rejoin.
4. **The three flow handlers** + the extended `runCanonicalTimer` (open **and** close).

## Load-bearing rules (from the spec)
- **Two "away" events, different (CT.1/CT.2):** accidental nav/drop → nothing torn down, `Session`
  stays `Active`, `byUser` holds, broker session installed → **reconnect** re-attaches the same session.
  Explicit Leave → **detach-keep**: `EndSession(Canonical)` frees XOR + keeps account loaded; L7 drops
  session + frees `byUser` → **fresh** rejoin (new session, same loaded account). Detach-keep frees XOR
  (user may go on-demand / another market); reconnect does not.
- **Leave reverts on failure (§5):** flip `Active→Stopping` (guard) under lock; off-lock `EndSession`
  (bounded ack); on error **revert `Stopping→Active`** and return 503/504 — the user is **never**
  half-detached. On success delete `sessions[sid]` + `members[sid]` + `byUser[user]` → 200.
- **One Join/Rejoin door (§6, CT.4):** `POST /session/canonical` — same-market + `Active` existing →
  **R1 reconnect**: return the existing `{sid, pid}` as-is, **no broker call**. Live-elsewhere /
  transient `Stopping` → **409**. Else **R2 fresh-join** (after-leave or first): `EnsureAccount` (same
  durable account, still loaded) + `StartSession` re-install over carried positions (B-5). Refines join
  J.4 (same-market is no longer a hard 409).
- **`/session/current` resolve (§6.2, CT.5):** `byUser` → `{session_id, mode, market, status,
  warmup_ends_at?, session_ends_at?}` — the `*_ends_at` **only** for on-demand; miss → **404** (→ UI
  shows no rejoin). Folds in P6; shared with the sibling.
- **Shape B (§7, CT.6):** one goroutine per market drives open **and** close (extends spin-up §6.1,
  reuses `Market.closeAt`). `shapeB_close`: lock → run-once guard (`!Live` → return) → flip
  `TearingDown` (joins now rejected: `findLiveCanonical` skips non-Live; dispatcher → 404) → off-lock
  `FinalizeMarket` (**one** barrier, bounded; log + **proceed** on error — forward-progress, T3.9) →
  `WindDown` (L2 fire-and-forget) → `exch.Stop()` → lock → reap members + `byUser` + `markets`.
- **THE RULE:** lock held **only** for the µs regions (guard flips, reserve, reap) — **never** across
  `EndSession` / `FinalizeMarket` / `WindDown` / `exch.Stop()`.

## Do NOT
- tear anything down on accidental nav / socket drop — that's reconnect, session persists `Active` (CT.1);
- `exch.Stop()` on leave — **only** Shape B (CT.3);
- unwire market-data anywhere in L7 — Model A, the socket close + shipped `Disconnect` own it (CT.3);
- trigger / sequence / wait on the pre-close square — it's the broker's own (B-3/CT.8);
- do N per-session saves or an N-ack barrier — one `FinalizeMarket` + one `Stop` + reap (CT.7);
- hard-409 a **same-market** reconnect — it's R1, return the existing session, no broker touch (CT.4);
- leave a user half-detached — revert `Stopping→Active` if `EndSession` fails;
- build the broker seam bodies in L7, or treat `EndSession`/`FinalizeMarket` as shipped — propose + flag;
- keep `Ended` tombstones — delete on reap/leave; `Stopping` is the only guard (CT.10);
- fork the timer — **extend** spin-up §6.1 (one goroutine, open + close);
- hold the registry lock across any call-out (THE RULE).

## Deps & seam
L7-0 scaffold; the **shipped** L7 spin-up + join (`runCanonicalTimer`/`spinUpCanonical`, `Market.closeAt`,
`findLiveCanonical`, the `/v1/ws` dispatcher, registry reserve/finalize/rollback, `BrokerPort`); shipped
broker `StartSession`/`InstallSession` + XOR; the **jointly-owned** broker seams B-1…B-6 (flag, don't
build); `simulator/exchange/wire` `exch.Stop()` (inbound→shards drain→tape closes→market-data
self-cleans); `lifecycle.Canonical`/`OnDemand` + `PersistOnClose`; the **stub** `AgentOrchestrator.
WindDown` (L2).

## Flags (orientation)
- **Registry states first** (`Stopping`, `TearingDown`) — the compile seam.
- Broker seams B-1…B-6 are the broker's Tier 3 (`PROPOSED → broker`, CT.11) — same status
  `WireInstance`/`StartSession` had; L7-facing contract here, struct/shard the broker's to confirm.
- **Broker close-time at wire (B-3/CT.8)** — how the broker learns a market's close instant to
  self-square is a **wire-time detail to pin** with the broker.
- `WindDown`/`BringUp` signatures **will** change with L2 (placeholder ports; same as spin-up §8).
- Server-crash recovery — a mid-day crash loses the live session; next day's timer rebuilds (spin-up
  C.9 / Tier-0 §6). **Not solved here.**
- On-demand teardown (Shape C) — sibling doc; reuses `EndSession(OnDemand=discard)`, `/session/current`,
  the exit endpoint's on-demand branch. Out of scope.

## Tests (objective; no goldens/auditor)
Against a **fake broker** (`EndSession`/`FinalizeMarket`/`StartSession` with XOR + a market→accounts
index), **stub `WindDown`**, injected `now`/`closeAt`, fake exchange handle:
- **`handleExit` (canonical):** unknown → 404; `!Active` → 409; `EndSession` err/timeout → **revert to
  `Active`** + 503/504, **no** session/`byUser` residue removed; success → `sessions`/`members`/`byUser`
  all gone, 200; assert **no** market-data call; THE RULE (no lock across `EndSession`) — structural.
- **`POST /session/canonical` idempotency:** same-market `Active` → **same** `sid`/`pid`, **zero**
  broker calls (R1); live-elsewhere / `Stopping` → 409; after-leave → `StartSession` **re-install**
  (XOR free) → new `sid`, same account, positions carried (R2); `StartSession` err → `rollback`.
- **`/session/current`:** `byUser` hit → session + mode + timers (`*_ends_at` only when mode on-demand);
  miss → 404; pure read.
- **`shapeB_close`:** run-once guard (`!Live` → return, no double-reap); `TearingDown` flip rejects a
  racing join/reconnect (409 / dispatcher 404); `FinalizeMarket` err → **proceeds** (forward-progress);
  members reaped + `byUser` freed + market deleted; a **detached** user (not in `members`) is **not**
  touched by L7 but **is** swept by the fake `FinalizeMarket` (assert via the index); `exch.Stop()` once;
  THE RULE.
- **timer:** open-arm → close-arm sequence; spin-up fail → **skip** teardown, reloop next weekday
  (injected `now`).
- `go test ./... -race`.