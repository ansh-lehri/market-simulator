Layer 7 — On-Demand Teardown & Reconnect: Implementation Design

What this doc is. How a private, single-user on-demand instance winds down — Shape C — and the nav-vs-exit distinction around it:

Explicit exit/leave button → full teardown now; no rejoin (the instance is theirs and dies).
Duration timeout → the per-session reap timer fires the same teardown at session_ends_at.
Accidental nav / back button / socket drop → not teardown; the instance stays live → reconnect (rejoin) available.

Sibling / reuse. This is the on-demand half of Tier 3; the canonical half is claude/LAYER7_CANONICAL_TEARDOWN_IMPL.md, which defines the shared pieces this doc reuses: the EndSession seam (here used in its Mode=OnDemand = discard branch), POST /session/current (the reconnect resolve), the exit endpoint skeleton, THE RULE, and the registry states. It also reuses claude/LAYER7_ONDEMAND_SPINUP_IMPL.md (m.closeAt = session_ends_at = T0 + warmupLen + duration, m.warmupEndsAt, ModeOnDemand, the od:<sid> instance) and the /v1/ws dispatcher (join doc §7). Builds on LAYER7_TIER3_TEARDOWN.md Shape C / §3–4 and T3.10/T3.11/T3.14.

Typed against simulator/broker/funnel, simulator/economy/lifecycle, simulator/contract, simulator/exchange/wire. Repo layout unspecified; path strings illustrative.

1. The model (exit-vs-nav is automatic)
Event	Teardown?	Backend effect	Rejoin
Explicit exit/leave	yes (Shape C)	discard ephemeral account, Stop(), reap, free byUser	none — instance gone; UI offers "start new"
Duration timeout (reap timer at session_ends_at)	yes (Shape C)	same as exit	none
Accidental nav / socket drop	no	nothing torn down; instance live (agents trading, timer running); Session Active, byUser holds	reconnect to the same private instance

The distinction needs no flag. It's purely "does the session still exist in byUser?": nav → session survives → POST /session/current returns it → rejoin works; exit/timeout → Shape C reaped it + freed byUser → POST /session/current → 404 → no rejoin (UI: "session ended, start a new one"). The teardown reaping the session is what removes the rejoin option.

On-demand has no detach-keep (unlike canonical leave): the market is the user's, so an explicit exit is a full teardown, not a detach. There is therefore no "fresh rejoin" for on-demand — once gone, the user starts a new on-demand (fresh spin-up), they don't rejoin.

2. Two triggers, converging (T3.10)

Explicit exit (exit endpoint, on-demand branch) and duration timeout (the reap timer) both call shapeC_teardown(marketID) and can race (user exits as the timer fires). The Live → TearingDown flip resolves it: whoever takes the lock first runs the teardown; the other sees TearingDown (or a gone market) and no-ops. Idempotent by the flip.

2.1 The reap timer (per-session, armed off m.closeAt)

One short-lived goroutine per on-demand session (T3.14, Option 1 — per-session, not a shared reaper), spawned at the end of on-demand spin-up, armed to m.closeAt (= session_ends_at = T0 + warmupLen + duration from the on-demand spin-up doc) — not "ondemand.duration from now," so the warm-up window is counted.

spawnOnDemandReap(marketID, closeAt):             // added to the on-demand spin-up finalize
    go func():
        timer := NewTimer(closeAt - now())
        select:
          case <-timer.C:        shapeC_teardown(marketID)     // duration elapsed
          case <-conductorQuit:  timer.Stop(); return          // process shutdown
    // if the user exits first, shapeC runs via the exit path; this timer firing after is a
    // safe no-op (TearingDown flip). The goroutine ends when shapeC completes or the timer fires.

Cross-doc: the on-demand spin-up doc's finalize gains this spawnOnDemandReap(marketID, m.closeAt) call (it previously computed m.closeAt but spawned no reaper — Tier 3 adds it). At m.closeAt the exchange clock also hits PhaseClosed, which does not stop the instance — L7's reap timer is what tears it down via exch.Stop().

3. Shape C — the teardown
shapeC_teardown(marketID):                 // marketID = "od:<sessionID>"
  lock:
    m := markets[marketID] ; m==nil || m.State != Live → unlock → return     // run-once; resolves exit-vs-timeout race
    m.State = TearingDown
    sid := the single member of m.members ; sess := sessions[sid]
    acct, user, exch, exchID := sess.Account, sess.User, m.exch, string(m.ID)
  unlock
  // off-lock (THE RULE):
  err := broker.EndSession(ctx, funnel.SessionEndCmd{AccountID:acct, SessionID:funnel.SessionID(sid),
                                                     Mode:lifecycle.OnDemand})   // DISCARD; wait ack (bounded; log+proceed)
  if err != nil → log "discard incomplete, proceeding"                          // forward-progress (T3.9); N=1 barrier
  agents.WindDown(tag, OnDemand)                                                // L2 fire-and-forget — discard throwaway population
  exch.Stop()                                                                   // tape closes → market-data self-cleans (nothing persisted)
  lock:
    delete sessions[sid] ; delete markets[marketID]
    if byUser[user] == sess → delete byUser[user]                              // frees XOR → user may start another on-demand / join canonical
  unlock

Differences from canonical Shape B:

EndSession(OnDemand=discard), not FinalizeMarket. One ephemeral account, thrown away.
No pre-close square — on-demand positions are discarded, not squared/saved (PersistOnClose = false, OD.7; Tier-0 §2.2 session_discard never writes Postgres).
N = 1 barrier (the single discard ack).
No operator (on-demand was never operator-seeded, T3.11).
4. Broker contract (on-demand — the EndSession discard branch)

No new broker seam beyond the EndSession defined in the canonical doc (§3 B-1) — this is its Mode=OnDemand branch:

go
broker.EndSession(ctx, funnel.SessionEndCmd{AccountID, SessionID, Mode: lifecycle.OnDemand})

Internal (discard): route to the owning shard (hash(AccountID)); uninstall the session binding; drop the ephemeral LoadedAccount entirely — never write Postgres (the account was loaded with PersistOnClose=false at the on-demand seal, OD.7). Synchronous return = the ack. Contrast the canonical branch (detach-keep, account preserved).

No FinalizeMarket, no market→accounts sweep, no auto-square for on-demand — a single ephemeral account that is discarded wholesale.

5. Exit endpoint — on-demand branch

The shared exit handler (canonical doc §4/§5) branches on mode; on-demand is a straight Shape C:

POST /session/{sessionID}/exit          // handleExit — on-demand branch
  defer recoverPanic
  lock: sess := sessions[sid] ; nil → unlock → 404 ; m := markets[sess.Market] ; mode := m.Mode ; marketID := m.ID
  unlock
  if mode == OnDemand:
     shapeC_teardown(marketID)           // flip is inside; race-safe with the reap timer
     → 200 { ok }                        // idempotent: if the timer already tore it down, flip no-ops, still ok

Status table — /session/{id}/exit (on-demand):

Condition	Status
success (torn down)	200 {ok}
unknown session	404
already torn down (timer won the race)	200 {ok} (idempotent)
EndSession discard err/timeout	200 {ok} + logged — forward progress always reaches Stop(); the user's intent (end it) is honored regardless
6. Reconnect (accidental nav → the rejoin option)

Nothing torn down; the od:<sid> instance is still live (agents trading into its single-subscriber FanOut, duration timer running). Rejoin = reconnect via the shared POST /session/current (canonical doc §6.2), which returns the on-demand timers so the UI countdown resumes:

POST /session/current   { user packet }
  → byUser[user] (still present) → 200 { session_id, mode:OnDemand, market, status:"active",
                                         warmup_ends_at, session_ends_at }
  → client re-opens /v1/ws?session=<session_id> → dispatcher → od instance's single-subscriber FanOut
While the socket is dropped, the FanOut has zero subscribers — agent publishes drop harmlessly (same as canonical with no humans). On reconnect the browser re-subscribes (shipped in-band path, join doc §8); shipped Disconnect had cleaned the old sub.
Race with the duration timeout: reconnect just before session_ends_at → gets the session, re-opens WS, then the reap timer fires Shape C → exch.Stop() closes the socket + the UI sees session-closed. Reconnect just after → byUser freed → 404 → "session ended." Both clean.
7. Decision log (On-Demand Teardown)
#	Decision	Status
OT.1	Three triggers: explicit exit (Shape C now), duration timeout (Shape C at session_ends_at), accidental nav (no teardown → reconnect)	LOCKED
OT.2	Exit-vs-nav distinction is automatic via byUser survival — no "away" flag; teardown reaping the session removes the rejoin option	LOCKED
OT.3	On-demand has no detach-keep — explicit exit is a full teardown; no fresh-rejoin, only start-new	LOCKED
OT.4	Reap timer = one goroutine per session, armed off m.closeAt (= session_ends_at, warm-up counted), spawned at spin-up finalize; idempotent via the flip	LOCKED (reconciles T3.14 "duration" with the on-demand warm-up model)
OT.5	Shape C = flip TearingDown → EndSession(OnDemand=discard) (N=1 barrier) → L2 wind-down (fire-and-forget, discard) → exch.Stop() → reap + free byUser; no square, no FinalizeMarket, no operator	LOCKED
OT.6	Broker: EndSession Mode=OnDemand branch = uninstall + drop ephemeral account, never persist (PersistOnClose=false); no new seam beyond the canonical doc's EndSession	PROPOSED → broker
OT.7	Reconnect reuses POST /session/current + the /v1/ws dispatcher; returns warmup_ends_at/session_ends_at so the UI countdown resumes	LOCKED
8. Flagged / out of scope
EndSession seam itself (dual-mode, shard delivery) is the broker's Tier 3 to confirm — defined in the canonical doc §3; this doc only pins its discard branch.
WindDown/BringUp signatures change with L2 (placeholder ports).
Server-crash recovery — a crash loses the ephemeral instance entirely (nothing persisted by design); the user starts a new on-demand. No recovery path (nor wanted).
Spawn point — spawnOnDemandReap is an addition to the on-demand spin-up finalize; update claude/LAYER7_ONDEMAND_SPINUP_IMPL.md when implementing so the reaper is spawned there.

End — on-demand teardown. Sibling: canonical teardown (shared EndSession + /session/current).