Layer 7 — Canonical Teardown, Leave & Rejoin: Implementation Design

What this doc is. How a canonical market and its human sessions wind down, plus the leave/rejoin/reconnect paths around it. Three flows:

Explicit Leave — a user deliberately leaves a live canonical market (detach-keep).
Join / Rejoin (one door) — first join, reconnect-after-nav, and rejoin-after-leave, unified into one idempotent endpoint; plus the POST /session/current reconnect resolve.
Shape B — scheduled close — the whole market winds down and persists at closeAt.

What it is NOT. On-demand teardown (Shape C) is the sibling doc claude/LAYER7_ONDEMAND_TEARDOWN_IMPL.md — it reuses the broker contract and the reconnect resolve defined here. This doc also does not re-derive bring-up/join (companion docs).

Authority & reconciliation. Builds on LAYER7_TIER3_TEARDOWN.md (the three shapes, THE RULE, the timers) but corrects it where later-locked decisions override — the Tier-3 doc predates the Model-A lock and the on-demand doc. The corrections (all agreed this session):

Shape A is not a teardown. Canonical leave is detach-keep, not "drop + free" — and it does signal the broker (the Tier-3 doc said "no broker signal"; that breaks rejoin).
Accidental nav ≠ leave. A dropped socket / back-button tears nothing down (reconnect); only the explicit Leave button is detach-keep.
Shape B uses one FinalizeMarket + one barrier, not N per-session StopSession(save)
N acks — because left (detached) accounts are off L7's books, so only the broker can sweep them.
No L7 market-data unwire anywhere (Model A — join doc J.5/§10: the browser's socket close + shipped Disconnect handle the feed).

Typed against shipped code: simulator/broker/funnel, simulator/economy/lifecycle, simulator/contract, simulator/exchange/wire. Reuses claude/LAYER7_CANONICAL_SPINUP_IMPL.md (the per-market timer, Market.closeAt, exch.Stop() semantics, THE RULE), claude/LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md (findLiveCanonical, the /v1/ws dispatcher, registry reserve/finalize/rollback, BrokerPort). Repo layout unspecified; L7 types shown as Go without an import path, external packages by real import path; HTTP path strings illustrative (HTTP-surface note).

1. The model (two "went away" events, very different)
Event	What it is	Backend effect	Rejoin path
Accidental nav / back / socket drop	user didn't press Leave	nothing torn down; Session stays Active, byUser holds, broker session installed, account carries	reconnect — re-attach the same session
Explicit Leave	user pressed Leave	detach-keep: broker uninstalls the session (frees per-account XOR) but keeps the account loaded (positions carry to EOD); L7 drops the session + frees byUser	fresh rejoin — new session on the same loaded account
Scheduled close	L7 timer at closeAt	Shape B — broker sweeps+saves all accounts, exch.Stop(), reap	n/a (market gone)

The two "away" events are distinct because detach-keep frees the XOR (so a left user can go start on-demand / join another market) while reconnect does not (same session resumes). Both land the user back with their positions, via different mechanisms. The account only ever persists + unloads at the scheduled close; the broker auto-squares it pre-close regardless of attachment.

2. Registry states used
go
// Session.State: Starting → Active → Stopping   (Stopping = the teardown/leave guard)
// Ended is VESTIGIAL — sessions are DELETED on reap/leave, never kept as a tombstone
//   (Tier-0 "no Reaped state" rule); only Stopping is used, as the transient guard.
// MarketState: Provisioning → Live → TearingDown (then DELETED on reap)
3. Broker contract (canonical teardown + leave/rejoin)

The broker ships StartSession/InstallSession (shard-routed by hash(AccountID), per-account XOR → ErrSessionAlreadyLive). It ships no session-end/save today (FireStop is stop-orders; "SessionSave" is only a forward comment in install_session.go). So the seams below are the broker's Tier 3 — jointly owned: L7-facing contract + required behavior specified here; struct shape + shard delivery are the broker's to confirm (same status WireInstance/StartSession had).

B-1. EndSession — dual-mode session-end seam (NEW).

go
func (b *funnel.Broker) EndSession(ctx context.Context, cmd funnel.SessionEndCmd) error
type funnel.SessionEndCmd struct {
    AccountID funnel.AccountID
    SessionID funnel.SessionID
    Mode      lifecycle.SessionType   // Canonical → DETACH-KEEP ; OnDemand → DISCARD (sibling doc)
}

Canonical (detach-keep): route to the owning shard (hash(AccountID), command+reply like InstallSession); shard handler uninstalls the session binding → frees the per-account session XOR; keeps the LoadedAccount on the shard — no persist, no square, no unload. Synchronous return = the ack. (OnDemand branch = discard; sibling doc.)

B-2. Loaded-account-without-session + market→accounts index (NEW, internal). After EndSession(canonical) the account is loaded but session-less. The broker must support that, and keep each LoadedAccount tagged with its ExchangeID (from the original StartSession) so it survives detach — because the pre-close square and FinalizeMarket must enumerate all of a market's accounts, including detached ones L7 no longer tracks. This index is what makes "broker owns the close sweep" implementable.

B-3. Pre-close auto-square (broker-owned, T3.3). The broker self-triggers squaring of all canonical positions for a market shortly before close, while the market is still accepting, attachment-independent (walks B-2's index). L7 never triggers/sequences/waits on it (T3.3). The broker must know the market's close time to self-trigger — passed at wire/bring-up time (L7 owns the schedule). Flag: how the broker learns close time is a wire-time detail to pin.

B-4. FinalizeMarket — close sweep seam (NEW).

go
func (b *funnel.Broker) FinalizeMarket(ctx context.Context, exchangeID funnel.ExchangeID) error

Called once per market by L7 at closeAt. Fans out to all shards; for every canonical LoadedAccount tagged exchangeID (attached and detached): persist to Postgres (honoring PersistOnClose — canonical = Durable = true) + uninstall any remaining session + unload. Positions already flat from B-3, so this is pure state-save + cleanup (no exchange needed). Returns when the whole market is finalized = the single per-market barrier L7 waits on. Replaces the Tier-3 doc's N-session StopSession(save) + N-ack barrier.

B-5. Re-install on a loaded, session-less account (behavior). StartSession must succeed when the account is loaded but has no session (post-EndSession): re-install a fresh binding over the carried positions. XOR was freed in B-1 → no ErrSessionAlreadyLive. This is the fresh-rejoin path.

B-6. Acks. EndSession and FinalizeMarket are synchronous (error = ack); internally shard command+reply, outer call blocks until done.

Broker-side assumptions this rests on (flagged, not L7's to solve): the broker keeps a detached canonical account live until the EOD sweep and squares it regardless of attachment (B-3); L2's wind-down never needs the live exchange.

4. Conductor additions
go
// BrokerPort gains (join doc already had WireInstance/EnsureAccount/StartSession):
//   EndSession(ctx, funnel.SessionEndCmd) error
//   FinalizeMarket(ctx, funnel.ExchangeID) error
// AgentOrchestrator gains:  WindDown(tag MarketTag, mode Mode)   // fire-and-forget; signature WILL change with L2
// Routes added on bringup.Runtime.Mux:
//   POST /session/{sessionID}/exit   → handleExit      (mode-branched: canonical here, on-demand = sibling)
//   POST /session/current            → handleCurrent   (reconnect resolve; also serves canonical + on-demand)
//   (POST /session/canonical already exists — §6 makes it idempotent for the unified Join/Rejoin)
5. Flow 1 — Explicit Leave (detach-keep)
POST /session/{sessionID}/exit          // handleExit — canonical branch
  defer recoverPanic
  lock:
    sess := sessions[sid] ; nil → unlock → 404
    m := markets[sess.Market] ; mode := m.Mode
    if mode == Canonical:
       if sess.State != Active → unlock → 409                 // double-leave guard
       sess.State = Stopping                                  // guard flip
       acct, user := sess.Account, sess.User
    // (mode == OnDemand → sibling doc: shapeC_teardown)
  unlock
  // off-lock (THE RULE):
  err := broker.EndSession(ctx, funnel.SessionEndCmd{AccountID:acct, SessionID:funnel.SessionID(sid),
                                                     Mode:lifecycle.Canonical})   // detach-keep; wait ack (bounded)
  if err != nil:
     lock: sess.State = Active ; unlock                       // REVERT — user stays in, nothing orphaned
     → 503 (load/err) / 504 (timeout)
  lock:
    delete sessions[sid] ; delete markets[sess.Market].members[sid]
    if byUser[user] == sess → delete byUser[user]             // frees XOR → user may go on-demand / another market
  unlock
  → 200 { ok }

No market-data action (Model A). The account stays loaded + carrying in the broker (B-1) and is finalized at close (B-4). On EndSession failure L7 reverts Stopping→Active so the user is never half-detached.

Status table — /session/{id}/exit (canonical):

Condition	Status
success (detached)	200 {ok}
unknown session	404
session not Active (already leaving/gone)	409
EndSession load/err	503
EndSession timeout	504
6. Flow 2 — The one Join/Rejoin door + reconnect resolve
6.1 POST /session/canonical — idempotent on same-market (the unified door)

Refines join doc J.4: byUser on the same market is no longer a hard 409 — it's a reconnect (returns the existing session). First-join, rejoin-after-leave, and reconnect-after-nav all use this one endpoint.

POST /session/canonical   { user, market }
  lock:
    m := findLiveCanonical(tag) ; nil → unlock → 409 market_not_live
    existing := byUser[user]
    if existing != nil:
       if existing.Market == m.ID && existing.State == Active:
          sid, pid := existing.ID, existing.Participant
          unlock → 200 {sid, pid, "active"}                   // R1 RECONNECT: return as-is, NO broker call
       unlock → 409 already_in_session                        // live elsewhere / transient Stopping
    sid := mintSessionID()
    sess := &Session{ID:sid, User:user, Market:m.ID, AccountType:Durable, State:Starting}
    sessions[sid]=sess ; byUser[user]=sess ; inbound, marketID := m.inbound, m.ID
  unlock
  // off-lock fresh-join (R2 after-leave, or first join):
  acct := broker.EnsureAccount(ctx, lifecycle.OwnerHuman, string(user))     // same durable account (still loaded post-leave)
  pid  := mintParticipantID()
  err  := broker.StartSession(ctx, funnel.SessionStartCmd{AccountID:acct, SessionID:funnel.SessionID(sid),
             Mode:lifecycle.Canonical, ExchangeID:funnel.ExchangeID(marketID), ExchangeInbound:inbound,
             Participant:contract.ParticipantID(pid)})                      // re-install over carried positions (B-5)
  if err → rollback(sid,user) → map err (join doc §6.3; ErrSessionAlreadyLive shouldn't occur — leave freed XOR)
  lock: sess.Account, sess.Participant, sess.State = acct,pid,Active ; markets[marketID].members[sid]=struct{}{} ; unlock
  → 200 {session_id:sid, participant_id:pid, status:"active"}

Client always: POST → open /v1/ws?session=<sid>. R1 returns the same session (no broker touch); R2 re-installs on the same loaded account → same positions.

6.2 POST /session/current — reconnect resolve (P6, folded in)

For a client that lost its sessionID (full reload) and needs it back to re-open the WS.

POST /session/current   { user packet }
  lock:
    sess := byUser[user] ; nil → unlock → 404 no_session
    m := markets[sess.Market]
    resp := { session_id: sess.ID, mode: m.Mode, market: m.Tag, status: sess.State,
              warmup_ends_at?: m.warmupEndsAt, session_ends_at?: m.closeAt }   // the *_ends_at only for on-demand
  unlock → 200 resp
  // client re-opens /v1/ws?session=<session_id> → existing dispatcher (join doc §7)

Shared with on-demand (sibling doc). After an explicit leave / close / on-demand teardown, byUser is freed → 404 → UI shows "no session." This is what makes the rejoin option appear only when a session actually survives.

7. Flow 3 — Shape B (scheduled close)

The same per-market timer goroutine that drives bring-up, extended to also drive close (reuses Market.closeAt committed at spin-up).

runCanonicalTimer(tag):                     // EXTENDS spin-up doc §6.1
  loop:
     arm to (open − preopen_lead) → spinUpCanonical(tag) → (marketID, closeAt)   // bring-up (C.*)
     if spin-up failed → reloop (next weekday); skip teardown
     arm to closeAt → shapeB_close(marketID)
     reloop → next weekday pre-open

shapeB_close(marketID):
  lock:
    m := markets[marketID] ; m==nil || m.State != Live → unlock → return          // run-once; already torn/gone
    m.State = TearingDown                                                          // joins now rejected (findLiveCanonical skips non-Live)
    members := copy(m.members) ; exchID := string(m.ID) ; exch := m.exch
  unlock
  // off-lock (THE RULE) — broker already auto-squared pre-close, positions flat (B-3 / T3.3):
  if err := broker.FinalizeMarket(ctx, funnel.ExchangeID(exchID)); err != nil:
     log "finalize incomplete, proceeding"                                         // ONE barrier, bounded; forward-progress (T3.9)
  agents.WindDown(tag, Canonical)                                                  // L2 fire-and-forget (flatten + persist)
  exch.Stop()                                                                      // inbound→shards drain→tape closes→market-data self-cleans
  lock:
    for sid in members: u := sessions[sid].User ; delete sessions[sid]
                        if byUser[u]==sessions[sid] → delete byUser[u]
    delete markets[marketID]                                                       // reap; exch handle drops out of scope
  unlock
Operator not triggered (dies with Stop(), T3.8). L2 fire-and-forget. Only off-lock work between the two µs locks (THE RULE).
A left (detached) user isn't in members → L7 does nothing for them; the broker's FinalizeMarket sweep persists their carried account (that's the whole reason B-4 sweeps by market, not by L7 members).
A reconnect racing teardown sees TearingDown → /session/canonical 409 / the /v1/ws dispatcher sees non-Live → 404. Clean.
No per-session save, no N-ack barrier — one FinalizeMarket + one Stop + reap.
8. Decision log (Canonical Teardown)
#	Decision	Status
CT.1	Accidental nav / socket drop ≠ leave: nothing torn down, session persists Active, reconnect re-attaches the same session	LOCKED
CT.2	Explicit Leave = detach-keep: broker.EndSession(Canonical) uninstalls session + frees per-account XOR, keeps the LoadedAccount loaded (positions carry to EOD); L7 drops session + frees byUser; rejoin = fresh join	LOCKED (reverses Tier-3 "no broker signal on leave")
CT.3	exch.Stop() only in Shape B (not on leave); no L7 market-data unwire anywhere (Model A)	LOCKED
CT.4	One Join/Rejoin door: POST /session/canonical idempotent on same-market (reconnect) else fresh-join; refines join J.4	LOCKED
CT.5	POST /session/current = reconnect resolve (byUser → session + timers); folds in P6; 404 when no live session (→ no rejoin shown)	LOCKED
CT.6	Shape B = flip TearingDown → FinalizeMarket(exchID) (one barrier) → L2 wind-down (fire-and-forget) → exch.Stop() → reap + free all byUser	LOCKED
CT.7	Close save = broker FinalizeMarket sweep over ALL accounts (attached + detached), not L7 per-session; replaces Tier-3 N-ack barrier	LOCKED
CT.8	Pre-close auto-square is the broker's own (B-3/T3.3); L7 never triggers/waits; broker learns close time at wire	LOCKED (wire detail flagged)
CT.9	Canonical timer = one goroutine per market driving open and close (extends spin-up §6.1)	LOCKED
CT.10	Session.Stopping is the only teardown guard used; sessions deleted on reap/leave (Ended vestigial)	LOCKED
CT.11	Broker seams EndSession(dual-mode) + FinalizeMarket + market→accounts index + re-install-on-loaded are the broker's Tier 3 to confirm	PROPOSED → broker
9. Flagged / out of scope
On-demand teardown (Shape C) — sibling doc claude/LAYER7_ONDEMAND_TEARDOWN_IMPL.md (reuses EndSession(OnDemand=discard), /session/current, the exit endpoint's on-demand branch).
Broker close-time at wire — how the broker learns a market's close instant to self-trigger the square (B-3) is a wire-time detail to pin with the broker.
WindDown/BringUp signatures will change with L2 (placeholder ports; same as spin-up §8).
Server-crash recovery — a mid-day crash loses the live session; next day's timer rebuilds (spin-up C.9 / Tier-0 §6). Not solved here.

End — canonical teardown. Sibling: on-demand teardown.