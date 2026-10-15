Layer 7 — On-Demand Session Spin-Up (Trigger + Warm-Up + WS): Implementation Design

What this doc is. The implementation design for a user starting an on-demand (private, single-user) practice session: the HTTP trigger POST /session/ondemand, the held-setup + UI-timed warm-up model, the status fallback, and the /v1/ws connect (same dispatcher as canonical, resolving to the on-demand instance). On-demand = the canonical spine wrapped in a money gate + fused spin-up + warm-up, so it reuses the two companion docs and states only the deltas.

Reuses (do not re-derive):

claude/LAYER7_CANONICAL_SPINUP_IMPL.md — NewExchange/Start, broker.WireInstance, marketData.WireInstance, BRING_UP, buildExchangeArgs/readReference, THE RULE.
claude/LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md — EnsureAccount, StartSession/ SessionStartCmd, the registry reserve/finalize/rollback pattern, and the entire /v1/ws dispatcher (§7 there) — on-demand reuses it unchanged.

Authority. LAYER7_TIER2_ADMISSION.md §3 (on-demand admit, gate, seal), LAYER7_TIER2_SEEDING.md §8 (on-demand seeding, warm-up), LAYER7_TIER1_* §2.5/§8 (on-demand deltas), LAYER7_TIER2_HTTP_SURFACE.md (revised — see OD.1). Contracts typed against simulator/broker/funnel, .../marketdata, simulator/economy/lifecycle, simulator/contract. Repo layout unspecified.

1. The model (how one HTTP call drives a multi-stage spin-up)

The HTTP response fires once. We hold the request through the deterministic setup only (gate → populate → construct → wire → Start → seal), return at warm-up-start, and let the UI self-time the 2-minute book warm-up against an absolute instant the server returns. The long part (warm-up) is neither held nor polled.

POST /session/ondemand?market=IN&duration=35        ← held through setup; returns ONCE
  inline (fast): reserve(XOR) → EnsureAccount → gate = QualifyOnDemand
      gate NO  → delete reservation → return { status:"insufficient_funds", message }   (quick)
      gate YES → (held setup, bounded by setupTimeout):
          BRING_UP(onDemand) [sync]                          // populate throwaway agents
          T0 := now ; construct 1-shard exchange(schedule)   // schedule below
          broker.WireInstance(Private) ; marketData.WireInstance(Public, ModeOnDemand)
          Start(tickInterval)                                // continuous opens → agents trade
          StartSession(Mode=OnDemand)                        // the SEAL (PersistOnClose=false)
        → return { status:"warming_up", session_id, warmup_ends_at, session_ends_at }

  UI:
    request pending        → "preparing your simulation…" spinner
    on response            → own countdown to warmup_ends_at   (2-min book warm-up; agents only)
    at warmup_ends_at      → treat sim open → connect /v1/ws → run session timer to session_ends_at
    setup failure          → response is { status:"failed", reason }  → show error + retry

Schedule (2 boundaries), relative to T0 = now at construct:

continuous @ T0
closed     @ T0 + warmupLen + durationMinutes
warmup_ends_at   = T0 + warmupLen            // UI flips to "open" here
session_ends_at  = T0 + warmupLen + duration // = the closed boundary (user's duration ends)

The exchange is continuous the whole span; "warm-up" is just the first warmupLen of it (agents trade, user not admitted) — there is no warm-up phase in the exchange. Duration genuinely starts after warm-up because the closed boundary is warmup_ends_at + duration.

BRING_UP runs before construct so T0 can equal the Start instant and the warm-up window is tight. Safe for abort: NewExchange starts no goroutines, so a populate failure aborts before anything is built.

2. Decisions (locked)
#	Decision
OD.1	Trigger = POST /session/ondemand, held through setup, single return at warm-up-start with absolute warmup_ends_at/session_ends_at; UI self-times the warm-up. Revises HTTP_SURFACE H.2 (pull-based) — polling is no longer the happy path; the status endpoint (OD.12) is demoted to fallback.
OD.2	duration = integer minutes, value only, query param. Bounds: min 5, default 60, max 120; out-of-range → 400.
OD.3	Warm-up length = 2 min, configurable (market config ondemand.warmup).
OD.4	Schedule as §1: continuous@T0, closed@T0+warmup+duration; T0=now at construct.
OD.5	Reference = prior-close (reuse canonical readReference by market tag) — simplest, Tier 1 §2.4-sanctioned swappable; not "last-session open."
OD.6	MarketID = ExchangeID = "od:<sessionID>"; numShards = 1; mode = ModeOnDemand (throwaway population, candles not persisted).
OD.7	The seal = StartSession(Mode=lifecycle.OnDemand) inside the held setup → OndemandSessionLoad, PersistOnClose=false (the ephemeral non-persisting wallet; balance read at load).
OD.8	No SeedLiquidity for on-demand (Seeding S1.13 parked); book forms from agents + the warm-up window. Can be added later if the thin book bites.
OD.9	BRING_UP before construct; held setup bounded by setupTimeout (~90s, configurable); exceed → failed. If BRING_UP proves long/variable, fallback is to flip this one request to poll — not the default.
OD.10	Session gains Failed state + FailReason. Gate-fail (pre-spin-up) deletes the Starting session. Setup failure after Start → exch.Stop() + CacheRegistry.Remove + mark Session Failed (full teardown choreography = Tier 3).
OD.11	WS connect = reuse the canonical /v1/ws dispatcher (join doc §7). It resolves sessionID → Session.Market (od:…) → Market.fanout and delegates to marketdata.NewWSHandler. On-demand FanOut has one subscriber (private feed). UI connects at warmup_ends_at.
OD.12	GET /session/{id}/status retained only as fallback/recovery (lost held POST, re-verify, "my current session"). Not the happy path.
OD.13	HTTP codes: §4.4.
3. External contracts bound (exact; on-demand-specific)
go
// gate — shipped
func (b *funnel.Broker) QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error)
//   true iff trading_cash > OnDemandMinimum (strict; debt rejected). errors: store.ErrNoAccount, store.ErrLoadFailed.

// seal + install — shipped (Mode switches the loader)
func (b *funnel.Broker) StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error
//   cmd.Mode = lifecycle.OnDemand → OndemandSessionLoad (PersistOnClose=false, never writes Postgres)

// account + market-data — shipped (same as canonical, Mode flips)
func (b *funnel.Broker) EnsureAccount(ctx, lifecycle.OwnerHuman, ownerID string) (AccountID, error)
func (b *funnel.Broker) WireInstance(exchangeID string, private <-chan contract.EngineResult)
func (m *marketdata.MarketData) WireInstance(exchangeID, tape, market string, marketdata.ModeOnDemand) *marketdata.FanOut

const lifecycle.OnDemand   // SessionType
4. The trigger — POST /session/ondemand
4.1 Wire
POST /session/ondemand?market=IN&duration=35
  body: { "user": { "email": "...", "user_id": "..." } }     // identity packet (H.1; trusted, not validated)
  200 (gate pass):  { "status":"warming_up", "session_id":"...", "warmup_ends_at":"<ISO>", "session_ends_at":"<ISO>" }
  200 (gate fail):  { "status":"insufficient_funds", "message":"move money, then retry" }
  503 (setup fail): { "status":"failed", "reason":"..." }
  400 / 409:        { "error":"..." }
4.2 Handler + Conductor (pseudo; real contracts)
handleStartOnDemand(w, r):
    defer recoverPanic(w)
    if r.Method != POST → 405
    userID := body.user.user_id ; tag := query.market
    durMin := parseInt(query.duration)                       // OD.2
    if userID=="" || tag=="" → 400
    if durMin < 5 || durMin > 120 → 400 ; if durMin==0 → durMin=60   // default
    ctx := context.WithTimeout(r.Context(), setupTimeout)    // ~90s (OD.9); bounds the held setup
    res, err := conductor.StartOnDemand(ctx, UserID(userID), MarketTag(tag), durMin)
    map res/err → §4.4

Conductor.StartOnDemand(ctx, userID, tag, durMin) -> (Result, error):

    // reserve (locked, µs)
    reg.mu.Lock()
      if reg.byUser[userID] != nil { reg.mu.Unlock(); return _, ErrAlreadyInSession }   // XOR (strict: canonical OR on-demand)
      sid := mintSessionID()
      sess := &Session{ID: sid, User: userID, AccountType: Ephemeral, State: Starting}  // Market set after gate
      reg.sessions[sid] = sess ; reg.byUser[userID] = sess
    reg.mu.Unlock()

    // gate (off-lock)
    accountID, err := broker.EnsureAccount(ctx, lifecycle.OwnerHuman, string(userID))
    if err != nil { rollbackDelete(sid, userID); return _, infraErr(err) }
    sess.Account = accountID
    ok, err := broker.QualifyOnDemand(ctx, accountID)
    if err != nil { rollbackDelete(sid, userID); return _, infraErr(err) }
    if !ok { rollbackDelete(sid, userID); return InsufficientFunds{}, nil }   // gate NO → nothing spun up

    // held setup (gate YES) — bounded by ctx (setupTimeout)
    marketID := MarketID("od:" + string(sid))
    reg.mu.Lock()
      m := &Market{ID: marketID, Mode: OnDemand, Tag: tag, State: Provisioning, members: {sid:{}}}
      reg.markets[marketID] = m ; sess.Market = marketID
    reg.mu.Unlock()

    if err := agents.BringUp(tag, OnDemand, instrumentIDs(cfg)); err != nil {   // SYNC; before construct
        failOnDemand(sid, marketID, "populate_failed:"+err); return _, FailedErr }

    T0 := conductor.now()
    schedule := { continuous@T0, closed@ T0 + warmupLen(cfg) + minutes(durMin) }
    numShards := 1
    specs := buildInstrumentSpecs(cfg, referenceByPriorClose)      // reuse readReference (OD.5)
    exch := exch.NewExchange(numShards, schedule, specs)

    broker.WireInstance(string(marketID), exch.Private)
    fanout := market.WireInstance(ExchangeID(marketID), exch.Public, string(tag), marketdata.ModeOnDemand)
    exch.Start(tickInterval)

    participant := mintParticipantID()
    err = broker.StartSession(ctx, funnel.SessionStartCmd{
        AccountID: accountID, SessionID: funnel.SessionID(sid), Mode: lifecycle.OnDemand,
        ExchangeID: funnel.ExchangeID(marketID), ExchangeInbound: exch.Inbound, Participant: participant })
    if err != nil { failOnDemand(sid, marketID, "install_failed:"+err); return _, FailedErr }

    // finalize (locked, µs)
    reg.mu.Lock()
      m.exch, m.inbound, m.fanout = exch, exch.Inbound, fanout
      m.warmupEndsAt = T0 + warmupLen ; m.closeAt = T0 + warmupLen + minutes(durMin) ; m.State = Live
      sess.Participant = participant ; sess.State = Active
    reg.mu.Unlock()

    // After the Live flip + closeAt commit, spawn the per-session reaper
    // that fires the on-demand teardown when wall-clock reaches closeAt.
    // Pinned here because a reaper spawned before the commit would race
    // the lock-take that stamps closeAt onto the market record. The
    // reaper registers on conductor.wg so Conductor.Stop drains it on
    // shutdown. Full teardown shape is covered by the on-demand teardown
    // doc; this is only the spawn-point pin.
    spawnOnDemandReaper(marketID, m.closeAt)

    return WarmingUp{SessionID: sid, WarmupEndsAt: m.warmupEndsAt, SessionEndsAt: m.closeAt}, nil

rollbackDelete(sid, userID):                 // gate-fail / pre-setup: nothing built yet
    reg.mu.Lock(); delete(reg.sessions, sid); if reg.byUser[userID]==sess delete(reg.byUser, userID); reg.mu.Unlock()

failOnDemand(sid, marketID, reason):         // setup failure — keep Session=Failed for recovery/poll
    if the exchange was Started: exch.Stop()  // closes edges → settlement consumer + ingestor exit; ingestor CacheRegistry.Remove
    reg.mu.Lock()
      sess.State = Failed ; sess.FailReason = reason
      delete(reg.markets, marketID)           // the instance is gone
      // sess stays (Failed) so a fallback status read / recovery sees it; cleared by Tier-3 teardown/timeout
    reg.mu.Unlock()

THE RULE holds: the lock is taken only for the reserve, the Market-create, and the finalize — never across BRING_UP, construct, wire, Start, or StartSession.

4.3 Registry additions
go
// Session (join doc) gains:
Failed SessionState        // added to the enum
FailReason string          // set only when State==Failed
// Market (spin-up doc) gains for on-demand:
warmupEndsAt time.Time     // = T0 + warmupLen ; closeAt already = session_ends_at
4.4 HTTP status table
Condition	Status
gate pass (setup complete, warming)	200 {status:"warming_up", session_id, warmup_ends_at, session_ends_at}
gate fail (QualifyOnDemand false)	200 {status:"insufficient_funds", message}
XOR conflict (already in a session)	409
bad body / duration out of [5,120]	400
setup failure or setupTimeout exceeded (populate/construct/start/install)	503 {status:"failed", reason}
unexpected (ErrBadBinding/ErrBadParticipant = L7 bug; other)	500
5. Status endpoint (fallback / recovery) — GET /session/{id}/status

Pure registry read; used only if the held POST was lost mid-flight, to re-verify before opening, or as "what's my current session."

GET /session/{sessionID}/status
  200: { status, warmup_ends_at?, session_ends_at?, reason? }
       // Starting → "warming_up" (+ warmup/session ends if Market up); Active → "ready"(+ends); Failed → "failed"+reason
  404: unknown session
6. WS connect (part of this sequence — reuses the canonical dispatcher)

At warmup_ends_at the UI opens wss://…/v1/ws?session=<sessionID>. This hits the same /v1/ws dispatcher from the join doc §7 — no on-demand-specific code:

AuthFromRequest(r) → sessionID
fanoutForSession(sessionID):  sessions[sid](Active) → markets[sess.Market=="od:<sid>"](Live) → m.fanout
→ marketdata.NewWSHandler(fanout, wsCfg).ServeHTTP(w, r)

On-demand specifics: the resolved FanOut belongs to the od:<sessionID> instance and has exactly one subscriber (the private feed). Subscription (snapshot/candle frames) is the shipped in-band WS path (join doc §8) — L7 not involved. Goroutine/lock topology is identical to canonical (join doc §9): the request goroutine runs the dispatcher then market-data's readLoop; lock released before delegating.

7. Flagged / parked
Setup-hold length depends on BRING_UP (L2, undesigned). Bounded by setupTimeout (OD.9); exceed → failed. Fallback (flip to poll) only if BRING_UP proves long.
On-demand liquidity (OD.8) — no operator; revisit if the thin book is a problem.
Failure teardown after Start (OD.10) uses exch.Stop() + CacheRegistry.Remove; the full teardown/reap choreography (and clearing the Failed session + the on-leave reap) is Tier 3.
BRING_UP/SeedLiquidity signatures will change (noted in the spin-up doc) — same placeholder ports here.
Reconnect token (P6) — AuthFromRequest carrier is the sessionID on connect; the longer-lived token is P6, parked.
Instrument allowlist — SessionStartCmd has no field; the on-demand session installs permissive (full instrument set), same as canonical.
8. Out of scope

Leave/exit + reap (Tier 3); the account read endpoints; the subscribe/stream internals (shipped market-data); canonical spin-up/join (companion docs).