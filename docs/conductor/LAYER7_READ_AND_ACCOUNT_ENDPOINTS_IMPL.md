Layer 7 — Read & Account Endpoints: Implementation Design

What this doc is. The implementation design for L7's remaining non-session-lifecycle endpoints — everything decided after the on-demand spin-up flow. Three implementable endpoints, plus a record of two parked ones:

Market catalog + canonical session window (P9 + P10, collapsed) — GET /v1/markets.
Instruments in a market (P8, list-only) — GET /v1/instruments?market=<tag>.
Account opening (P7) — POST /v1/account/open.
Parked: session-status read (P3), identity-on-reconnect (P6) — §7, rationale kept so they are not silently lost.

What it is NOT. It does not cover session create/join/on-demand (the three companion flow docs), and it does not cover exit / teardown / reap (P4 → Tier 3), which is explicitly held off (§9).

Authority & contracts. Typed against shipped code: simulator/broker/funnel, simulator/broker/reads, simulator/broker/bringup, simulator/economy/lifecycle, simulator/contract. Builds on and reuses, does not re-derive:

claude/LAYER7_CANONICAL_SPINUP_IMPL.md — MarketConfig/SessionBoundaryConfig/ InstrumentConfig, listConfiguredMarkets(), loadConfig(tag), atWallClock, isWeekend, the Market/Registry structs, THE RULE.
claude/LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md — findLiveCanonical(tag), the shared HTTP plumbing (recoverPanic/writeJSON/method guard), mux registration on bringup.Runtime.Mux, BrokerPort (already carries EnsureAccount).
LAYER7_TIER2_HTTP_SURFACE.md — H.1 (no auth; trusted identity packet), H.8 (L7 returns session identity, never market data).
LAYER7_TIER0_REGISTRY_AND_IDENTITY.md — §7 EnsureAccount seam, the money-blind principle (L7 may not read the money tables, even to check).

Repo layout intentionally unspecified — package names, file paths, module path are the implementer's call. Only external packages are named by real import path; L7's own types are shown as Go without a path. The literal path strings (/v1/...) are illustrative of the locked semantics, not separately ratified naming (HTTP-surface doc note); they follow the shipped broker's ?query= convention for consistency.

1. The frame (what these endpoints are, and the two walls they respect)

All three endpoints are read-shaped doors the frontend hits before/around a session, not part of any session lifecycle. They share two load-bearing boundaries established earlier, and every design choice below falls out of them:

H.8 — L7 returns session identity, never market data. The market catalog and instrument list are session/market metadata (config + registry liveness), squarely L7's. Prices (LTP, quote, depth) are market data → never served by L7; they stream over the shipped market-data WS (join doc §8). This is why P8 is list-only.
Money-blind (Tier-0 §7) — L7 may not read the money tables. account_open opens or fetches the account and returns only the account_id handle. Balance / trades / P&L are the broker's shipped /v1/account/* reads, which the frontend calls directly with that handle. L7 never proxies or aggregates them.
Endpoint	Owns / source	Blocks?	Touches registry?
GET /v1/markets (P9+P10)	config catalog + schedule; registry liveness	read	reads (liveness scan), no mutation
GET /v1/instruments?market= (P8)	config instrument list only	read	no
POST /v1/account/open (P7)	broker.EnsureAccount → account_id	fast (sync broker call)	no

None of the three mutate L7 state; none take the registry mutex for a write. The catalog only reads the registry (a brief lock for the liveness scan). This keeps them off THE RULE's critical path entirely.

2. Shared HTTP plumbing (reused, not re-invented)

All three mount on the shared bringup.Runtime.Mux (same mux the broker registered its /v1/account/* reads + /v1/candles on; L7 adds its routes there — join doc prerequisite). No path collisions: these are new exact patterns.

Each handler opens with the same two guards the shipped broker handlers use (broker/reads/common.go, broker/marketdata/http.go):

defer recoverPanic(w, path) — a panic on the request goroutine becomes a clean 500.
method guard — non-matching method → 405 with Allow.
writeJSON(w, status, payload, path) / writeError(w, status, msg, path) — the plain { "...": ... } / { "error": "..." } envelopes.

Routes added by L7:

GET  /v1/markets                      → handleMarkets        (P9 + P10)
GET  /v1/instruments?market=<tag>     → handleInstruments    (P8)
POST /v1/account/open                 → handleAccountOpen    (P7)

Conductor dependencies these use (all already present from the companion docs): reg *Registry, cfgDir + loadConfig/listConfiguredMarkets, broker BrokerPort (carries EnsureAccount), now func() time.Time, log.

3. P9 + P10 — Market catalog & canonical session window
3.1 What the UI needs (the three infos, one call)

One endpoint answers the pre-join screen's three questions:

Which markets are supported — the configured catalog (IN, US, …).
Which are active right now — a canonical market instance is Live in the registry.
The canonical session window + duration — open/close times and length.

Source split is clean and deliberate: config answers #1 and #3 (it is the authority for the catalog and the schedule — the same MarketConfig the spin-up timer reads), the registry answers #2 (the only thing that knows what is actually up). No prices → no H.8 tension; this is pure session/market metadata.

3.2 Flow (detailed)
GET /v1/markets
  1. tags := listConfiguredMarkets()                    // reuse spin-up's config source of truth
  2. for each tag:
       cfg          := loadConfig(tag)                  // config: catalog + schedule
       active       := findLiveCanonical(tag) != nil    // registry: Tag==tag && Mode==Canonical && State==Live
       open, close  := nextSessionWindow(cfg, now())    // config-derived current-or-next window (Option A)
       emit { tag, active, session:{opens_at:open, closes_at:close,
                                    duration: close-open, wall_clock: fmt(cfg)} }
  3. 200 { markets: [ ...one per configured tag... ] }
Array membership == "supported" — every entry is a configured market, so there is no separate supported:true field (it would be trivially true on every row). The list is info #1.
active is the only field read from the registry. The scan excludes on-demand instances automatically because findLiveCanonical filters Mode==Canonical (the private od:<sessionID> instances are Mode==OnDemand — you do not "join" someone else's private market, so they never appear here).
Liveness and window are independent. active says "is it up now"; the window says "when is its session." A market can be active:false with a window pointing at today (pre-open) or at the next weekday (post-close) — both are correct and the UI renders "opens in …" / "join now" off the pair.
3.3 The window computation — Option A (current-or-next), config-only

nextSessionWindow derives the window purely from config boundaries; it needs no registry read, so liveness and window stay orthogonal. For a Live market it returns today's window (whose close equals the retained Market.closeAt by construction, since both anchor the same config boundary to the same day); for a closed market it rolls forward to the next weekday it will run.

nextSessionWindow(cfg, now) -> (open, close time.Time):
    loc       := LoadLocation(cfg.Timezone)
    openHHMM  := cfg.Session[0].At                 // first boundary = continuous-open
    closeHHMM := cfg.Session[len-1].At             // terminal boundary = PhaseClosed
    for dayOffset := 0, 1, 2, ... :
        day := now.In(loc) advanced by dayOffset calendar days
        if isWeekend(day): continue                // weekdays only — no holiday calendar yet
        open  := atWallClock(day, openHHMM, loc)
        close := atWallClock(day, closeHHMM, loc)
        if dayOffset == 0 && !close.After(now): continue   // today's session already ended → next weekday
        return open, close                         // dayOffset 0 → today's (live or pre-open); else → next

Reuses the exact LoadLocation/atWallClock/isWeekend helpers and weekday logic from the spin-up timer (nextCanonicalFireTime), but anchors to the open boundary itself, not open − preopen_lead — the lead is a spin-up-timing concern, invisible to the user-facing window.

wall_clock is a display string built from the config HH:MM + the zone, e.g. the open instant's Zone() abbrev → "09:15–15:30 IST". duration is close − open rendered as a Go duration string ("6h15m"); the UI may also just subtract the two instants.

3.4 Handler (pseudo; real contracts)
handleMarkets(w, r):
    defer recoverPanic(w, "/v1/markets")
    if r.Method != GET → 405 (Allow: GET)
    out := []MarketEntry{}
    for tag in listConfiguredMarkets():
        cfg, err := loadConfig(tag)
        if err != nil { log.Warn("skip market, bad config", tag, err); continue }  // one bad config ≠ whole endpoint down
        active := conductor.isCanonicalLive(tag)                 // reg.mu briefly; findLiveCanonical(tag) != nil
        open, close, ok := nextSessionWindow(cfg, now())
        if !ok { log.Warn("skip market, no session window", tag); continue }
        out = append(out, MarketEntry{
            Tag:    string(tag),
            Active: active,
            Session: SessionWindow{
                OpensAt:   open.UTC().Format(RFC3339),
                ClosesAt:  close.UTC().Format(RFC3339),
                Duration:  (close-open).String(),
                WallClock: wallClockString(cfg, open),
            },
        })
    writeJSON(w, 200, map{ "markets": out }, "/v1/markets")

isCanonicalLive(tag) -> bool:                               // THE RULE: lock only for the scan, no call-out inside
    reg.mu.Lock(); defer reg.mu.Unlock()
    return findLiveCanonical(tag) != nil                    // reuse join doc's scan (Tag,Mode==Canonical,State==Live)
3.5 Wire shape
GET /v1/markets
  200: { "markets": [
    { "tag":"IN", "active":true,
      "session": { "opens_at":"2026-10-12T03:45:00Z", "closes_at":"2026-10-12T10:00:00Z",
                   "duration":"6h15m", "wall_clock":"09:15–15:30 IST" } },
    { "tag":"US", "active":false,
      "session": { "opens_at":"...", "closes_at":"...", "duration":"6h30m", "wall_clock":"09:30–16:00 EDT" } }
  ]}
3.6 Status table
Condition	Status
success (zero or more markets)	200 {markets:[...]}
method not GET	405
a single market's config/window is bad	not an error — that market is skipped + logged; the rest return 200
panic	500 (recoverPanic)

No 4xx for "no markets" — an empty catalog is a valid 200 {markets:[]}.

4. P8 — Instruments in a market (list-only)
4.1 What it is, and why no price

Returns the static instrument list for a market, from config — IDs and trade-time static metadata. No LTP, no quote, no depth. Those are market data (H.8) and reach the UI only over the shipped WS snapshot stream (subscribe per instrument → seed → live updates; join doc §8). Grounded in the shipped broker/marketdata:

the MarketSnapshot is tape-learned (snapshot.go: "L7 provides no instrument list") — market-data does not even hold the config list;
LastPrice returns (0,false) until an instrument's first trade this session — so a never-yet-traded instrument legitimately has no LTP;
there is no REST price endpoint anywhere — only /candles (history) and the WS.

So L7 is the right owner of the list, and the list is the only thing it serves. The UI renders a fresh/just-opened instrument with a dash (or the config reference) until the first trade seeds a price over the WS.

4.2 Flow + handler

Config read, no registry, no liveness requirement — the list is static, so it is available even when the market is closed (a "what trades here" preview works pre-open).

handleInstruments(w, r):
    defer recoverPanic(w, "/v1/instruments")
    if r.Method != GET → 405 (Allow: GET)
    tag := r.query["market"]
    if tag == "" → 400 "market required"
    cfg, err := loadConfig(tag)
    if err == ErrUnknownMarket → 404 "unknown market"
    if err != nil → 500
    out := for each inst in cfg.Instruments:
        { "id": inst.ID, "tick_size": inst.TickSize }      // core list-only fields
    writeJSON(w, 200, map{ "market": tag, "instruments": out }, "/v1/instruments")
4.3 Wire shape
GET /v1/instruments?market=IN
  200: { "market":"IN", "instruments":[ { "id":"ACME", "tick_size":5 }, { "id":"BETA", "tick_size":10 } ] }
4.4 Field scope (locked core + optional-from-config)
Core (locked): id, tick_size — a trading UI needs the tick to render order-entry increments; both are pure config.
Optional (config, includable if the UI asks, still not market data): band_bps (circuit band) and seed_price (the config seed — a static reference, explicitly not LTP; usable as the pre-first-trade display fallback). Not included by default to keep the payload strictly list-shaped; add on request.
Never: last_price / quote / depth — WS only.
4.5 Status table
Condition	Status
success	200 {market, instruments:[...]}
missing market query param	400
unknown market tag (no config)	404
method not GET	405
config load infra error	500
5. P7 — Account opening
5.1 What it is, and the flow it serves

The entry handshake for signup and login. The frontend's auth layer (above L7, trusted — H.1) authenticates the user, then calls this to materialize/fetch the durable account:

Signup → EnsureAccount creates the account (broker applies starting capital — it owns that policy; L7 passes no amount → stays money-blind), returns the account_id. Balance exists immediately (the starting cash); trades/P&L are empty.
Login → EnsureAccount hits the "already exists" path, returns the same account_id.

Both are the same idempotent call — there is no account-layer signup/login distinction; that distinction lives in the auth layer, which already knows which button the user pressed. After the handle comes back, the frontend fetches balance/trades/P&L from the broker's shipped reads directly (/v1/account/balance, /v1/account/trades, /v1/account/pnl, keyed by account_id) — not from L7. The single "loading" spinner the user sees covers account_open + those reads happening back-to-back (pure UI).

5.2 Boundary (what it does not touch)
No registry, no lock, no XOR — opening an account is not starting a session; nothing in L7's in-memory registry changes. (Contrast join/start, which reserve a Session.)
No market param — an account is market-independent (durable, keyed by owner).
Money-blind — returns the handle only; never balance/P&L.
5.3 Contract bound (shipped broker call)
go
func (b *funnel.Broker) EnsureAccount(ctx context.Context,
        ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
//   get-or-create, atomic (leans on L5 uq_accounts_owner as the sole race arbiter);
//   idempotent; starting-capital policy inside the broker.
//   errors: lifecycle.ErrBadOwnerType, ErrBadOwnerID, ErrNegativeStarting*, ErrCreateFailed,
//           store.ErrNoAccount (get-or-create retry exhausted — NOT "user has no account").
const lifecycle.OwnerHuman   // owner type for a human (J.1 precedent; OwnerAgent is the other)
5.4 Handler + Conductor (pseudo)
handleAccountOpen(w, r):
    defer recoverPanic(w, "/v1/account/open")
    if r.Method != POST → 405 (Allow: POST)
    body := decodeJSON(r)                                    // { user:{ email, user_id } }
    if body.user.user_id == "" → 400 "user_id required"
    ctx := context.WithTimeout(r.Context(), accountOpenTimeout)   // handler owns the deadline
    accountID, err := conductor.OpenAccount(ctx, UserID(body.user.user_id))
    if err != nil → map per §5.6
    writeJSON(w, 200, map{ "account_id": string(accountID) }, "/v1/account/open")

Conductor.OpenAccount(ctx, userID) -> (AccountID, error):
    return broker.EnsureAccount(ctx, lifecycle.OwnerHuman, string(userID))   // whole body; pass-through

OpenAccount is a straight pass-through — nothing to roll back (EnsureAccount is idempotent

durable, so a client retry is always safe). email is attribution only (H.1 — trusted, never validated); user_id is the key → ownerID.
5.5 Wire shape
POST /v1/account/open
  body: { "user": { "email":"...", "user_id":"..." } }
  200:  { "account_id":"..." }          // 200 for BOTH signup and login (no create-vs-fetch distinction surfaced → not 201)
  err:  { "error":"<reason>" }
5.6 Status table
Condition	Status
success (created or already existed)	200 {account_id}
missing/empty user_id, undecodable body	400
lifecycle.ErrBadOwnerID (bad id bubbled past edge check)	400
store.ErrNoAccount (get-or-create retry exhausted — infra/race)	503
lifecycle.ErrCreateFailed (account create failed — persistence)	503
ctx deadline exceeded	504
lifecycle.ErrBadOwnerType / ErrNegativeStarting* (L7 always sends OwnerHuman; starting-capital is server config → L7 bug / misconfig)	500
method not POST	405
any other	500
6. (reserved)
7. Parked endpoints (recorded, not designed)
7.1 P3 — session-status read GET /session/{sessionID}/status — PARKED

Born as the on-demand readiness poll target (HTTP-surface H.2/H.3: on-demand returned preparing, the UI polled until ready/failed). The on-demand doc's held-setup + UI-self-timed model (OD.1) removed that: the POST now holds through setup and returns once with absolute warmup_ends_at/session_ends_at, so the UI never polls for readiness. The only leftover uses are "what's my current session" and recovery after a lost POST — both of which need a by-user lookup (the client lacks the sessionID), which is really P6 territory, not a sessionID-keyed path. No concrete consumer survives → parked. Trivial to spec later (pure registry read) if a use case returns.

7.2 P6 — identity-on-reconnect — PARKED (pre-existing)

A reconnecting/refreshed browser presenting a longer-lived token so L7 re-finds its membership by user and re-binds. AuthFromRequest (join doc §7) is the seam; the base WS connect carries the sessionID directly (J.6). Flagged since Tier 0 §9; out for the prototype (no auth layer). Would also be the natural home for the "find my session from my identity packet" read that P3's leftover use cases wanted.

7.3 P7-adjacent — strict-login & created flag — PARKED

If login should ever 404 an unknown user instead of auto-creating, that needs a pure get-account broker seam (new work), since EnsureAccount always creates. And if a future auth layer wants L7 to report new-vs-existing, EnsureAccount would need to surface a created bool (signature change). Both deferred — in the trusted-identity prototype, EnsureAccount-for-both is correct and the caller already knows which it did.

8. Consolidated decision log
#	Decision	Status
MC.1	GET /v1/markets answers all three infos in one call: config → catalog (#1) + session window (#3); registry → active (#2). No prices → no H.8 tension.	LOCKED
MC.2	Market list source = spin-up's listConfiguredMarkets()/loadConfig — the same config the timer reads; no second catalog.	LOCKED
MC.3	active = findLiveCanonical(tag) != nil (Tag,Mode==Canonical,State==Live); on-demand od: instances excluded (filtered by Mode==Canonical).	LOCKED
MC.4	Session window = Option A, current-or-next via nextSessionWindow (config-only; anchors the open boundary, not open−lead; weekday roll; live market → today's, closed → next weekday).	LOCKED
MC.5	Window is config-derived and independent of liveness; closes_at of a live market equals retained Market.closeAt by construction.	LOCKED
MC.6	Array membership == "supported"; no redundant supported field. Payload per market = {tag, active, session:{opens_at, closes_at, duration, wall_clock}}.	LOCKED
MC.7	One bad market config/window is skipped + logged, not a whole-endpoint error; empty catalog = 200 {markets:[]}.	LOCKED
IN.1	GET /v1/instruments?market=<tag> = list-only, from MarketConfig.Instruments; never LTP/quote/depth (H.8 — those stream over the shipped WS).	LOCKED
IN.2	Core fields {id, tick_size}; band_bps/seed_price optional-from-config (static, not LTP); last_price never.	LOCKED
IN.3	Works regardless of live state (static config read); unknown tag → 404.	LOCKED
AO.1	account_open = sole call broker.EnsureAccount(OwnerHuman, user_id) → returns account_id; money-blind, session-less, registry/XOR untouched.	LOCKED
AO.2	Signup & login both hit this one idempotent endpoint; no create-vs-fetch surfaced → 200 for both (not 201).	LOCKED
AO.3	Account info (balance/trades/pnl) is not here — frontend calls the broker's shipped /v1/account/{balance,trades,pnl} directly with the returned account_id.	LOCKED
AO.4	Status map: 400 client input · 503 infra (retryable) · 504 timeout · 500 L7 bug/misconfig.	LOCKED
GEN.1	All three mount on the shared bringup.Runtime.Mux; reuse the shipped recoverPanic/method-guard/writeJSON plumbing; path strings illustrative (HTTP-surface note).	LOCKED
P3	Session-status read — PARKED (held-setup model removed the poll use case; residual needs are by-user = P6).	PARKED
P6	Identity-on-reconnect — PARKED (no auth layer in prototype).	PARKED
9. Out of scope / next
Exit + teardown + reap (P4 → Tier 3) — user-initiated leave (canonical: drop from a still-running market; on-demand: tear the whole instance down), scheduled canonical close, on-demand close at session_ends_at, broker unbind, exch.Stop(), CacheRegistry.Remove, fanout/registry cleanup, XOR release, and the Stopping/Ended session states (+ the Failed on-demand record cleanup). Held off deliberately; the session-end half of everything the companion docs build.
Holiday calendar — nextSessionWindow is weekdays-only, same caveat as the spin-up timer; later config.
Reference as a display price — P8 can optionally expose seed_price as a static pre-first-trade fallback; the live price is always WS.

End. Companion docs: canonical spin-up, canonical join + WS, on-demand spin-up + WS. Remaining L7 flow: Tier 3 exit/teardown/reap.