layer7/join — Canonical Join + WebSocket Establishment (L7-2)
<!-- Provisional path; orientation's call. READ ORDER: 1) /CLAUDE.md 2) layer7/CLAUDE.md 3) THIS file 4) docs/LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md -->
Spec & authority

LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md is the spec (decisions J.1–J.9). Needs a Live canonical market (L7-1). No broker prereq — every broker/marketdata seam it binds is shipped (verified: EnsureAccount, StartSession/SessionStartCmd, marketData.WSConfig(), marketdata.NewWSHandler). Code wins over doc.

What it IS (one line)

A human joins a live canonical market and gets live data: POST /session/canonical (reserve XOR → EnsureAccount → StartSession install → finalize → {session_id, participant_id, "active"}) and the GET /v1/ws dispatcher (resolve sessionID → Market → FanOut, delegate to marketdata.NewWSHandler). Subscribe + stream are the shipped in-band WS path — L7 has no code there.

Registry additions (per spec §4)

Session{ID, User, Market, Account, AccountType(Durable), Participant, State}; states already in the L7-0 enum. Grow BrokerPort with the shipped EnsureAccount/StartSession (assertion stays green — both exist).

Load-bearing rules
POST /session/canonical is fast + blocking (market already up). Flow (THE RULE, lock twice):
lock: findLiveCanonical(tag) (Tag,Mode==Canonical,State==Live) — nil → 409 ErrMarketNotLive (J.2, no wait); byUser[userID] != nil → 409 ErrAlreadyInSession (L7 XOR front-gate, J.4); mint sid (UUID, J.3); create Session{State:Starting}; sessions[sid]=sess, byUser[userID]=sess (the reservation is the Starting record); copy inbound, marketID off-lock. unlock.
off-lock: EnsureAccount(ctx, lifecycle.OwnerHuman, userID) (J.1) → on err rollback+map; mint participant (UUID); StartSession(SessionStartCmd{AccountID, SessionID, Mode:lifecycle.Canonical, ExchangeID:marketID, ExchangeInbound:inbound, Participant}) → on err rollback+map.
lock: set sess.Account/Participant, flip Active, add to markets[marketID].members (guard: market may be gone). unlock. Return.
rollback: delete the session + free byUser (if still ours); account is not rolled back (EnsureAccount idempotent+durable, A2.5).
Status table (J.9/§6.3): 200 active · 400 missing user_id/market · 409 ErrMarketNotLive / ErrAlreadyInSession / broker ErrSessionAlreadyLive (backstop, also rolled back) · 504 ErrBrokerTimeout · 503 lifecycle load/snapshot fail · 500 EnsureAccount other + ErrBadBinding/ ErrBadParticipant (L7 fills both → seeing them = L7 bug).
GET /v1/ws dispatcher — L7's ONLY WS code: sid := AuthFromRequest(r) (reads ?session=, J.6) — "" → 401 before Upgrade; fanout := fanoutForSession(sid) — nil → 404 before Upgrade; else marketdata.NewWSHandler(fanout, conductor.wsCfg).ServeHTTP(w, r) (delegate; blocks for the connection's life). Both guards MUST precede ServeHTTP — once gorilla Upgrades, L7 can't write a status.
fanoutForSession (Model A, J.5): lock → sessions[sid] (must be Active) → markets[sess.Market] (must be Live) → f := m.fanout → unlock (J.7 — release BEFORE delegating) → return f. No separate bindings map; derived from the registry.
conductor.wsCfg = marketData.WSConfig() captured at construction.
Deps & seam

L7-0 scaffold + L7-1's Market (fanout/inbound). Shipped: funnel.EnsureAccount/StartSession/ SessionStartCmd, lifecycle.OwnerHuman/Canonical, marketdata.NewWSHandler/WSConfig. Subscribe/ stream internals are shipped market-data — do not reimplement (spec §8).

Do NOT
hold the registry mutex across EnsureAccount/StartSession or across ServeHTTP (J.7 — a connection blocks its request goroutine for life; holding the lock freezes the registry);
write any HTTP status after ServeHTTP (guards go before Upgrade);
build a subscribe endpoint or any subscribe/stream logic (shipped, in-band);
roll back the account on failure (only the session + byUser).
Flags (orientation)
SessionStartCmd has no Instruments field — installed sessions are permissive (P5-D9 parked); L7 passes nothing for it.
P6 reconnect token is parked — base connect carrier is sessionID on ?session= (J.6); AuthFromRequest is the seam. (The idempotent-rejoin refinement is L7-5, not here.)
ErrSessionAlreadyLive shouldn't occur if L7's byUser held; if it does (stale broker), roll back + 409.
Tests (objective; no goldens/auditor)
Join happy path (fake broker port): reserve→ensure→start→finalize; returns active + ids; member added.
XOR front-gate (second join same user → 409); ErrMarketNotLive (no Live market → 409); rollback frees byUser + deletes session on StartSession error; the full status table incl. 504/503/500 mapping.
Dispatcher: "" session → 401 (no Upgrade); unknown/non-Active session → 404 (no Upgrade); happy → delegates to a fake WS handler; fanoutForSession releases the lock before returning (structural).
-race on concurrent join + dispatcher resolve.