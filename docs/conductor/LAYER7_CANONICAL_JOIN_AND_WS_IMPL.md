Layer 7 — Canonical Join + WebSocket Establishment: Implementation Design

What this doc is. The implementation design for a human joining a live canonical market and then receiving live data: the POST /session/canonical endpoint, the /v1/ws connection establishment, and (for completeness, marked shipped) how the browser's subscribe reaches the data. Companion to claude/LAYER7_CANONICAL_SPINUP_IMPL.md (which produced the Live market this consumes).

Prerequisite. A canonical market for the tag is already Live (spin-up doc). The broker is bootstrapped and running. L7 mounts its routes on the shared HTTP mux that bringup exposes (bringup.Runtime.Mux).

Authority & contracts. Typed against shipped code: simulator/broker/funnel, simulator/broker/marketdata, simulator/economy/lifecycle, simulator/contract. Builds on LAYER7_TIER2_ADMISSION.md (§2 canonical admit, lock pattern, rollback), LAYER7_TIER2_HTTP_SURFACE.md (§2.1 endpoint, principles), LAYER7_TIER0_REGISTRY_AND_IDENTITY.md (registry, XOR, ParticipantID), and the locked live-data flow (Phases C–F). Repo layout is intentionally unspecified; external packages are named by real import path, L7's own types are shown as Go without an import path.

1. The arc (end to end)
Join — browser POST /session/canonical {user, market}. L7 resolves the live market, reserves the session (XOR), ensures the account, calls broker.StartSession to install it on the owning shard, finalizes, and returns {sessionID, participantID, "active"}. This is fast + blocking (the market is already up).
Connect — browser opens wss://…/v1/ws?session=<sessionID>. L7's dispatcher resolves sessionID → market → FanOut, then hands the socket to the shipped marketdata.NewWSHandler. The connection is now live but silent (one connected frame + keepalives).
Subscribe — browser sends a subscribe JSON frame on that same socket when the user opens an instrument. This is processed entirely by the shipped market-data handler; L7 is not involved.
Stream — the ingestor publishes matching tape events to the FanOut → the connection's write loop → the browser.

The money axis (orders/fills, keyed by AccountID) and the market-data axis (keyed by ExchangeID+Instrument) stay separate; join wires the session into both.

2. Decisions (locked this session)
#	Decision
J.1	EnsureAccount owner type for a human join = lifecycle.OwnerHuman; ownerID = the packet's user_id.
J.2	If no Live canonical market exists for the tag → reject immediately (no wait), HTTP 409.
J.3	SessionID and ParticipantID are opaque UUIDs (ParticipantID ≠ AccountID/BrokerID, per T0.8).
J.4	Layered XOR kept: L7's byUser is the cheap pre-I/O front-gate (per-user); the broker's handleInstallSession per-account check is the authoritative backstop. Both stay.
J.5	Market-data binding = Model A: L7 resolves sessionID → Session.Market → Market.fanout at WS-connect time. No separate bindings map — derived from the registry (supersedes the attachment's flat-map sketch; equivalent, one source of truth).
J.6	WS connect carrier = the sessionID presented directly on the connect (?session=<sessionID> query param, or header). AuthFromRequest reads it. P6 (identity-on-reconnect token) only hardens the reconnect case later; the base connect is this.
J.7	The dispatcher releases the registry lock before delegating to ServeHTTP (which blocks for the whole connection life).
J.8	Subscription is in-band on the WS, processed by shipped market-data (readLoop→handleSubscribe→FanOut.Subscribe). No L7 subscribe endpoint, no L7 subscribe logic.
J.9	HTTP status table for /session/canonical: §6.3.
3. External contracts bound (exact, shipped)

Broker (simulator/broker/funnel, with simulator/economy/lifecycle):

go
func (b *funnel.Broker) EnsureAccount(ctx context.Context,
        ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
//   errors: lifecycle.ErrBadOwnerType / ErrBadOwnerID / ErrNegativeStarting* / ErrCreateFailed,
//           store.ErrNoAccount (retry-exhausted). Never returns "not found" as normal.

func (b *funnel.Broker) StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error
type funnel.SessionStartCmd struct {
    AccountID       funnel.AccountID
    SessionID       funnel.SessionID
    Mode            lifecycle.SessionType           // lifecycle.Canonical here
    ExchangeID      funnel.ExchangeID               // = our MarketID string
    ExchangeInbound chan<- contract.InboundMessage  // = Market.inbound (retained at spin-up)
    Participant     contract.ParticipantID
}
//   errors: funnel.ErrBadBinding (nil inbound), ErrBadParticipant (empty participant),
//           lifecycle.ErrNoAccount / ErrLoadFailed / ErrSnapshotWriteFailed,
//           funnel.ErrSessionAlreadyLive (broker per-account XOR), funnel.ErrBrokerTimeout.

const lifecycle.OwnerHuman   // owner type for a human (OwnerAgent is the other)
const lifecycle.Canonical    // SessionType

SessionStartCmd has no Instruments field — the per-session instrument allowlist (P5-D9) isn't on this seam yet, so the installed session defaults to permissive (every instrument tradeable). Inherited/parked; L7 passes nothing for it.

Market-data (simulator/broker/marketdata) — shipped, used at WS establishment:

go
func marketdata.NewWSHandler(fanOut *marketdata.FanOut, cfg marketdata.WSConfig) http.Handler
func (m *marketdata.MarketData) WSConfig() marketdata.WSConfig
4. Registry additions for join

(Market already defined in the spin-up doc; fanout retained there.)

go
type Session struct {
    ID          SessionID
    User        UserID
    Market      MarketID        // → resolves to the market & its FanOut
    Account     AccountID
    AccountType AccountType     // Durable for canonical
    Participant ParticipantID
    State       SessionState    // Starting → Active → Stopping → Ended
}
const ( Starting SessionState = iota; Active; Stopping; Ended )
const ( Durable AccountType = iota; Ephemeral )

// Registry already holds: markets map[MarketID]*Market, sessions map[SessionID]*Session,
// byUser map[UserID]*Session, under one mutex.
5. Conductor additions
go
// BrokerPort extended (spin-up gave it WireInstance):
type BrokerPort interface {
    WireInstance(exchangeID string, private <-chan contract.EngineResult)              // spin-up
    EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (AccountID, error)
    StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error
}
// Conductor already holds: reg, wsCfg (= marketData.WSConfig()), log, now.
// L7 registers these routes on bringup.Runtime.Mux:
//   POST /session/canonical   → handleJoinCanonical
//   GET  /v1/ws               → handleWS
6. Endpoint — POST /session/canonical
6.1 Wire shapes
POST /session/canonical
  body: { "user": { "email": "...", "user_id": "..." }, "market": "IN" }
  200:  { "session_id": "...", "participant_id": "...", "status": "active" }
  err:  { "error": "<reason>" }           // non-2xx

user_id is the identity key (email is attribution only, per HTTP H.1 — L7 trusts, never validates).

6.2 Handler + Conductor (pseudo; real contracts)
handleJoinCanonical(w, r):
    defer recoverPanic(w)
    if r.Method != POST → 405
    body := decodeJSON(r)
    if body.user.user_id == "" || body.market == "" → 400 "user_id and market required"
    ctx := context.WithTimeout(r.Context(), joinTimeout)   // handler owns the deadline
    res, err := conductor.JoinCanonical(ctx, UserID(body.user.user_id), MarketTag(body.market))
    switch { map err → status per §6.3 }
    on success → 200 { session_id: res.SessionID, participant_id: res.ParticipantID, status: "active" }

Conductor.JoinCanonical(ctx, userID, tag) -> (JoinResult, error):

    // ---- reserve (locked, µs) ----
    reg.mu.Lock()
      m := findLiveCanonical(tag)                 // Tag==tag && Mode==Canonical && State==Live
      if m == nil { reg.mu.Unlock(); return _, ErrMarketNotLive }        // J.2
      if reg.byUser[userID] != nil { reg.mu.Unlock(); return _, ErrAlreadyInSession }   // L7 XOR front-gate (J.4)
      sid := mintSessionID()                      // UUID (J.3)
      sess := &Session{ID: sid, User: userID, Market: m.ID, AccountType: Durable, State: Starting}
      reg.sessions[sid] = sess
      reg.byUser[userID] = sess                   // the reservation IS this Starting record (A2.3)
      inbound, marketID := m.inbound, m.ID        // copy handles for off-lock use
    reg.mu.Unlock()

    // ---- off-lock slow work (THE RULE: no lock across these calls) ----
    accountID, err := broker.EnsureAccount(ctx, lifecycle.OwnerHuman, string(userID))   // J.1
    if err != nil { rollback(sid, userID); return _, ensureErr(err) }

    participant := mintParticipantID()            // UUID (J.3)
    err = broker.StartSession(ctx, funnel.SessionStartCmd{
        AccountID:       accountID,
        SessionID:       funnel.SessionID(sid),
        Mode:            lifecycle.Canonical,
        ExchangeID:      funnel.ExchangeID(marketID),
        ExchangeInbound: inbound,
        Participant:     contract.ParticipantID(participant),
    })
    if err != nil { rollback(sid, userID); return _, startErr(err) }   // incl. broker ErrSessionAlreadyLive backstop

    // ---- finalize (locked, µs) ----
    reg.mu.Lock()
      sess.Account, sess.Participant, sess.State = accountID, participant, Active
      if m2 := reg.markets[marketID]; m2 != nil { m2.members[sid] = struct{}{} }  // guard: market may be gone
    reg.mu.Unlock()
    return JoinResult{SessionID: sid, ParticipantID: participant}, nil

rollback(sid, userID):                            // undo only what L7 created (A2.5)
    reg.mu.Lock()
      delete(reg.sessions, sid)
      if reg.byUser[userID] == sess-for-sid { delete(reg.byUser, userID) }   // frees the XOR slot
    reg.mu.Unlock()
    // account is NOT rolled back (EnsureAccount is idempotent + durable)
6.3 Status table (J.9 — confirmed)
Condition	Status
success	200 active
missing user_id/market	400
no Live canonical market for tag (ErrMarketNotLive)	409
user already has a live session (L7 ErrAlreadyInSession)	409
broker ErrSessionAlreadyLive (per-account backstop; also rolled back)	409
StartSession ErrBrokerTimeout	504
StartSession lifecycle load/snapshot fail (ErrNoAccount/ErrLoadFailed/ErrSnapshotWriteFailed)	503
EnsureAccount failure / any other (incl. ErrBadBinding/ErrBadParticipant = L7 bug)	500

ErrBadBinding/ErrBadParticipant map to 500 because L7 fills both fields — seeing them means an L7 bug, not a client error.

6.4 Helpers
findLiveCanonical(tag): scan reg.markets for Tag==tag && Mode==Canonical && State==Live (one per tag). O(markets); cold path.
mintSessionID(), mintParticipantID(): new UUID string each.
7. WebSocket establishment — GET /v1/ws
7.1 Dispatcher (L7's only WS code)
GET /v1/ws?session=<sessionID>           // carrier pinned (J.6); header is an equivalent option

handleWS(w, r):
    defer recoverPanic(w)                 // only fires on the reject paths; after delegate, gorilla owns the response
    sid := AuthFromRequest(r)             // reads ?session= → SessionID ; "" if absent   (P6 hardens the token later)
    if sid == "" → 401 "no session"; return            // MUST be before Upgrade (can't write status after)
    fanout := conductor.fanoutForSession(sid)
    if fanout == nil → 404 "no live session"; return   // MUST be before Upgrade
    marketdata.NewWSHandler(fanout, conductor.wsCfg).ServeHTTP(w, r)   // delegate; blocks for the connection's life

fanoutForSession(sid) -> *marketdata.FanOut:
    reg.mu.Lock()
      sess := reg.sessions[sid]
      if sess == nil || sess.State != Active { reg.mu.Unlock(); return nil }
      m := reg.markets[sess.Market]
      if m == nil || m.State != Live { reg.mu.Unlock(); return nil }
      f := m.fanout                        // Model A derive: sessionID → Market → fanout (J.5)
    reg.mu.Unlock()                        // released BEFORE the caller delegates (J.7)
    return f

The two guards (401/404) are written before ServeHTTP, because once ServeHTTP runs, gorilla's Upgrade hijacks the connection and L7 can no longer write an HTTP status.

7.2 What the shipped handler then does (NOT L7 — do not reimplement)

marketdata.wsHandler.ServeHTTP: upgrader.Upgrade (HTTP 101) → NewConn → set pong/read-deadline → go writeLoop(conn) (first frame it sends: {"type":"connected","market","timezone"}) → run readLoop(conn) on the current goroutine → block on writeLoopDone. Connection is established once Upgrade succeeds and both loops run; then it's silent until a subscribe.

8. UI → server subscription (SHIPPED; L7 not involved — shown for the full picture)

No HTTP call, no L7 code. The browser sends a WS frame on the socket from §7:

browser: ws.send('{"type":"subscribe","subs":[
                     {"stream":"snapshot","instrument":"ACME"},
                     {"stream":"candle","instrument":"ACME","timeframe":"5m"}]}')
   ↓ same socket, WS text frame
readLoop(conn)                 // marketdata/ws.go — on the request goroutine (see §9)
  ReadMessage → json.Unmarshal → wsInboundFrame{Type:"subscribe", Subs}
  handleSubscribe(conn, Subs)
    per sub: validateSubItem (stream∈{snapshot,candle}; fanOut.KnownInstrument; tf∈{1m,5m,30m,60m})
    conn.Enqueue(ackFrame)                     // ack first
    fanOut.Subscribe(conn, accepted)           // register conn under subKey{stream,instr,tf}; seed snapshot subs
   ↓
writeLoop(conn)                // spawned goroutine — drains conn.outbound → socket (ack, then seed)

Thereafter the ingestor's PublishSnapshot/PublishCandle reach this conn via the FanOut → writeLoop → browser. Switching instruments is the frontend sending unsubscribe(old) + subscribe(new) on the same socket; a sub persists until the client unsubscribes (the server never auto-drops). One socket can hold many subs at once. All of this is shipped market-data; L7 has no route and no logic here.

9. Goroutine & lock topology (per live viewer)
HTTP server accepts GET /v1/ws
  → goroutine G1 (net/http per-request goroutine):
       runs L7 handleWS:  AuthFromRequest ; fanoutForSession [takes+RELEASES reg.mu] ; then →
       runs marketdata ServeHTTP (synchronously, still G1):
            Upgrade ; NewConn
            go writeLoop(conn)         ──►  goroutine G2 (sends connected frame, ack, seeds, live frames)
            readLoop(conn)                  (stays on G1: reads subscribe/unsubscribe for the conn's life)
            <-writeLoopDone                 (G1 parks here until the connection ends)
G1 = the request goroutine. It runs L7's dispatcher, then market-data's readLoop. So subscribe frames are processed on the goroutine that entered through L7's handler — but the code on it is market-data's, and L7 holds no lock and runs no logic there (the registry mutex was released inside fanoutForSession, J.7). L7's dispatcher frame is merely parked on the stack inside the ServeHTTP call until the socket closes.
G2 = the spawned writeLoop (sole socket writer).
The ingestor goroutine (per market, from spin-up) is separate; it enqueues into conn.outbound, which G2 drains.
Cost per viewer: 2 goroutines (G1 running readLoop + G2 writeLoop), standard for net/http + WS. The registry mutex is never held across a connection.

This is why the lock-before-delegate rule (J.7) is load-bearing: G1 is blocked in ServeHTTP for the entire connection; holding reg.mu across it would freeze the whole registry for as long as the socket is open.

10. Flagged / parked
P6 — identity-on-reconnect token. The base connect carrier is pinned (sessionID on connect, J.6). The hardening — a longer-lived token so a refreshed/dropped browser re-finds its session — is still open; AuthFromRequest is the seam where it lands.
Per-session instrument allowlist (P5-D9). SessionStartCmd has no field for it; sessions install permissive. Inherited parked item.
Frontend subscribe timing / switch behavior. When to subscribe (on connect vs instrument-click) and whether to unsubscribe on switch is a frontend choice; the backend supports any pattern.
Broker XOR desync. ErrSessionAlreadyLive shouldn't happen if L7's byUser held; if it does (e.g. stale broker state), L7 rolls back + 409.
Teardown unbind. Removing the session + its membership + stopping is Tier 3; join only creates. A WS drop itself is handled by the shipped Disconnect; L7's session record persists (reconnect-friendly) until exit/teardown.
11. Out of scope

On-demand join (gate + fused spin-up); leave/exit + teardown (Tier 3); the account read endpoints (balance/positions/orders/observation — broker-owned); the subscribe/stream internals (shipped market-data).