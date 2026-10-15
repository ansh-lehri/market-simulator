Layer 7 — Tier 0: Registry & Identity (Design Handoff)

Status: Tier 0 LOCKED. This document is the complete record of L7's foundational tier — the state L7 owns, how it is accessed, and the identity/persistence decisions under it — plus the settled infra/flow context Tier 1 builds on. It is written to start the Tier 1 thread cold.

What L7 is: the session/market conductor. It creates markets, sizes/seeds them, tracks who is in each, wires the broker and market-data component to them, and tears them down. L7 does no matching and holds no money. It is a pure live-wiring layer.

Source-of-truth docs this rests on (not restated here, only referenced): LAYER7_OBLIGATIONS.md, BROKER_EXPECTATIONS_FROM_L7.md, BROKER_MARKET_DATA_L7_EXPECTATIONS.md, BROKER_CONCURRENCY_AND_ROUTING.md, EXCHANGE_00_ARCHITECTURE.md, EXCHANGE_10_WIRING_SHARDING.md, EXCHANGE_18_SESSION_PHASE_CLOCK.md, LAYER_5_2_LIFECYCLE_FLOWS.md, LAYER_2_ORCHESTRATOR.md. The old L7 thread (f9c792b3-…) is discarded — superseded by the current component docs.

0. The mental model (load-bearing)

The word "session" was overloaded; split it:

A market = one running exchange instance (its own books, clock, matching, edge channels). The canonical evening session is one market instance. Each on-demand session is also just a market instance — a small, short-lived, single-user one. IN and US canonical are separate instances on separate clocks.
A user showing up does not create a market. L7 spins up markets; a user joins one.

So "3 users want on-demand" = L7 spins up 3 exchange instances and routes each user to theirs. The exchange is instantiated per market, never shared across independent sessions. Everything below follows from this.

One-line frame: the exchange matches orders in one market; the broker holds money and places orders; L7 is the conductor that creates markets, sizes/seeds them, tracks who is in each, wires the broker to them, and tears them down.

1. Process topology & persistence spine (settled context)
1.1 One process

L7, the exchange instances, the broker (money funnel), the market-data component, and L2 (agents) all run in one OS process. This is forced, not preferred: L7's wiring job is handing Go channels around (RegisterMarket(inbound, private, public), WireInstance(tape, …)), and channels are process-local. A separate-process L7 could not construct an exchange or hand the broker a channel — every wiring call would become RPC, forcing the split the design defers.

L7 talks to the frontend over HTTP + WebSocket (the one external door).
L7 talks to broker/exchange/market-data via direct Go calls + channels (internal).
The only things outside the process are Redis and Postgres (network).

L2 caveat. L2 is not yet designed/locked (unlike exchange/broker/L5, which are). Whether agents are in-process records or external is an L2-internal decision behind the trigger seam. L7 does not track agents and does not care who market participants are. L7 tells the orchestrator "populate this market with N agents of this profile" and tears it down at session end — that is the entire agent interaction. Nothing about agent execution appears in L7's registry.

1.2 Persistence split (system-wide, inherited)
Postgres = durable cross-session truth, touched only at create/load/save boundaries: accounts (L5), agent population + trade logs (L2), canonical OHLCV (market-data).
Redis = intra-session crash-recovery snapshot (L5 account split) + write-behind buffer for high-frequency writes (closed trades → drain to Postgres async).
In-memory = live working truth during a session.

L7 owns nothing durable. See §6 for the reasoning. (Terminology honesty: a single-process demo has no true HA — a process crash kills everything at once. Redis buys crash-recovery snapshotting, not hot failover.)

2. The two user flows (settled — Tier 0 data was derived from these)

The canonical and on-demand lifecycles diverge structurally, not just by account type. The difference: what triggers market preparation.

Canonical — the market is prepared on a schedule, independent of any user. L7's spin-up timer constructs/sizes/schedules/wires/seeds it; the clock opens it. The market is alive with agents even with zero humans in it (confirmed). Join and spin-up are separate operations. Per-user leave and whole-market scheduled teardown are both real and distinct.
On-demand — the user's HTTP action triggers the entire preparation of a private, one-user instance. Spin-up and join are fused. User-leave = teardown.
2.1 Canonical lifecycle (end to end)
(scheduled, no user) L7 timer → construct canonical instance(s) (IN, US — separate clocks), size (many shards), build SessionSchedule from rulebook (absolute boundaries), inject rulebook + reference prices (= prior close), wire broker edge + WireInstance(canonical).
(scheduled) Seed: trigger orchestrator BRING_UP (revive persisted population + top-up shortfall) + starting liquidity — before boundary-0.
(scheduled) Clock crosses boundary-0 → market open; live with agents, OHLCV persisting, tape flowing.
(user) User arrives → has a durable account → clicks join → L7 checks XOR, records membership, mints ParticipantID, sends broker the start signal (session_load), binds the user's WS to the canonical FanOut. User trades.
(user, optional) User leaves mid-session → per-user stop (auto-square → save → drop membership) while the market keeps running for everyone else.
(scheduled) Scheduled close → closing auction → closed → L7 teardown: stop emitting → close tape → broker stop for all users (session_save) → agent wind-down (flatten + persist population) → acks → Shutdown() → reap → clear registry.
2.2 On-demand lifecycle (end to end)
(user) Clicks start on-demand → gate first: balance check against the canonical account (durable Postgres read — see §7), pull bank→trading to qualify if short; the qualifying figure must settle before sealing.
(user) L7 constructs a fresh instance (1 shard, on-the-fly schedule: now → now+X, one continuous phase, no auction), wires broker + WireInstance(onDemand → never persists), seeds a throwaway population (fresh CREATE ×N, empty logs) + liquidity + seed reference price (no prior close for a fresh private market).
(user) Snapshot the qualifying figure → seed an isolated ephemeral account (PersistOnClose=false) via ondemand_session_load; mint ParticipantID; inject binding = this instance's channel; bind WS to this instance's private FanOut. Open.
(user) User leaves / timeout → teardown → close tape → broker stop → agent discard (throwaway, no persist) → session_discard (never writes to Postgres) → ack → Shutdown() → reap the instance → clear registry. Nothing reflects back to canonical; ephemeral OHLCV gone.
3. Tier 0 scope

Tier 0 designs the state L7 owns and how it is accessed — before anything mutates it. It does not wire a market (Tier 1), seed (Tier 2), or tear down (Tier 3). Four things were settled: the registry data model, the concurrency model, participant-id ownership, and the persistence answer. Plus the EnsureAccount account seam (§7), settled alongside.

4. The registry data model (LOCKED)

Two entities. Everything L7 does reads or mutates one of them. This is a description of in-memory Go structures, not database tables.

4.1 Identifiers & enums
go
type MarketID      string
type SessionID     string
type UserID        string
type AccountID     string        // handle from broker.EnsureAccount — NEVER an amount
type ParticipantID string        // L7-minted, per-session, opaque, ≠ BrokerID/AccountID

type Mode int
const ( Canonical Mode = iota; OnDemand )

type MarketTag string            // "IN" | "US" (on-demand inherits the tag it simulates)

type MarketState int
const ( Provisioning MarketState = iota; Live; TearingDown )
// no "Reaped" state — a reaped market is DELETED from the registry, not kept as a tombstone

type AccountType int
const ( Durable AccountType = iota; Ephemeral )   // STORED, not derived (explicit field)

type SessionState int
const ( Starting SessionState = iota; Active; Stopping; Ended )
4.2 Per live market
go
type Market struct {
    ID    MarketID
    Mode  Mode
    Tag   MarketTag
    State MarketState

    // live wiring — pointers into running goroutines; unserialisable by nature
    exch    *Exchange              // to Shutdown() at teardown/reap
    inbound chan<- InboundMessage  // to build each joiner's SessionBinding (see note)
    fanout  *FanOut                // to bind each live WS connection

    closeAt time.Time              // retained: arms reap/teardown regardless of trigger

    members map[SessionID]struct{} // ID set only — references into Registry.sessions
}

Field rationale (each traced to a job):

exch — reach the instance to Shutdown() at teardown. Without it → memory/goroutine leak (the reaping obligation).
inbound — the one pipe orders travel down to the exchange. InboundMessage is the exchange's submit/cancel wrapper (tagged BrokerID + Instrument). Retained for the market's whole life because every joiner (first and 50th) needs a fresh SessionBinding built from it — not just the first. chan<- (send-only) so nobody on L7's side ever consumes orders; reading is the exchange's job.
fanout — the thing that distributes the public tape to individual live WS connections. Retained because binding happens per-connection (every join and every browser reconnect), throughout the market's life. One FanOut per market; canonical users all share one (that's how many users share one feed); an on-demand instance's FanOut has exactly one subscriber (that's what makes its feed private).
mode — branches nearly everything (schedule shape, sizing, seed type, save-vs-discard, feed sharing).
tag — implies the session-local clock, selects the rulebook, names the persistence series.
State — a join is rejected before Live; teardown must not double-fire.
closeAt — retained (decided): needed whether teardown is L7-timer-driven or reacts to an exchange signal, and to arm the reap.
members — ID set only, references into the flat sessions map (see §4.4).

What L7 does NOT retain per market: the private and public output channels. The exchange has one inbound + two outbound. private (per-order fill notifications) is handed to the broker once (RegisterMarket) and forgotten — L7 settles no money. public (the anonymous tape) is handed to the market-data component once (WireInstance) and forgotten — L7 reads no tape. Of the exchange's four surfaces (exch handle + 3 channels), L7 retains handle + inbound + fanout, forgets private + public.

4.3 Per human session (membership)
go
type Session struct {
    ID          SessionID
    User        UserID
    Market      MarketID          // which market → answers "which FanOut on reconnect"
    Account     AccountID         // carried in the start signal; opaque handle only
    AccountType AccountType       // stored: teardown branches save/discard on it
    Participant ParticipantID     // L7-minted; broker stamps it on orders for self-match
    State       SessionState
}

Session carries no channel — the wiring lives on the Market (shared by all its members); a session only references its market by ID and reaches the wiring through it. The SessionBinding is built fresh per-join from the market's retained inbound.

4.4 The registry container + membership placement (LOCKED)
go
type Registry struct {
    mu       sync.Mutex                 // §5 — the one guard

    markets  map[MarketID]*Market
    sessions map[SessionID]*Session     // the ONE home a Session lives in
    byUser   map[UserID]*Session        // user → their (single, XOR) live session
}

Placement decision: a Session lives in exactly one place — the flat sessions map. Market.members holds only session-IDs; byUser is an index holding a pointer, not a copy. Rationale: a membership is reached from three directions — by-session (broker stop signal / teardown ack name a SessionID), by-user (leave, reconnect re-bind, XOR check), by-market (whole-market teardown). Flat home + two indexes makes all three direct (O(1) or O(members-of-one-market)); putting the full Session inside Market.members would force by-user and by-session lookups to scan every market. Single home = single source of truth = nothing to keep in sync on a state change.

Accepted tradeoff: whole-market teardown does one extra deref per member (ID → sessions[id]) — negligible (cold path, a handful of humans) in exchange for making the two frequent lookups direct.

byUser is the relaxation seam. Under strict XOR (§ policy) it is map[UserID]*Session. If the policy later relaxes to one-live-session-per-mode (canonical + on-demand at once, different accounts), this single field becomes map[UserID]map[Mode]*Session and nothing else changes — the sessions primary map (keyed on SessionID) is untouched. Mirrors the broker's own "keyed for it, not built yet" stance.

5. Concurrency & ownership (LOCKED — mutex, Option B)

The problem. The registry is mutated concurrently by: HTTP handlers (join/start/leave/end), the canonical spin-up timer, the reap timer, and completion callbacks (broker teardown-ack). Concurrent map access = data race.

Decision: mutex-guarded registry (Option B), not a single-writer event-loop (A).

Reasoning: the event-loop's headline advantage is "no lock on the hot path" — but L7 has no hot path. It mutates its state a handful of times per session, not thousands of times a second (that's the exchange/broker, sharded and long-lived by necessity). L7's real work — constructing an exchange, seeding, waiting for the broker teardown-ack — is slow and must run off in a spawned goroutine in either model, so only tiny state transitions are ever serialized. That flattens A's advantage to ceremony L7 doesn't need. B is far less machinery for a cold path. (A was a legitimate call for pattern-consistency

saga cohesion; B chosen for simplicity.)
5.1 Goroutine model

Different shape from broker/exchange:

Broker/exchange shards = long-lived, permanently parked, because they're on the hot path (thousands of orders/sec).
L7 helpers = short-lived / one-shot. A slow task (spin-up a market, tear one down) spawns a helper goroutine that does the work and then returns and vanishes. No permanent pool.
The only long-lived L7 goroutines = its timers (canonical spin-up, reap) that must watch the clock continuously.

So: ~two permanent timer goroutines + a scattering of short-lived task goroutines that come and go.

5.2 THE RULE (load-bearing, holds regardless of A/B)

L7 never holds its critical section across a call into another component or a wait.

Concretely — the wrong way to do a join:

lock → build exchange (4s) → wire broker → seed agents → mark Live → unlock   ✗

This holds the only lock for seconds, freezing every other join and both timers. The right way:

lock → create market record, mark Provisioning → unlock          (microseconds)
(no lock held:) build exchange (4s) → wire broker → seed agents   (the slow work)
lock → mark Live → unlock                                        (microseconds)

The slow steps are calls into other components (build exchange, wire broker, wait for broker teardown-ack). The ack-wait is the dangerous one: waiting while holding the lock can freeze L7 indefinitely if the broker is slow. Lock only to touch L7's own state, for an instant; do all call-outs and waits lock-free; record completion as a fresh short critical section. The spawning goroutine already holds the market it created, so "mark Live" needs no lookup.

6. Persistence (LOCKED — none)

L7 owns no durable state. The entire registry is in-memory — not Postgres, not Redis.

Per field: exch/inbound/fanout are live pointers into running goroutines (unserialisable by nature); ParticipantID is per-session (regenerated fresh); AccountType is derivable from mode; memberships are session-scoped live wiring.

Why not even Redis for membership (the reasoned answer, not laziness):

Browser crash / refresh / network drop (common): the server process is fine — market alive, book intact, broker holding channels, FanOut alive with its pointer still in L7's memory. Only the browser's WebSocket dropped. On reconnect the browser opens a new WS and L7 re-runs the normal bind ("which market is this user in? → attach to that FanOut") against membership it never lost. No persistence needed — L7's memory was never touched.
Server-process crash (rare): the whole process dies, and the exchange book is gone (exchange-internals persistence is explicitly deferred). A persisted "user U was in market M" would point at a market that no longer exists — nothing to bind to, no session to resume. So the fact is useless exactly in the crash that would consume it.
Persisting it therefore buys nothing in the case that matters (browser) and nothing recoverable in the case that would consume it (server), while adding a staleness/consistency failure mode for a fact whose authoritative copy already sits in live memory — the same reasoning L5 used to reject Redis-caching the account row.

Accepted posture: a mid-session server crash loses the live canonical session; on restart L7 starts clean and its timer prepares the next scheduled session. The population REVIVEs from Postgres (identity + P&L history intact); the market is brand new. This is a stated prototype limitation, not a solved case.

Everything durable the simulation needs lives below L7 already (accounts → L5, population → L2, canonical OHLCV → market-data). L7 is a pure live-wiring layer.

7. The account seam — EnsureAccount (LOCKED, Tier-0-adjacent)

Every user entry (canonical join or on-demand start) begins with broker.EnsureAccount(userID) → accountID.

It is a broker call, not L7 reading Postgres. "Does this user have an account" is a money-domain question (L5 owns the accounts table; backend is sole writer). L7 is money-blind — it may not read the money tables even to check. And the live account may be loaded in-memory on a broker shard, making the Postgres row stale — another reason L7 must not read it directly.
Get-or-create, atomic, inside the broker (try open_account; on ErrAccountExists → get_account_handle; return the handle either way). This leans on L5's uq_accounts_owner UNIQUE constraint as the sole race arbiter — splitting the check (L7) from the create (broker) would reintroduce the check-then-write race the constraint exists to kill.
Starting-capital policy stays inside the broker (it owns economy config). L7 passes no amount and receives only the AccountID handle → L7 stays money-blind.
Idempotent + fronts both flows → a brand-new user whose first action is "start on-demand" still has a durable canonical account to gate against. And a future auth/signup layer calling EnsureAccount at registration makes L7's entry-time call a guaranteed "already exists" hit — nothing in L7 changes.

On-demand gate reads the durable Postgres account directly (decided): an on-demand user is not canonical-live (XOR), so there's no loaded canonical account/shard to route a pull through, and the durable figure is the cleanest source (no phantom canonical session).

8. Decision log (Tier 0)
#	Decision	Status
T0.1	Registry = two entities (Market, Session), in-memory Go structs	LOCKED
T0.2	Per-market fields: exch handle, inbound, fanout, mode, tag, state, closeAt, members(ID-set); private+public NOT retained	LOCKED
T0.3	Per-session fields: ID, user, market ref, account handle, accountType (stored), participant, state; no channel	LOCKED
T0.4	Membership placement: flat sessions is the home; market.members=ID-set; byUser=index	LOCKED
T0.5	Concurrency: mutex-guarded registry (Option B), not event-loop	LOCKED
T0.6	Goroutine model: 2 permanent timers + short-lived one-shot task goroutines	LOCKED
T0.7	THE RULE: never hold the lock across a call-out or wait	LOCKED
T0.8	ParticipantID: L7 mints, opaque, per-session, ≠ BrokerID/AccountID	LOCKED
T0.9	One-session policy: strict XOR (one live session per user, period); reject on conflict, never auto-end	LOCKED
T0.10	Persistence: none — entire registry in-memory (Postgres and Redis both empty for L7)	LOCKED
T0.11	Account seam: broker.EnsureAccount(userID) — broker call, get-or-create atomic, fronts both flows	LOCKED
T0.12	On-demand gate reads durable Postgres canonical account directly	LOCKED
9. Flagged / deferred (named, not designed)
FLAGGED — identity-on-reconnect. A reconnecting browser must present something (a user id / session token) so L7 can find its membership in memory and re-bind. Real requirement; solved with a client-held token, not persistence. No auth layer in the prototype, so the mechanism is open. Carried into the interface work.
DEFERRED — crash-recovery binding re-injection. On a future server-crash-recovery path, re-establishing each session's SessionBinding/FanOut. A joint L5+L2+engine+L7 pass, only meaningful after exchange-internals persistence lands.
DEFERRED — auth / identity / signup layer. Nominally L7-adjacent future scope; out for the prototype. EnsureAccount is already shaped so a signup layer slots in above it for free.
OPEN (Tier 3) — teardown trigger: L7-timer vs exchange-signal. closeAt is retained either way; which mechanism fires teardown is a Tier 3 decision.
NOTED — instruments. For now L7 is not responsible for generating instruments; it will read a country/market's available instruments from config (e.g. a YAML) and push them to the exchange at construction. The push mechanism is a Tier 1 detail.
NOTED — reaped-market tombstone. No Reaped state today (torn-down markets are deleted). If late-arriving acks ever need to correlate against a just-reaped market, a 4th state / short-lived tombstone is the additive change.
10. What Tier 1 inherits & opens

Tier 1 = Market construction & wiring — bringing a market instance into existence, wired to both broker axes, but empty (no users, not seeded). It inherits from Tier 0: the Market/Registry structs to populate, the mutex + THE RULE for how construction mutates state (mark Provisioning → build lock-free → mark Live), and the retain/forget channel split.

Tier 1 stages (preview, not designed):

Exchange constructor/lifecycle API — formalize NewExchange(config)/Start()/Shutdown()/Inbound()/Private()/Public() over the existing Exchange struct + startup sequence (the gap: EXCHANGE_10 §8 describes struct + startup, doesn't expose these as methods; LAYER7_OBLIGATIONS assumes them). Touches the exchange doc.
Config assembly + rulebook + instrument push (the NOTED instruments item) + sizing (NumShards: canonical many, on-demand 1) + schedule construction (canonical from rulebook; on-demand on-the-fly).
Money-side wiring — RegisterMarket(marketID, inbound, private, public); hand the broker the market edge.
Market-data wiring — WireInstance(tape, market, mode) → *FanOut; retain the FanOut.

Design sequence from here: T1 construct+wire → T2 seed (liquidity + BRING_UP + refs)

admit user + draft the L7→broker session-start command (unblocks broker Tier 3) → T3 teardown ordering + agent wind-down + lock the stop command → T4 on-demand deltas (gate/pull, ephemeral, throwaway agents, reap) → T5 membership invariants → T6 deferred (crash-recovery, auth).