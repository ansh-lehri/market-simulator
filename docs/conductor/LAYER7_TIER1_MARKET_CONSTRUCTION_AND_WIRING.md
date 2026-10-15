Layer 7 — Tier 1: Market Construction & Wiring

What this doc covers. Tier 1 is how L7 stands an exchange instance up, feeds it, wires it to the other services, runs it, and (in outline) tears it down — plus the canonical daily trigger that does this without a user. It is the lifecycle skeleton: the stages an exchange goes through and the calls L7 makes at each. It deliberately does not cover seeding, admission/session-start command shapes, or teardown choreography — those are later tiers, flagged where they touch this one.

Source of truth. The exchange side is taken from the wire codebase (wire/exchange.go, example/main.go, wire/state.go), not from prose. Where the older LAYER7_OBLIGATIONS.md and the code disagree, the code wins — see §6 for the corrections that implies.

Companion docs.

LAYER7_TIER0_REGISTRY_AND_IDENTITY.md — the registry, session/market entities, the mutex concurrency model, EnsureAccount. Tier 1 builds directly on it.
EXCHANGE_10_WIRING_SHARDING.md — the exchange struct + startup sequence (prose; the code has moved past it).
EXCHANGE_18_SESSION_PHASE_CLOCK.md — the time-driven phase clock.
BROKER_EXPECTATIONS_FROM_L7.md, BROKER_CONCURRENCY_AND_ROUTING.md, BROKER_ORDER_LIFECYCLE_intake_punch_settle.md — the broker seam (expectations, conceptual — not exact call signatures).
BROKER_MARKET_DATA_L7_EXPECTATIONS.md — the market-data seam.
0. The four stages of Tier 1
Lifecycle API (§1) — the exchange surface L7 calls: construct, start, stop, and the three edge channels.
Config assembly (§2) — how L7 builds the three constructor arguments for a market, from a per-market config file plus dynamic per-spin-up values.
Wiring + the session cycle (§3) — delivering the instance's channels to the broker and market-data, and the full mutex/goroutine cycle L7 runs when a user joins or starts a session.
Canonical scheduling (§4) — the user-independent daily trigger that brings canonical markets up on their own schedule.
1. Stage 1 — The exchange lifecycle API L7 depends on
1.1 The surface (from wire/exchange.go; code is the contract)
go
NewExchange(numShards int, schedule SessionSchedule, instruments []InstrumentSpec) *Exchange
(*Exchange).Start(tickInterval time.Duration)   // launches goroutines; idempotent
(*Exchange).Stop()                              // explicit teardown; idempotent; safe if never started

// edges are EXPORTED, direction-narrowed FIELDS — not accessor methods:
exch.Inbound   // chan<- InboundMessage   (send-only  — orders in)
exch.Public    // <-chan TapeEvent        (read-only  — tape out)
exch.Private   // <-chan EngineResult     (read-only  — results out)
1.2 The lifecycle sequence per market
NewExchange(...)   → everything built, three channels born, nothing running
   ↓  wire the emit-side readers to Public and Private (before Start)
Start(tickInterval) → goroutines live; the clock's monitor begins ticking
   ↓  market runs; the clock opens/closes it purely by time
Stop()              → explicit teardown
1.3 Locked decisions
Adopt the code's API as-is. No redesign; NewExchange / Start(tickInterval) / Stop() + the three exported edge fields.
No Open(). The market opens purely by time — the monitor that Start() launches flips the phase to continuous when real time crosses the boundary. L7 never "opens" anything.
Stop() is explicit and L7-driven, for both canonical and on-demand. Reaching PhaseClosed does not stop the goroutines — it only makes the gate reject orders. Goroutines run until Stop() is called. This is what on-demand early reap needs (kill mid-session, no scheduled close reached). L7 is the sole owner of teardown timing.
Stop() owns edge-closing. It closes inbound, waits for the shards to finish (so nothing is still emitting), then closes Public + Private. L7 never reaches in to close a channel it handed away. This supersedes Tier 0's wording "L7 closes the tape" → it is now "L7 calls Stop(), which closes the tape after the exchange has stopped emitting."
Edge retention: L7 holds Inbound (for the session wiring); it hands Public and Private to the services that consume them.
MarketState maps onto the lifecycle: construct+wire = Provisioning, running = Live, Stop() in progress = TearingDown. (The exact Provisioning → Live flip point touches seeding/open and is settled in Tier 2 — see §5.)
1.4 The channels — how they come to exist

The three channels are created inside NewExchange (plain make, unbuffered). Two things happen to each before the constructor returns: it is wired into the exchange's own internals (shards, emitters, ingress), and it is exposed as an exported, direction-narrowed field on the returned *Exchange. So L7 does nothing special to "get" the channels — it called NewExchange, holds the returned pointer, and reads exch.Inbound / exch.Public / exch.Private as fields. Nothing flows until Start(). The direction-narrowing (chan<- / <-chan) is a type-level guarantee that orders only go in and tape/results only come out, enforced the moment L7 hands an edge onward.

2. Stage 2 — Config assembly
2.1 What the stage is

L7 must produce the three NewExchange arguments (numShards, schedule, instruments) for a market. Everything L7 feeds is either static (read from a per-market config file, stable across sessions) or dynamic (computed fresh at each spin-up).

2.2 Config file — one per market (markets/IN.yaml)
yaml
market: IN                    # the tag services stamp ('IN'/'US')
timezone: Asia/Kolkata        # market's local TZ — anchors wall-clock → absolute
ohlcv_interval: 1m            # candle cadence (market-wide)
num_shards: 0                 # 0/absent → L7's default sizing; >0 → use verbatim
preopen_lead: 25m             # bring the instance up this long before continuous-open

session:                      # canonical phases, wall-clock in `timezone`
  - { at: "09:15", phase: continuous }
  - { at: "15:29", phase: closing_auction }
  - { at: "15:30", phase: closed }

ondemand:
  duration: 35m               # practice-session length

instruments:
  - { id: ACME, tick_size: 1, band_bps: 1000, seed_price: 10000 }
  - { id: BETA, tick_size: 5, band_bps: 500,  seed_price: 50000 }
Market-level: timezone, ohlcv_interval, num_shards, preopen_lead, session, ondemand.
Instrument-level: tick_size, band_bps, seed_price (these genuinely vary per instrument).
session is a boundary list ({at, phase}), so L7 translates whatever phases are listed — no L7 logic decides which phases exist. An auction phase appears iff the config lists it; whether the exchange's auction package is built is not L7's concern.
seed_price stays in the structure per instrument; the exact point where L7 uses it (see §2.4) is settled where needed, not baked into the structure.
2.3 The InstrumentSpec shape (from wire/state.go)
go
type InstrumentSpec struct {
    Instrument    contract.InstrumentID
    Reference     contract.Ticks    // reference price
    BandBps       int               // circuit band width, basis points
    Rules         gate.Rulebook     // today: { TickSize } (lot size / min qty are future)
    OHLCVInterval time.Duration
    OHLCVStart    time.Time
}
2.4 How each argument is built

numShards — config num_shards if > 0; else L7's default sizing (runtime.NumCPU()-ish; the exact default mechanism is tentative). On-demand: 1.

schedule (SessionSchedule, a boundary list):

Canonical: for each phase the config lists, anchor its wall-clock time to today's date in the market's timezone → an absolute instant; collect, sort ascending, assert first = session start and last = PhaseClosed.
On-demand: L7 generates it — now → now + ondemand.duration, a single continuous phase, no auction.
The time-generation mechanism itself is tentative and expected to change. The boundary list is the field it writes into regardless.

instruments []InstrumentSpec — per instrument:

Instrument, Rules.TickSize, BandBps, OHLCVInterval → direct copy from config.
OHLCVStart → the session start instant (canonical: today's open in TZ; on-demand: now).
Reference → L7 reads it directly from the country's OHLCV database (L7 has direct DB access; no service intermediary in this path):
Canonical: prior close (the latest canonical candle's close). Fallback for the first-ever session (no candle exists): the per-instrument seed_price.
On-demand: the open of the last canonical session for that instrument (the first candle of the last session, its open). This is swappable to prior-close later — a one-query change, nothing downstream cares. (The "open" variant asks L7 to find "first candle of the last session," slightly more than "latest candle.")
2.5 Canonical vs on-demand — the only differences

On-demand is a full copy of the canonical market (same whole instrument list, same rules). It differs only in:

mode = onDemand (services never persist its data; its account is ephemeral),
schedule = generated (now + duration, single continuous phase),
numShards = 1,
Reference = last canonical session's open (vs canonical's prior close).

The instruments argument is otherwise identical to canonical for that market. There is no on-demand instrument selection — the user gets the full set and trades whichever they want.

3. Stage 3 — Wiring + the L7 session cycle
3.1 Channel handoff

An exchange instance owns its three channels. L7 delivers them to the services, tagged with (user identity, instance id):

→ broker: Inbound, Private, and Public (the broker feeds its own best-ask cache off the tape).
→ market-data: Public.

L7's job is to deliver the channel + the identity and say "this user, this instance, these channels." What each service does internally with them (including how it handles a canonical instance shared by many users) is the service's concern, not L7's. Public fans out to both broker and market-data; Inbound/Private go to the broker.

3.2 The concurrency model (from Tier 0)
One mutex over the registry; ~2 permanent timer goroutines (one per canonical market — see §4); short-lived goroutines for anything that waits.
The lock is never held across a call-out or a wait. A session-start is therefore split into quick locked bookkeeping and unlocked slow work in a spun-off goroutine that merges back under the lock.
3.3 The full cycle — a user joins / starts a session

1. Request lands. Under lock: enforce one-live-session-per-user; look up the target instance. Two paths follow.

2a. Instance already up (canonical steady state). Under lock: reserve the user's session slot, read the instance's channels. Release lock. Wire the session — hand the channels + identity to the broker and market-data. Return "joined."

2b. Instance must be brought up (canonical first-of-day, every on-demand). Under lock: mark the instance Provisioning (so a second concurrent joiner waits rather than building a duplicate). Release lock. Spin off a goroutine — nothing below holds the lock:

build config (Stage 2) → NewExchange(...),
wire the emit-side consumers, then Start(tickInterval), then wait for the instance to report ready (consumers attached before Start, so the first emit can't block),
re-acquire the lock and merge back: flip Provisioning → Live, commit the instance + its channels into the registry, release lock,
then wire this user's session (channels + identity → broker, market-data) and return "joined." Any joiners who queued on Provisioning now proceed down path 2a.

3. Teardown (a later tier). Reverses it: unwire the session; when the last session leaves an on-demand instance (or canonical hits close), Stop() the instance and drop it from the registry.

3.4 The load-bearing point

Building / starting / waiting for an exchange is slow and must not freeze the whole registry — hence the spin-off goroutine. The mutex only guards the registry mutations (reserve slot, Provisioning gate, commit Live). The Provisioning gate is what stops two users racing to build the same instance; it is kept for on-demand as well, not just canonical.

4. Stage 4 — Canonical scheduling (the user-independent trigger)

Canonical comes alive on its daily schedule — with agents, before any human joins — so its trigger is a clock, not a user.

Timer topology: one timer goroutine per canonical market, each armed to its own market's open (its own timezone, from the S2 config).
Reuses Stage 3: a timer firing runs the same construct → wire → Start → merge-into-registry cycle as a bring-up (§3.3, path 2b); only the trigger differs. A canonical user then almost always hits the "already up" path (2a), because the timer beat them to it.
Lead time: the instance is brought up ~20–25 min before continuous-open, configurable per market via preopen_lead. What fills that pre-open window (agent seeding) is deferred to Tier 2 — this stage only pins that the trigger fires on that lead.
Re-arm: after today's bring-up, the timer re-arms for the next weekday. No holiday calendar for now (flagged as a later config addition).
Crash posture: no session/state recovery. Nothing is persisted on the exchange side, so a crash disconnects every live canonical user; they see an "internal issue" and reconnect fresh into whatever instance exists. The daily timer is the only bring-up — a mid-day crash leaves that canonical market down until the next day's timer fires. (See §5 for the deferred startup-recovery idea.)
5. Parked / deferred / TBD
Seeding window (Tier 2). The market only accepts orders during a continuous phase and rejects while PhaseClosed, so seeding = injecting funded orders during an accepting phase. How agents seed inside the pre-open lead is a Tier-2 problem. The exact Provisioning → Live flip point (§1.3) settles with it.
Per-session inbound binding — exact command shape. L7 delivers the channels + identity per session (§3.1); the precise session-start command struct the broker consumes is the broker's "injection mechanic," designed in the admission/lifecycle tier.
Teardown choreography (Tier 3). Unwire ordering, on-demand reap on last-leave, canonical Stop() on close — outlined in §3.3 step 3, designed later.
Crash startup-recovery pass — TBD (dropped for now). A startup reconciliation pass was considered: on restart, for each canonical market, if wall-clock is inside the open window, bring the instance up immediately rather than waiting for the next day's timer. It restores no state (positions/orders/agents are gone) — its only value is demo optics (not showing a dead market for the rest of a day after a mid-day crash). Dropped for beta; revisit post-beta if the mid-day-dead-window matters.
Holiday / trading calendar — later config. Weekdays only for now.
Reference "open" vs prior-close for on-demand. Currently last canonical session's open; swappable to prior close later (one-query change).
Service internals. How the broker and market-data internally handle a shared canonical instance, result routing, best-ask caching, etc. — out of L7's scope; not designed here.
6. Corrections to LAYER7_OBLIGATIONS.md (code wins)

The obligations doc was written ahead of the code and is stale in these specifics:

Shutdown() → Stop() — the teardown method is named Stop().
Edges are fields, not methods — exch.Inbound, not exch.Inbound().
Start takes a tickInterval — L7 passes it (production ≈ time.Second); the obligations doc omitted it.
No config wrapper — the constructor is NewExchange(numShards, schedule, []InstrumentSpec), three positional arguments.
RegisterMarket(marketID, inbound, private, public) is superseded. The real model (§3.1) delivers the instance's channels to the services per session, tagged with user + instance identity — Public to both broker and market-data, Inbound/Private to the broker. The obligations doc's single per-market call with that positional signature does not reflect what was decided.

The obligations doc remains valid as a statement of conceptual expectations; it is not an exact call-signature contract.