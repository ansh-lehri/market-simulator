Layer 7 — Tier 2, Stage 1: Seeding

What this doc covers. How L7 makes a constructed market ready to trade before humans arrive — populating agents and establishing starting liquidity — for both canonical and on-demand markets. It is the first stage of Tier 2 (seed + admit). It covers only seeding; admission (the admit-a-user flow, the session-start command, the market-data-at-admission reconciliation, identity-on-reconnect, the HTTP surface) is the next stage and is not designed here.

Scope discipline. L7 does not seed anything itself. It is a pure orchestrator: it triggers two external actors (the agent orchestrator L2, and a separate market-operator participant) and sequences those triggers relative to the exchange clock. What those actors do internally, and how the exchange gate accepts or rejects orders, are their concerns, not L7's. This doc stays at the trigger + timing interface.

Builds on.

LAYER7_TIER0_REGISTRY_AND_IDENTITY.md — the registry, the mutex + THE RULE, the Provisioning/Live market states, the two user flows.
LAYER7_TIER1_MARKET_CONSTRUCTION_AND_WIRING.md — construct → wire → Start, the per-market config file, the provisioning goroutine, Reference read from the OHLCV DB, the internal phase clock (PhaseClosed rejects orders; the market opens purely by time).
1. What "seeding" is

Reference prices were pulled forward into Tier 1 (Stage 2), so seeding is now two things:

Population — bring the AI agents into existence for this market. L7 triggers the agent orchestrator (L2) via BRING_UP. L2 owns the agents; L7 owns only when the trigger fires.
Starting liquidity — establish a resting order book so the market isn't empty when trading begins. This is done by a separate market-operator participant, not by L7 and not by the agents. L7 triggers it via SeedLiquidity.

Both are triggers L7 fires, not work L7 does. Neither actor's internals are L7's concern.

2. The load-bearing timing fact, and how it's resolved

The exchange's phase clock rejects orders while PhaseClosed and only accepts them during a continuous phase. But the canonical flow requires the market to be seeded before humans arrive. These appear to conflict: "seed before open" vs "orders only after open." The resolution is a split between two distinct instants:

Internal continuous-open — the exchange's real open, the instant its clock crosses into the continuous phase and orders start being accepted (e.g. 09:15). This is the truth the exchange runs on.
User-facing open — the time the product tells users the market opens (e.g. 09:30). This is a UI/frontend gate only. It is not modeled in L7 — L7 admits a user whenever a join request actually arrives; "don't let anyone in before 09:30" is the frontend's job, and there is no corresponding action for L7 to take.

The gap between the two instants is where the book forms. Concretely, two different events happen at two different times:

Population happens during the pre-open lead, while PhaseClosed. BRING_UP brings agents into existence — that is not order flow, so no gate rejects it. Agents can be created while the market is still closed.
Order placement happens only after internal continuous-open. Agents place orders, and the market-operator places its liquidity, only once the phase accepts orders. The book therefore forms in the window between internal-open and user-facing-open.

So the two windows do two different jobs: the pre-open lead buys agent population; the internal-open → user-facing-open gap buys book formation.

3. The market-operator participant (liquidity)

Starting liquidity is provided by a market-operator / liquidity-manager participant that is entirely separate from L7. Its properties:

It has its own broker account and effectively unlimited balance.
It has access to all canonical markets.
Its sole job is to seed liquidity before humans enter.
Mechanically it is just another participant: it places orders through the broker onto the market's inbound channel, exactly like an agent. To the exchange/broker it is indistinguishable from any other participant.

L7's only relationship to it is the trigger. L7 does not become a participant, hold any balance, or place any orders. (This corrects an earlier framing where L7 itself was the operator — L7 stays a pure orchestrator.)

3.1 What L7 hands the operator

L7 triggers it with:

SeedLiquidity(marketID, instruments[])
    where each instrument carries: id, reference, tick_size,
    and its liquidity metadata { levels, spacing_pct, near_qty, taper }
L7 forwards the instrument list + per-instrument liquidity metadata as parameters. The metadata lives in the market config file (one file — see §4); L7 is already parsing that file for construction, so it forwards the relevant slice rather than making the operator read the file itself. (The operator could read the same file; passing it as params was chosen so the operator needs no file-reading path and there is one source of truth.)
L7 also forwards the reference it already computes in Tier 1 (canonical: prior close; on-demand: last canonical session open). The operator anchors its ladder on the same reference the circuit band uses — no separate price lookup on the operator's side.
3.2 The liquidity shape — and an explicit warning

The per-instrument shape chosen for the demo is a tapered ladder:

levels — number of price levels per side.
spacing_pct — spacing between levels as a percentage of the reference (chosen over raw ticks so one set of knobs scales across instruments of very different price magnitudes).
near_qty — quantity at the level closest to the reference (the fattest level).
taper — a multiplier applied per step outward, so quantity thins from the touch outward (e.g. taper: 0.7 → 100, 70, 49, 34, …).

Bids and asks mirror the same shape around the reference (symmetric).

This shape is deliberately bad. Real markets do not have liquidity sitting in an identifiable geometric / multiplier ladder — a tapered ladder is trivially recognizable and unrealistic. This is a demo placeholder to be replaced, not the intended design. It is recorded as such so it is not mistaken for a considered liquidity model later.

4. Config surface

The per-instrument liquidity metadata lives in the same per-market config file L7 already reads (Tier 1, Stage 2). One file, one source of truth.

yaml
market: IN
timezone: Asia/Kolkata
ohlcv_interval: 1m
num_shards: 0
preopen_lead: 25m

session:
  - { at: "09:15", phase: continuous }
  - { at: "15:29", phase: closing_auction }
  - { at: "15:30", phase: closed }

ondemand:
  duration: 35m

instruments:
  - id: ACME
    tick_size: 1
    band_bps: 1000
    seed_price: 10000
    liquidity:                 # NEW in Tier 2 — the operator's per-instrument shape
      levels: 5
      spacing_pct: 0.1
      near_qty: 100
      taper: 0.7
  - id: BETA
    tick_size: 5
    band_bps: 500
    seed_price: 50000
    liquidity:
      levels: 5
      spacing_pct: 0.2
      near_qty: 60
      taper: 0.6

The liquidity block is per-instrument and authored (not derived), because the book shape genuinely differs per instrument. Everything else in the file is as Tier 1 locked it.

5. The two triggers, precisely
5.1 BRING_UP — agent population (LOCKED)
BRING_UP(market, mode, instruments+related-data)
L7 passes only what it alone knows: market (which market to populate) and mode (canonical | onDemand — the thing that drives revive-first-then-topup for canonical vs fresh-throwaway for on-demand; L2 cannot know it otherwise). Plus the instrument list + related data as a safety param — forwarded for now, removable later if L2 turns out not to need it.
L2 owns the population count, the behavioral profile mix, its own store / persistence (its Postgres, its revive path), and any RNG seed. These are L2's calibration domain; L7 does not pass them (Option B). L7 passes no store handle (L2 owns its own persistence) and no seed (canonical revives — a seed would wipe accumulated P&L; on-demand throwaway needs no reproducibility for the demo).
BRING_UP is a synchronous (blocking) call. The provisioning goroutine blocks on it until population is done. This is safe and desirable:
No cross-user head-of-line blocking, because every bring-up runs in its own off-lock goroutine (canonical is one instance per market; on-demand is one instance per user). User A's population blocking inside goroutine A does not touch user B in goroutine B. The registry lock is held only for the microsecond bookkeeping, never across the wait (Tier 0's THE RULE).
On-demand wants the wait. The on-demand user's 3–4 min is legitimate "preparing your simulation" time — they should enter a populated market, not an empty one. Blocking is the intended UX there, not a cost.
Canonical blocks inside the pre-open provisioning goroutine, where no user is waiting on it and there is only one instance per market — nothing to head-of-line block.
5.2 SeedLiquidity — operator liquidity (LOCKED)
SeedLiquidity(marketID, instruments+liquidity-metadata+reference)   // §3.1
Fire-and-trust — L7 does not wait for a seed-complete signal. This is not a choice but a necessity: the operator's effect (resting orders in the book) cannot happen until after internal continuous-open, because orders bounce while PhaseClosed. So "SeedLiquidity returned" could only ever mean "the operator acknowledged the trigger," never "the book is seeded" — there is nothing to synchronously block on in the pre-open window. The operator acts once the phase opens; L7 moves on.
5.3 Ordering between the two triggers (LOCKED)
Independent — no dependency. The operator seeds off the L7-computed reference, not off the agents; the agents trade off whatever is in the book. Neither needs the other to exist first.
BRING_UP is fired first by convention only (readability: participants exist, then liquidity) — the order has no functional effect, since neither places orders until after internal-open anyway.
BRING_UP is sync (population can complete pre-open); SeedLiquidity is fire-and-trust (its effect is inherently post-open). This asymmetry is intrinsic, not a preference.
6. The Provisioning → Live flip (LOCKED — Option 1)
Live means "constructed + populated + started = admittable." The flip happens right after Start, in the closing re-lock of the provisioning goroutine (the same place Tier 1 placed it).
"Tradeable" is NOT an L7 state. Whether orders actually flow is the exchange phase gate's concern. L7 does not mirror the clock in its own state — doing so would duplicate clock state L7 doesn't own.
Rationale: what Live needs to gate is "is it safe to admit a session to this market," and that is true the moment it is constructed + populated + started — admission (wiring a user's channels) does not require the phase to be open. A user can be wired and simply be unable to place orders until the clock opens, which the exchange gate enforces regardless.
Accepted transient: a canonical join arriving between Start and internal-open is admitted (wired) into a market that is Live but still PhaseClosed — the user is connected, but their orders bounce until the clock opens. The user-facing open (UI gate) makes this moot in practice; L7 does not try to prevent it.
7. Canonical seeding — end-to-end flow

The two triggers sit inside the Tier 1 provisioning goroutine (spun off, off-lock). The full canonical spine:

preopen_lead fires (timer, per canonical market)
  → [registry lock: create market record, mark Provisioning; unlock]   (microseconds)
  → (provisioning goroutine, OFF-LOCK — nothing below holds the lock:)
        construct + wire                         (Tier 1)
        BRING_UP(market, canonical, instruments) (SYNC — blocks until agents populated)
        SeedLiquidity(market, instruments+meta+ref)  (fire-and-trust)
        Start(tickInterval)
     [registry lock: flip Provisioning → Live, commit instance + channels; unlock]
  → clock crosses internal continuous-open (e.g. 09:15), phase = continuous
        → agents place orders + operator places its tapered ladders → BOOK FORMS
  → user-facing open (e.g. 09:30): humans admitted  (UI-gated — not L7)

Notes:

Populate before open, place orders after open. BRING_UP runs during the pre-open lead (PhaseClosed); order flow (agents + operator) starts only after internal-open.
The provisioning goroutine finishes at Start (well before internal-open, thanks to preopen_lead). Nothing waits on order placement or on the book forming — that is the clock's job.
The mutex is held only for the two microsecond bookkeeping steps (mark Provisioning; flip Live). All slow work — construct, wire, the sync BRING_UP wait, Start — is off-lock, per Tier 0's THE RULE.
The Provisioning gate (from Tier 1) means a second concurrent joiner waits on the in-flight bring-up rather than building a duplicate instance.
8. On-demand seeding — end-to-end flow

On-demand is a full copy of the canonical market (same whole instrument list, same rules) that isn't persisted and has a flexible start. It differs from canonical in that its spin-up is fused with the user's join and it has no pre-open lead — it opens at ~now with a single continuous phase. That changes the seeding shape:

mode = onDemand → the population is a throwaway set (fresh, never persisted / revived). Reference = last canonical session's open.
BRING_UP is sync here too, and the wait is wanted: it is the on-demand user's "preparing your simulation" time. The user should enter a populated market.
Small book-formation window, absorbed within the prep wait. Because there is no pre-open lead to form the book in, on-demand adds a brief window after the sync BRING_UP completes: let agents (and any liquidity) trade for a few seconds so the user drops into a moving, populated book rather than an empty one filling in live. This mirrors canonical's internal-open → user-facing-open gap, just tiny, and costs nothing perceptually since the user is already on the prep screen.

Sequence (conceptual — exact fusion with the join is the admission stage):

user starts on-demand
  → [registry lock: create on-demand instance record, mark Provisioning; unlock]
  → (per-user goroutine, OFF-LOCK:)
        construct + wire
        BRING_UP(instance, onDemand, instruments)   (SYNC — user is waiting: "preparing…")
        Start(tickInterval)                          (single continuous phase, opens ~now)
        [small formation window: agents/liquidity trade a few seconds → book forms]
     [registry lock: flip Provisioning → Live; unlock]
  → admit the waiting user into the now-populated, moving market
8.1 On-demand liquidity injection — PARKED (in memory)

Whether the market-operator seeds liquidity for on-demand markets is left unresolved for now:

On-demand markets have few participants, so it's unclear the operator is even needed there.
The single canonical operator cannot serve N concurrent on-demand instances (many on-demand markets can be live at once, unlike canonical where no two run simultaneously).
A fresh throwaway operator per on-demand instance is too much work to stand up.

Decision: no dedicated operator seeds on-demand for now. Revisit only if the thin on-demand book turns out to be a real problem in practice.

9. Goroutine & mutex summary
Seeding adds no new concurrency machinery. Both triggers live inside the existing Tier 1 provisioning goroutine (one per bring-up, off-lock, short-lived).
The registry mutex guards only the two instantaneous state transitions (mark Provisioning, flip Live). It is never held across BRING_UP, SeedLiquidity, construct, wire, or Start — THE RULE.
BRING_UP sync ≠ cross-user blocking. The blocking wait lives inside a per-bring-up goroutine (per-market for canonical, per-user for on-demand), so concurrent users never queue behind one another. This is the mechanism that lets on-demand keep the desirable "user waits for a prepared sim" UX without penalizing other users.
10. Parked / deferred (named, not designed)
Sync-BRING_UP failure / timeout path. Because on-demand's session-start blocks on a sync BRING_UP, a failed or hung population leaves the user's spin-up waiting. Needs a timeout + a failure behavior (what the on-demand user sees if population doesn't come up). Flagged; not designed here. (Argues for "sync + timeout," not for async.)
On-demand liquidity injection. §8.1 — no operator seeds on-demand for now; revisit if the thin book bites.
The liquidity shape itself. §3.2 — the tapered ladder is a deliberately-bad demo placeholder to be replaced.
Whether L2 needs the instrument list. BRING_UP forwards it as a safety param; remove if unused.
11. Decision log (Tier 2 — Seeding)
#	Decision	Status
S1.1	Seeding = population (BRING_UP → L2) + liquidity (SeedLiquidity → market-operator); L7 only triggers, never seeds itself	LOCKED
S1.2	Liquidity is a separate market-operator participant (own broker account, unlimited balance, all canonical markets); L7 is NOT a participant	LOCKED
S1.3	Timing: internal continuous-open (real) vs user-facing open (UI gate only, not modeled in L7); populate during pre-open PhaseClosed, place orders after internal-open	LOCKED
S1.4	BRING_UP(market, mode, instruments+data) — L7 passes market+mode+instruments; L2 owns count/profile/store/seed (Option B)	LOCKED
S1.5	BRING_UP is sync (blocks till populated); no cross-user blocking (per-bring-up goroutine); on-demand wants the wait	LOCKED
S1.6	SeedLiquidity(marketID, instruments+meta+reference) — liquidity metadata in the one market config file, forwarded as params	LOCKED
S1.7	SeedLiquidity is fire-and-trust — its effect is inherently post-open, nothing to block on pre-open	LOCKED
S1.8	Liquidity shape = tapered ladder {levels, spacing_pct, near_qty, taper}, per-instrument, symmetric — deliberately bad demo placeholder	LOCKED (placeholder)
S1.9	Trigger ordering: independent; BRING_UP first by convention only	LOCKED
S1.10	Provisioning → Live (Option 1): Live = constructed+populated+started = admittable; flip right after Start; "tradeable" is not L7 state	LOCKED
S1.11	Both triggers live in the Tier 1 provisioning goroutine, off-lock; mutex only for the two state flips	LOCKED
S1.12	On-demand: full canonical instrument set, mode=onDemand throwaway population, small book-formation window absorbed within the prep wait	LOCKED
S1.13	On-demand liquidity injection	PARKED
S1.14	Sync-BRING_UP failure/timeout behavior	PARKED

End of Tier 2 — Seeding. Next stage: admission (admit-a-user flow, session-start command, market-data-at-admission reconciliation, identity-on-reconnect, HTTP surface).