Layer 7 — Tier 3: Teardown

What this doc covers. How L7 winds a market and its sessions down — the inverse of everything Tiers 1–2 built. It covers the three teardown shapes a user's actions produce, the timers that drive them, and the ordering L7 enforces so nothing is lost, corrupted, or leaked. Teardown is the one tier whose cross-component ordering is L7's own invention — no single component states the full sequence, because each knows only its local cleanup.

Status. All three shapes are designed and locked. Two items remain carried, not designed here: the session-stop command struct (the broker's Tier 3 — jointly owned; L7 owns what rides in it + when) and identity-on-reconnect internals (deferred to after this tier, per decision).

Scope discipline. L7 stays a pure orchestrator and money-blind. It triggers and sequences; it never settles money, never places orders, never reads a balance. Every money touch (save, discard, the end-of-day auto-square) is the broker's.

Builds on.

LAYER7_TIER0_REGISTRY_AND_IDENTITY.md — the registry, the mutex + THE RULE, the Market/Session states, the reserved on-demand reap timer.
LAYER7_TIER1_MARKET_CONSTRUCTION_AND_WIRING.md — Stop() semantics (it owns edge-closing; the tape closing is market-data's self-clean signal), the per-canonical- market timer goroutine, the retained closeAt.
LAYER7_TIER2_* — admission (what teardown reverses), the model-(b) market-data handoff, the seeding actors (L2 population, the market-operator).
0. The three shapes

Teardown is not one operation. There are exactly three, because there are exactly three things a user's world can do at end-of-life:

Shape	Trigger	Market dies?	Data	Waits on
A — canonical leave	user exits one session	no (keeps running)	positions carry to end-of-day sweep	nothing
B — canonical close	L7 timer at closeAt	yes	save (persist)	broker save-acks
C — on-demand teardown	user leave or duration timeout	yes	discard (nothing persists)	broker discard-ack

Cross-shape invariants (hold for all three):

exch.Stop() is always the last act. Every path ends there (B and C); A never calls it.
byUser is always freed when a session is dropped, so the user can start again. Rejoin-after-leave is simply a fresh admission (Tier 2), not a special path.
The state flip (Live → TearingDown for a market, Active → Stopping for a session) is always the idempotency + race guard — a second trigger sees the flipped state and bails, so teardown runs once.
L2 is fire-and-forget. L7 triggers the agent wind-down and never waits on it or receives any confirmation. Only the broker is waited on.
Teardown always makes forward progress to Stop(). It never wedges on a missing ack — a leaked live exchange is the worst outcome — so waits are bounded by a timeout that logs and proceeds.
Auto-square is the broker's, not L7's. The broker flattens all canonical positions on its own trigger, shortly before close, while the market is still in an accepting phase. L7 never triggers, sequences, or waits on squaring. By the time L7's teardown fires, positions are already flat — so L7 teardown is pure state-save/discard + stop, with no trade-settlement to wait on and no PhaseClosed-vs-orders conflict.
1. Shape A — canonical per-user leave

One person exits; the market keeps living for everyone else. The lightest shape — no broker signal, no Stop(), no reap. Essentially "undo one admission," L7-side only.

1.1 Flow
Flip the session Active → Stopping. Lock → flip (bail if not Active) → unlock. Instant. The flip guards double-leave (a second exit for the same session sees Stopping and bails). The market is not touched — it stays Live.
Unwire the market-data feed. Off-lock. L7 signals the market-data side "this user's session is ending" and stays out of the mechanics (model-(b) handoff). Mirror of how the feed was wired at admission.
Drop from the registry. Lock → remove the session from sessions, from the market's members, and from byUser (frees the XOR slot) → unlock. Instant. The market record stays, minus this one member.
1.2 What Shape A does NOT do (and why)
No broker signal. On a mid-session leave L7 tells the broker nothing. The user's positions are not squared or saved on leave — they carry to the broker's normal end-of-day auto-square sweep, which flattens that account whether or not a human is still attached. The broker keeps the account live and humming until then. (This rests on the broker's close sweep tearing down all canonical accounts regardless of attachment — a broker-side assumption, flagged, not L7's to solve.)
No Stop(), no reap, no agent wind-down. The market and every other participant are untouched; the market doesn't notice the leaver is gone.
1.3 Concurrency

Same two-grab lock pattern as everywhere: lock for the flip (1), off-lock for the feed unwire (2), lock for the removal (3). With no broker call, Shape A is nearly synchronous — the only off-lock work is the market-data unwire signal; the goroutine is lighter than B/C.

2. Shape B — canonical whole-market close

The scheduled close; everything winds down and is persisted. This is the fullest shape.

2.1 Trigger

L7's own per-market timer goroutine, armed off the retained closeAt (not an exchange signal). It is the same goroutine that fired the market's pre-open bring-up (see §4.1) — teardown owned by L7, not reactive to the exchange.

2.2 Flow
Flip Live → TearingDown. Lock → flip (bail if not Live) → unlock. Instant. From this moment the admission path rejects new joins ("market closing"). Doubles as the run-once guard. Then spin off the teardown goroutine — all slow work runs there, off-lock.
Fire the wind-downs, in parallel, block on none:
Human sessions: loop market.members; for each, send the broker a stop signal, mode = canonical → save. Fire all of them, then wait (Step 3) — not one-by-one-blocking.
Agents: one trigger to L2 — "wind down this market's population" (flatten + persist), the mirror of BRING_UP. Fire-and-forget — L7 does not wait on L2 or receive confirmation.
Operator: the market-operator holds positions in this market but is not triggered — it dies when the exchange stops (§2.4). It receives no wind-down signal.
Barrier — wait on the broker save-acks only. L7 knows it sent N; it receives acks on one channel and counts to N. This is the gate before Stop(): the exchange must not die mid-save. Bounded by a timeout: on timeout, or on a failed-save ack, L7 logs which sessions didn't confirm and proceeds anyway — forward progress always wins (a leaked live exchange is worse than a rare unsaved final state, and positions are already flat so the loss is small).
exch.Stop() — off-lock; blocks until the instance is truly down (it closes inbound, waits for shards to stop emitting, then closes the tape). The tape closing is market-data's self-clean signal, so L7 does nothing separate for market-data. Unconditionally last.
Reap + drop registry. After Stop() completes, lock → remove every member session from sessions, each member's byUser entry (frees their XOR slot), and the market from markets → unlock. The instance handle drops out of scope with the market record.
2.3 Independence + the barrier

Humans, agents, and the (absent) operator are independent actors with separate state — no ordering between them. L7 fires the human stops and the L2 wind-down together; the only hard gate is the broker save-acks (L2 isn't in the gate; the operator isn't). So "parallel wind-downs, barrier on broker acks, then Stop()" is the shape.

2.4 Consequences named (not problems for the demo)
L2 may still be mid-persist when Stop() fires (fire-and-forget). Safe, because L2's wind-down is flatten-in-memory + write-to-its-own-DB — it does not need the live exchange (no orders).
The operator dies with the exchange. It held positions with no persistence of its own; Stop() ending the instance is what ends it. This is the "operator trigger out" decision cashing out.
2.5 Concurrency

Two brief lock acquisitions bookend an all-off-lock middle: lock for the flip (Step 1), nothing held across the stops / ack-wait / Stop() (Steps 2–4, all call-outs and waits — THE RULE), lock for the reap (Step 5).

3. Shape C — on-demand teardown

The single user leaving is the teardown — the market is theirs. Structurally Shape B with discard semantics and one session instead of N, plus a second trigger.

3.1 Triggers — two, converging (new to C)
User leaves — the exit endpoint fires for an on-demand session.
Duration timeout — the per-session reap timer fires (§4.2).

Both land in the same teardown flow and can race (user exits at the same instant the timer fires). The TearingDown flip resolves it: whoever takes the lock first flips and runs teardown; the other sees TearingDown (or a gone market) and no-ops. A reap timer firing after a manual leave is therefore harmless — teardown is idempotent by the flip.

3.2 Flow
Flip Live → TearingDown. Lock → flip (bail if not Live) → unlock. Resolves the leave-vs-timeout race; run-once guard. Spin off the teardown goroutine.
Fire the wind-downs (parallel, block on none):
One broker stop signal for the single session, mode = onDemand → discard (the ephemeral account is dropped, never written to the durable account; on-demand OHLCV gone). L7 waits on this one ack.
L2 agent wind-down, mode = onDemand → discard the throwaway population (no persist). Fire-and-forget.
No operator — on-demand was never seeded by the operator (on-demand liquidity injection is parked/absent), so there is nothing to wind down.
Barrier — wait on the one broker discard-ack (timeout → log + proceed; same forward-progress rule as B). Trivial barrier, count of 1.
exch.Stop() — off-lock, blocks until down; tape closes → market-data self-cleans (nothing was persisted anyway). Last act.
Reap + drop registry. Lock → remove the session from sessions, the market from markets, and the user's byUser entry (frees XOR so they can start another session) → unlock. Instance handle drops out of scope.
3.3 Concurrency

Identical shape to B — flip under lock, all wind-down/ack/Stop() off-lock, reap under lock. Just N = 1.

4. The timers
4.1 Canonical — one long-lived goroutine per market

Per Tier 1: one timer goroutine per canonical market (not per session — canonical is scheduled and shared; a canonical user has no timer, only the market does). The same goroutine drives both ends of the market's day, sequentially (open always precedes close):

compute next event time (from config session hours + preopen_lead)
  → sleep until preopen_lead before open → fire bring-up (Tier 1/2)
  → sleep until closeAt → fire Shape B teardown
  → sleep until tomorrow's pre-open → …

So: N canonical markets → N timer goroutines, each cycling sleep→bring-up→sleep→teardown→sleep. The instants come from the config (today's absolute open/close in the market's timezone). One goroutine holding a market's whole lifecycle keeps it in one place.

4.2 On-demand — one short-lived goroutine per session (Option 1)

On-demand is per-user, and many sessions can be live at once, so timing is per session, not per market. When an on-demand session spins up, L7 spawns a goroutine that sleeps the configured on-demand session lifespan (ondemand.duration, whatever the config/rulebook sets it to — not a fixed number) then fires Shape C teardown for that session. When the session ends by either path (leave or this timer), the goroutine goes away.

Chosen: Option 1 (per-session goroutine), not a shared reaper. Rationale mirrors the Tier 0 mutex-over-event-loop call: concurrent on-demand count is small for beta, goroutines are cheap, and per-session self-reaping needs no shared expiry structure and no bookkeeping when sessions come and go. A shared reaper goroutine (one goroutine holding all expiry times, waking for the next due) is the scale answer — swap to it only if concurrent on-demand ever grows large; premature now.

Race-safety: the reap timer and a user leave both call teardown; the TearingDown flip (§3.1) makes a timer firing on an already-torn-down session a safe no-op. The timer firing must be idempotent — it is, by the flip.

5. Decision log (Tier 3)
#	Decision	Status
T3.1	Three teardown shapes: A canonical leave, B canonical close, C on-demand	LOCKED
T3.2	exch.Stop() is always the last act, every shape	LOCKED
T3.3	Auto-square is the broker's own pre-close job; L7 never triggers/sequences/waits on it; L7 teardown = save/discard + stop, no trade-settlement	LOCKED
T3.4	Shape A: flip session Stopping → unwire market-data → drop session + free byUser; no broker signal, no Stop(), no reap	LOCKED
T3.5	Shape A leaver's positions carry to the broker's end-of-day sweep (broker keeps the account live; broker-side assumption)	LOCKED
T3.6	Shape B trigger = L7's own per-market timer at closeAt (not exchange-signalled)	LOCKED
T3.7	Shape B: flip TearingDown → fire N broker stops (save) + L2 wind-down → barrier on broker acks → Stop() → reap	LOCKED
T3.8	Wind-downs are parallel/independent; the only hard gate is the broker acks; L2 fire-and-forget; operator not triggered (dies with Stop())	LOCKED
T3.9	Ack barrier bounded by timeout; on timeout or failed-save ack → log + proceed to Stop() (forward progress always)	LOCKED
T3.10	Shape C triggers = user leave OR duration timeout, converging; TearingDown flip resolves the race (idempotent)	LOCKED
T3.11	Shape C: flip → 1 broker stop (discard) + L2 wind-down (discard) → barrier on the 1 ack → Stop() → reap; no operator	LOCKED
T3.12	byUser freed on every session drop (all shapes); rejoin-after-leave = fresh admission	LOCKED
T3.13	Canonical timer = one long-lived goroutine per market, driving both open and close	LOCKED
T3.14	On-demand reap = one short-lived goroutine per session (Option 1), duration-only, lifespan from ondemand.duration config; shared reaper is the deferred scale alternative	LOCKED
6. Carried / deferred (named, not designed)
Session-stop command struct. The exact struct the broker consumes for a stop signal is the broker's Tier 3 (paused pending L7, jointly owned). L7 owns what rides in it (which session; mode → save vs discard) and when it's sent; the broker owns the struct shape and its delivery onto the owning shard. Mirror of the session-start command.
Identity-on-reconnect internals. Deferred to after Tier 3 (per decision). Reconnect ≈ a lightweight re-admit: the reconnecting client re-sends its userID packet, L7 finds the still-live session in byUser and re-wires. The HTTP-surface identity flow already carries what's needed; only the reconnect endpoint/behavior remains to design.
Broker-side assumptions this tier rests on (flagged, not L7's to solve): the broker keeps a left canonical account live until the end-of-day sweep and squares it regardless of human attachment; L2's wind-down never needs the live exchange.