Layer 7 — Tier 2, Stage 2: Admission

What this doc covers. How L7 puts a specific human into a market — the flow that runs when a person clicks "join canonical" or "start on-demand" and ends with them wired in, able to see the market and trade. Seeding (the prior stage) makes a market ready; admission is the first place a real person enters the picture, so it is where L7's registry gets a Session, where per-user account/identity/channel wiring happens, and where the money gate for on-demand lives.

Status. The two admit flows (canonical and on-demand) are designed and locked. Two admission-stage items remain open and are NOT covered here: the HTTP surface (the external endpoints and what they carry / return) and identity-on-reconnect. Both are named at the end as next work.

Scope discipline. L7 is money-blind and stays a pure orchestrator. It never reads a balance, never moves money, never handles an amount. Every money touch (account creation, the qualify check, the fund move, the ephemeral snapshot) is the broker's; L7 only asks and triggers.

Builds on.

LAYER7_TIER0_REGISTRY_AND_IDENTITY.md — the Session/Registry model, the SessionState enum (Starting/Active/…), the mutex + THE RULE, strict XOR, EnsureAccount, ParticipantID minting, T0.12 (on-demand gate reads the durable account directly).
LAYER7_TIER1_MARKET_CONSTRUCTION_AND_WIRING.md — the per-session channel handoff and the provisioning goroutine.
LAYER7_TIER2_SEEDING.md — the per-user spin-up + seed goroutine that on-demand admission reuses (construct → wire → sync BRING_UP → Start → formation window).
1. The two flows share a spine

Both admit flows share the same core: ensure the person has an account, enforce one live session, record them in the registry with a minted participant id, wire them to the market, return "joined." On-demand wraps that spine in two extra things:

A money gate up front (can this person afford to practice), and
A fused spin-up — there is no already-running market to join; starting on-demand creates the market (the seeding stage's per-user goroutine).

Canonical is designed first as the base; on-demand is the wrapper.

2. Canonical admission

By the time a person joins, the canonical market is already up and trading (agents + liquidity, from the seeding stage). Admission is the per-person wiring.

2.1 The steps, in order
Ensure account. L7 calls the broker's EnsureAccount(userID); the broker finds or creates the account and returns the handle. L7 never sees a balance — only the handle. (T0.)
XOR check. One live session per person. If they already have one (canonical or on-demand), reject — "finish your current session first." (T0.)
Record + mint. L7 creates the Session record (user, market, account handle) and mints the person's ParticipantID (the opaque tag the exchange uses for self-trade prevention). (T0.)
Wire to the market. L7 hands the market's channels + this person's identity to the broker (orders flow, fills return) and to the market-data side (the person's screen gets the live feed). Per the (b) handoff — see §2.3.
Return "joined." The person is connected — sees the chart, can place orders.
2.2 The lock / goroutine pattern

A join mixes fast registry touches (XOR check, record the session) with a slow call-out (EnsureAccount, wiring). Per THE RULE, the lock is never held across a call to another service. So the join is split:

lock → XOR check + create Session in `Starting` state (this IS the reservation) → unlock   (microseconds)
(off-lock:) EnsureAccount → wire broker + market-data                                      (slow)
   ├─ success → lock → flip Session `Starting` → `Active` → unlock → return "joined"       (microseconds)
   └─ any failure → lock → delete the `Starting` Session → unlock → return error           (rollback)
The reservation is the early Starting session record itself — there is no separate "reserved" flag. Creating the Session in Starting state, inside the first locked moment, is the reservation. The XOR check simply looks in byUser for any existing session (including a Starting one). This is why a double-click / race can't produce two sessions: the second request grabs the lock, sees the Starting session already in byUser, and rejects before any slow work starts. (Same idea as Tier 1's Provisioning gate for markets, applied to a person.)
The lock is held only for the two instant touches (reserve; finalize). EnsureAccount and wiring run with the lock free, so one person's join never freezes another's.
2.3 Market-data handoff — model (b) (LOCKED)

Two earlier docs disagreed on L7's role in the live feed. Resolved to (b): L7 hands the market-data side "here's the person, here's the market" and stays out of how the feed reaches the browser. L7 does not hold or manage any fan-out handle; it does not attach browsers to feeds. This matches how L7 treats the broker — hand off, stay out of internals. (This supersedes Tier 0's wording where L7 retained a fan-out and bound each WS connection.)

2.4 Rollback (LOCKED)

If any step after the reserve fails (EnsureAccount, or the broker/market-data wiring), L7 undoes only what L7 created:

Re-acquire the lock → delete the Starting session (which frees the XOR slot in byUser) → release → return an error to the user.
The account is NOT rolled back. EnsureAccount is idempotent and the account is durable, so a leftover account is harmless — the next join finds it. L7 only undoes the session record it created.

One path, no branches. The reason rollback matters: a stuck Starting session would otherwise sit in byUser and block that person from ever joining again (XOR would keep rejecting them).

2.5 Join arriving while the market is still Provisioning

No special case. A join that arrives before the market is Live is handled exactly like any user who shows up before the market is up — the same behavior applies. (There is no distinct "join racing a spin-up" path.)

3. On-demand admission

On-demand admission = the canonical spine, wrapped in a money gate and a fused spin-up (there is no running market to join — starting on-demand creates it, via the seeding stage's per-user goroutine).

3.1 The flow, end to end
user starts on-demand
  → EnsureAccount (broker)                                            [same as canonical]
  → XOR check                                                         [same as canonical]
  → GATE: ask broker "does this person qualify to start on-demand?"
       ├─ NO  → return "insufficient funds — move money, then retry" → STOP
       └─ YES → seal ephemeral account (trigger, no amount)
              → spin up + seed the market  (seeding stage's per-user goroutine)
              → wire the person  (same wiring + rollback as canonical §2)
              → return "joined"

The gate sits fully in front of spin-up: a person who fails the gate wastes nothing — no market constructed, no agents populated. Fail fast.

3.2 The gate (LOCKED)
L7 is money-blind, so it does not know the threshold. L7 asks the broker "does this person qualify?" and the broker answers yes/no. The broker checks the person's trading-account balance ≥ the on-demand minimum; the minimum is the broker's/economy's number, never L7's.
Source: the broker reads the durable account directly (T0.12) — the person is not canonical-live (XOR), so there is no in-memory account on a broker shard to consult.
L7 handles no figure — only the yes/no.
3.3 The fund move (pull) — NOT in L7's flow (LOCKED)

If the gate says NO, the person needs to move money (bank → trading) to qualify. L7 is not in this path at all:

The move is the user talking to the broker directly — a plain HTTP call to the broker (the broker is one, so there is no "which broker" ambiguity), the user acting as a participant.
On a gate-fail, L7 simply returns "insufficient funds — move money, then retry" and stops. The user moves the money themselves (user ↔ broker), then retries start-on-demand from scratch. On the retry, the gate passes.
So L7 never orchestrates a pull. There is no "pull → settle → seal" sequence inside L7. The fund move happens between two attempts, entirely between user and broker.

This is what collapses the old money choreography: the "settle-before-seal" ordering concern disappears, because by the time a retry's gate passes, the moved money is already settled (it was a completed broker call before the retry even started). L7 has nothing to sequence.

3.4 The seal (LOCKED)

When the gate says YES, the practice session needs an isolated throwaway wallet to trade against, so practicing never touches the person's real account.

L7 triggers the seal with no amount — it says "seal an ephemeral account for this on-demand session," and the broker snapshots the figure itself. L7 stays money-blind (it only ever got a yes/no from the gate, never a balance, so it cannot pass a number).
The sealed amount = the person's current trading-account balance at the moment of sealing — not the minimum, not a fixed figure. Whatever is in trading right now (guaranteed ≥ the on-demand minimum by the gate) is what they practice with.
Example: minimum ₹10k, user has ₹35k in trading → gate passes, they practice with the full ₹35k. User has ₹6k → gate fails, they top up to ₹15k and retry → practice with ₹15k.
The ephemeral account is isolated and never persisted (PersistOnClose=false, via the broker's ondemand_session_load flow). Nothing about the practice session reflects back to the durable account.
No timing wrinkle: because L7 isn't in the fund-move path, the trading balance is already settled before the qualifying attempt starts, so the snapshot at gate-pass is a stable, real figure.
3.5 Spin-up + wiring

Once sealed, on-demand reuses locked machinery:

Spin up + seed = the seeding stage's per-user goroutine: construct → wire → sync BRING_UP (throwaway population, the user's "preparing your simulation" wait) → Start (single continuous phase, opens ~now) → small book-formation window. (Seeding stage.)
Wire the person + rollback = identical to canonical §2.2–2.4.
4. What A + B dissolved (why on-demand is smaller than it looked)

The on-demand money choreography originally had four open pieces (gate, pull, settle, seal). Two decisions collapsed most of it:

Gate = ask-broker-yes/no → L7 never holds the threshold or the balance.
Pull = user↔broker directly, then retry → L7 is never in the money-move path, so the "settle-before-seal" ordering concern vanishes (money is already settled at retry), and the seal is a no-amount trigger.

Net: on-demand admission = canonical spine + a yes/no gate + a no-amount seal trigger + the already-locked spin-up/seed/wiring. L7 orchestrates a money-sensitive flow while touching zero money.

5. Decision log (Tier 2 — Admission)
#	Decision	Status
A2.1	Canonical admit spine: EnsureAccount → XOR → record+mint → wire → joined	LOCKED
A2.2	Lock pattern: reserve (lock, XOR + create Starting session) → release → off-lock work → finalize (flip Active)	LOCKED
A2.3	The reservation IS the early Starting session record; no separate reserved flag; XOR checks byUser for any session	LOCKED
A2.4	Market-data handoff = model (b): L7 hands person+market to market-data, holds no fan-out, stays out of feed delivery (supersedes T0 fan-out wording)	LOCKED
A2.5	Rollback: any post-reserve failure → delete the Starting session (frees byUser) → return error; account NOT rolled back (idempotent, durable)	LOCKED
A2.6	Join during Provisioning = same as any user arriving before the market is up; no special case	LOCKED
A2.7	On-demand flow: EnsureAccount → XOR → gate → (NO: bounce+stop / YES: seal → spin-up+seed → wire → joined)	LOCKED
A2.8	Gate = L7 asks broker "qualify?", broker answers yes/no (checks trading ≥ on-demand minimum, reads durable account per T0.12); L7 money-blind	LOCKED
A2.9	Gate sits fully in front of spin-up (fail fast, waste nothing)	LOCKED
A2.10	Fund move (pull) = user↔broker directly (HTTP to the one broker); L7 not in path; on fail L7 bounces with message, user moves money + retries from scratch	LOCKED
A2.11	Settle-before-seal concern dissolved by the retry model (money already settled when a retry's gate passes)	LOCKED
A2.12	Seal = L7 triggers "seal ephemeral account" with NO amount; broker snapshots the figure	LOCKED
A2.13	Sealed amount = current trading balance at seal (≥ minimum, guaranteed by gate); ephemeral, PersistOnClose=false, via ondemand_session_load	LOCKED
6. Still open in the admission stage (NOT designed here)
The HTTP surface. L7's external door — the endpoints (join canonical / start on-demand / leave / end) and what each carries in and returns. This is where the loose ends land: what a successful "joined" returns to the frontend (initial market state vs bare success), the gate-fail "insufficient funds, retry" response, and the leave/end exit paths (join is designed for both modes; the exit is not, and it touches Tier 3 teardown — a scoping call on how much to define now vs defer).
Identity-on-reconnect. A reconnecting browser must present a token so L7 finds its membership in memory and re-binds (reconnect ≈ a lightweight re-admit). Open call: design it, or defer for beta.

Elsewhere (not this stage): the session-start command struct → the broker's Tier 3 (jointly owned); teardown / reap ordering → Tier 3.