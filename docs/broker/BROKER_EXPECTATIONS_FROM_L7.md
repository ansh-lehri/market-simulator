Broker — Expectations from Layer 7 (L7)

What this doc covers. Everything the broker needs from L7 to function, stated as a contract of expectations. It is the mirror image of BROKER_OBLIGATIONS.md (what the exchange expects of the broker): here we enumerate what L7 must hand the broker, when, and in what shape — the session-control and connectivity seam. It exists so that when L7 is actually built, its authors know exactly what the broker is already designed to consume, and so the broker's paused Tier-3 (session lifecycle) can be finished against a real, agreed interface instead of a guessed one.

Status of this seam. Tier 3 (session & account lifecycle) is paused pending L7. The carrying model for L7's wiring is already locked in Tier 0 (SessionBinding co-located on ownedSession); the injection mechanics (the exact session-start/stop commands L7 sends) are deliberately not designed yet, because designing them against a non-existent L7 would repeat the exchange-contract assumption error. This doc pins the expectations so that design can proceed the moment L7 has shape.

Companion docs.

BROKER_CONCURRENCY_AND_ROUTING.md §7 — the locked SessionBinding carrying model (Tier 0).
BROKER_ORDER_LIFECYCLE_intake_punch_settle.md — the punch path that reads sess.binding and sess.participant.
BROKER_RESPONSIBILITIES_AND_PLAN.md — the responsibility list; B9/A7/B11 are the L7-facing items.
BROKER_DESIGN_TRACKER.md — Tier 3 status (paused pending L7).
L5 lifecycle: LAYER_5_2_LIFECYCLE_FLOWS.md — the six flows the broker drives on L7's signals.
Exchange_Broker_Contract.docx — the other seam (exchange side); L7 is separate from it.

The one-line framing. L7 is the connectivity + session-control layer. The broker does not set up connections, own the clock, know countries, or decide when sessions begin — it consumes what L7 provides and does the money/order/account work inside the boundaries L7 hands it. Everything below is a thing the broker is waiting for L7 to provide, not something the broker will build itself.

0. Scope boundary — what is L7's job, not the broker's

Locked, explicit "out of scope for the broker; L7 provides it" decisions (from BROKER_RESPONSIBILITIES_AND_PLAN.md scope + spine):

Connectivity setup — establishing/holding the actual exchange connections. The broker consumes handles; it does not create them.
Auth / login / identity — who a user is, and whether they may act. (Tier 4 also assumes no auth; identity resolution, if it ever exists, is L7's.)
Deciding when a session starts or stops — the trigger is L7's (a user logs in, joins canonical, spins up on-demand, or ends). The broker reacts to L7's signal.
The clock / market hours — canonical runs at country-time, on-demand any time; the exchange owns the session-phase clock. The broker just uses whichever exchange handle L7 bound.
The exchange-instance mapping — which exchange goroutine a given session punches to (canonical shared instance vs a private on-demand instance). L7 owns this mapping and injects it.

The broker's L7-facing responsibilities are the consuming side: B9 (drive the L5 lifecycle flows on L7's signals), A7 (session start/stop actions), B11 (hold the N exchange handles L7 gave and select the right one per punch — selection in scope, setup out).

1. Expectation 1 — Per-session exchange wiring (the SessionBinding)

What the broker needs. For every live session, the broker must know which exchange inbound channel to punch that session's orders to. This is per-session, not per-account: canonical-you punches to the canonical exchange; on-demand-you punches to a private on-demand instance. The broker cannot know this mapping itself — L7 owns it and must inject it at session start.

The exact shape the broker already expects (locked in Tier 0, BROKER_CONCURRENCY_AND_ROUTING.md §7):

go
type SessionBinding struct {
    SessionID       SessionID
    ExchangeID      ExchangeID              // which exchange instance (routing sanity / logging)
    ExchangeInbound chan<- InboundMessage   // the ONE inbound channel this session punches to
}

What L7 must guarantee about it:

One channel per session — ExchangeInbound is the single inbound channel for this session's lifetime. The broker reads it as a plain field on the punch hot path (no lookup, no lock); L7 must ensure it is valid for the whole session.
Correct instance per mode — for a canonical session, the canonical exchange's channel; for an on-demand session, that isolated on-demand instance's channel. The entire canonical-vs-on-demand punch difference is absorbed into which channel L7 injects — the broker's punch logic never branches on mode. This is a load-bearing simplification: L7 getting the channel right is what keeps the broker mode-blind.
Lifetime = session lifetime — injected at session start, valid until the broker tears the session down. The same account across different sessions gets a different binding; L7 must supply a fresh one each session, never assume the account keeps a permanent channel.
ExchangeID for sanity/logging — the broker uses ExchangeInbound to punch; ExchangeID is carried for routing-sanity checks and logging. L7 should populate it consistently with the channel.

How the broker will consume it (already designed): the shard stores ownedSession{account, binding} in s.sessions[accountID] and punches to sess.binding.ExchangeInbound. The broker "carries" the handle as ordinary owned session state; L7 "injects" it by handing it in at session start. The carrying is locked; the injection command is what L7's interface must define (§6).

2. Expectation 2 — The session's participant identity

What the broker needs. Each session has a Participant id — the self-match / self-trade identity used inside the InboundMessage, distinct from the BrokerID. The broker punches with both. It does not mint this; L7 must inject it at session start, alongside the binding.

The broker already expects it here (Tier 0 finalization + Tier 1 punch path):

ownedSession holds participant ParticipantID next to account and binding.
The punch (handlePlaceOrder) reads sess.participant into the FundedOrder.

What L7 must guarantee:

A ParticipantID per session, distinct from BrokerID, stable for the session's lifetime.
Consistency with whatever the exchange expects as the participant/self-match identity (this is the identity the exchange uses for self-trade prevention). L7 sits between and must hand the broker the id the bound exchange instance will recognize.
3. Expectation 3 — The session-start signal (and its payload)

What the broker needs. A signal from L7 that says "start a session for this account, of this mode, with this wiring." The broker reacts by driving the correct L5 lifecycle flow and installing the session on the owning shard. The broker does not decide to start sessions; L7 triggers them.

The payload the broker expects L7 to provide at start (assembled from the locked consuming design):

AccountID — whose session.
Mode — canonical vs on-demand (this selects which L5 flow runs; see §5).
SessionBinding — the per-session exchange wiring (§1).
ParticipantID — the session's self-match identity (§2).
SessionID — the session's id (also on the binding).

What the broker will do with it (B9 + A7, to be designed in Tier 3):

Route to the owning shard (hash(AccountID) % N) — because installing the account must happen on the shard goroutine that will own it (single-writer; even loading is a shard-serialized op).
Drive the correct L5 flow (session_load for canonical, ondemand_session_load for on-demand).
Install ownedSession{account, binding, participant} into s.sessions[AccountID].
Resume the shard's nextOrderID counter past the highest reloaded OrderID (so a reload never reissues an id) — a broker-owned detail, but it depends on L7 having triggered the load.

The open mechanic (what L7's interface must nail down): the exact shape of this start signal — is it a typed command into the funnel processed on the owning shard (the broker's strong lean, since install must be shard-serialized), or something else? This is the injection mechanic deliberately left for Tier 3, to be designed once L7 exists. The broker expects it to be a per-account, shard-routable start command carrying the payload above.

4. Expectation 4 — The session-stop signal (and teardown ordering)

What the broker needs. A signal from L7 that a session is ending, so the broker can settle and tear down cleanly. Again, L7 decides when; the broker executes the teardown.

What the broker will do on stop (B9, to be designed in Tier 3) — the ordering matters and the broker owns it:

Auto-square any open positions (drive L5 5.4 SettleSessionEnd — flatten to zero; positions are session-scoped and must end flat).
Flush the resulting closed trades (through the write-behind path).
Save or discard per mode: session_save (canonical — assert flat, reconcile the split to one signed number, Postgres UPDATE in one txn, then drop the snapshot) or session_discard (on-demand — drop snapshot + in-memory copy, never write back).
Remove ownedSession (account + binding + participant + any OrderTracks) from the shard.

What L7 must guarantee:

The stop signal identifies the session/account unambiguously.
If L7 needs to know teardown completed (e.g. before releasing the exchange instance for an on-demand session), there should be an acknowledgement back to L7 — flagged as a likely two-way need. The broker expects to signal "session torn down" so L7 can reclaim the on-demand exchange instance. The exact ack shape is part of the interface to define.
5. Expectation 5 — Mode, and the on-demand seeding boundary

What the broker needs. L7 must tell the broker the session mode (canonical vs on-demand), because the two run different L5 flows and — for on-demand — involve a seeding boundary that the broker orchestrates but that depends on L7's framing.

The locked rules the broker will honor (spine items 5 & 6):

One live session per user at a time — canonical XOR on-demand, no switching mid-session. L7 must enforce this at the trigger level (it must not ask the broker to start a second live session for a user who already has one). The broker assumes L7 upholds the one-live-session rule.
On-demand seeding = check-and-snapshot. On-demand start is gated by a real balance check against the live canonical account; the user may pull bank→trading (a real canonical mutation, through the canonical owner) to qualify; the qualifying figure is then snapshotted to seed an isolated on-demand Account (PersistOnClose=false). On-demand changes never reflect back to canonical — not at end, not ever.

What this needs from L7 (the open sequencing question): the on-demand gate involves a canonical mutation (the bank-pull-to-qualify) that must fully settle before the on-demand copy is sealed. If the same user's canonical and on-demand map to the same shard (same AccountID hash), that ordering is naturally serialized — but L7's trigger must present canonical and on-demand consistently so the broker can guarantee the pull-then-seal order. The broker expects L7 to frame the on-demand start such that the canonical account is reachable for the gate/pull step before the on-demand session is sealed. The precise choreography is Tier-3 work, pending L7's shape.

6. Expectation 6 — The exchange-handle set (B11)

What the broker needs. The broker holds N exchange handles L7 provides and routes each punch to the right one for the session. L7 provides the handles; the broker does handle selection, not setup.

Setup (out of scope): creating/holding the actual exchange connections — L7's job.
Selection (in scope): given a session, punch to the right handle. This is already solved by the co-located SessionBinding — the "selection" is just reading sess.binding.ExchangeInbound, which L7 bound at start. So B11's "selection" collapses into Expectation 1: if L7 injects the right channel per session, selection is automatic and branch-free.

What L7 must guarantee: the handles it provides are the valid inbound channels for the exchange instances, and the per-session binding points at the correct one for that session's mode.

7. Expectation 7 — Crash-recovery coordination (deferred, joint)

What the broker needs (eventually). On crash recovery, L5 restores the account split verbatim from Redis (crash_recovery_reload), but the per-session SessionBinding is broker/L7 state, not L5 state — L5 knows nothing about which exchange channel a mid-session account was punching to. So a recovered session needs its binding re-established. Open question the broker cannot answer alone:

Does L7 re-inject the binding on recovery (treat recovery like a fresh start signal), or
Does the broker persist the binding and restore it itself?

Current posture: this is an explicitly deferred, joint L5+L2+L7+broker pass (L5's crash-recovery doc already flags cross-layer coordination as open). The broker expects L7 to participate in defining recovery — most likely by re-injecting bindings on recovery, since the binding is inherently L7's wiring and re-injection matches the "L7 owns the exchange mapping" principle. Not designed now; flagged so it is not forgotten.

8. Summary — the L7→broker contract, at a glance
#	Expectation	What L7 provides	When	The broker consumes it as
1	Per-session exchange wiring	SessionBinding{SessionID, ExchangeID, ExchangeInbound}	session start	sess.binding, read at punch (field, no lock)
2	Participant identity	ParticipantID (≠ BrokerID)	session start	sess.participant, into FundedOrder
3	Session-start signal	{AccountID, mode, binding, participant, SessionID}	on user join/spin-up	shard-routed start command → drive L5 load + install session
4	Session-stop signal	session/account id (+ expect ack back)	on user end	drive auto-square → flush → save/discard → remove session
5	Mode + one-live-session rule	mode flag; enforce canonical-XOR-on-demand	session start	selects L5 flow; on-demand gate/seed choreography
6	Exchange-handle set	the N valid exchange inbound channels	provisioning	B11 selection = read the injected per-session channel
7	Crash-recovery participation	re-inject bindings on recovery (likely)	on recovery	deferred joint pass

The through-line. L7 owns when sessions happen, which exchange each session talks to, and who the participant is. The broker owns what happens to the money and orders inside the session L7 opened. The single most load-bearing expectation is #1 + #2: if L7 injects the right exchange channel and participant id per session, the broker's entire canonical-vs-on-demand distinction becomes free — absorbed into what was injected, never branched on. Everything the broker deferred in Tier 3 is waiting on L7 defining the start/stop command shape (#3/#4) so these injections have a concrete delivery mechanism.

9. What is already locked vs. waiting on L7

Locked (broker side, ready to consume):

The SessionBinding carrying model — co-located on ownedSession, read branch-free at punch (Tier 0).
ownedSession holding account + binding + participant (Tier 0 finalization).
The mode-blind punch path (Tier 1) — reads sess.binding.ExchangeInbound, never branches on mode.
The L5 lifecycle flows the broker will drive (open_account, session_load, ondemand_session_load, session_save, session_discard, crash_recovery_reload) — all specced in L5, verified.

Waiting on L7 (the interface to define, then Tier 3 unblocks):

The exact start/stop command shape L7 sends (the injection mechanics).
The on-demand gate choreography framing (pull-to-qualify then seal ordering).
The teardown acknowledgement back to L7 (so it can reclaim on-demand exchange instances).
Crash-recovery binding re-injection (the joint pass).

Until L7 exists at least in skeleton, these stay open by design — pinning them here so that when L7 lands, the broker's Tier 3 can be built against an agreed contract instead of a guess.