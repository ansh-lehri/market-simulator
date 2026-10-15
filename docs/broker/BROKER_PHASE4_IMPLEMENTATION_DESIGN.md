Broker — Phase 4 Implementation Design (Exchange Wire Seam — Points 1 & 2)

Scope: the exchange-seam half of Phase 4 — the broker↔exchange order loop. Point 1 is the punch path (the broker's outbound door: writing an InboundMessage onto a session's exchange inbound edge). Point 2 is the settlement consumer (the inbound door: draining the exchange's private result stream and routing each result to the owning account's shard). Together they open and close the full order round-trip across the exchange boundary.

Status: Points 1 & 2 LOCKED (design + source-verified in this session). Points 3, 4, 5 — DEFERRED (not in this doc). See §6.

Where it sits: after Phase 0 / Tier 0 (the funnel — BROKER_TIER0_IMPLEMENTATION_DESIGN.md), Phase 1 (wiring root & durable-store layer — BROKER_PHASE1_IMPLEMENTATION_DESIGN.md), Phase 2 (session & account lifecycle — BROKER_PHASE2_IMPLEMENTATION_DESIGN.md), and Phase 3 (bank transfer — BROKER_PHASE3_IMPLEMENTATION_DESIGN.md). Phase 4 is the first phase that physically connects the broker to the exchange — until now the broker was a self-contained account machine. It exposes no external surface; it is internal wiring that Phase 5 (orders) sits on top of.

Authority:

Punch (Point 1): Exchange_Broker_Contract (the wire-seam contract, §3) + BROKER_ORDER_LIFECYCLE_intake_punch_settle.md (§2, §3 — the punch as done inside the Phase-5 handlers).
Settlement (Point 2): the Tier-0 result-ingress decision (BROKER_TIER0_IMPLEMENTATION_DESIGN.md Point 3) + BROKER_ORDER_LIFECYCLE_intake_punch_settle.md (§4.1, §6).
Not backend_stream_consumer_design.md — that is the Redis chat-pipeline consumer (Event Router / PEL recovery / narrative assembly over outbound:{session_id}), a different subsystem. Called out so it isn't mistaken for the settlement consumer.

The two points:

The punch path — s.punch(sess, msg), the shard primitive that delivers a built contract.InboundMessage onto sess.binding.ExchangeInbound. Pure transport; persists nothing.
The settlement consumer — one goroutine per exchange instance that drains exchange.Private (contract.EngineResult), resolves the owning AccountID from the echoed OrderID, and routes an ApplyResult push-command to that shard. Pure transport; persists nothing.

Read rule: every type / signature / field below is grounded in the real contract, exchange, and economy source and the locked broker design docs. Reused machinery (route, the funnel, the SessionBinding, ApplyResult, hashAccount) is cited to where it was built.

0. Orientation — what these two points are, and what they reuse

Phase 4's exchange seam has three channel edges between broker and exchange, exposed by the Exchange struct (exchange/wire/exchange.go, whose own comment reads "caller writes to Inbound and reads from Public / Private"):

Inbound chan<- contract.InboundMessage — broker writes (punch out). → Point 1.
Private <-chan contract.EngineResult — broker reads (per-account settlement). → Point 2.
Public <-chan emit.TapeEvent — broker reads (market-wide tape). → Point 3 (deferred).

Points 1 & 2 are the order loop: a submit/cancel leaves on Inbound, and the exchange's answer (fill / rest / reject / cancel-ack) returns asynchronously on Private, correlated by OrderID + BrokerID. There is no synchronous reply on the punch — the two directions are decoupled channels, not a request/response.

What these two points reuse (they add almost no new machinery):

Tier 0 — route() (the dumb hash(AccountID) % N ingress), the ApplyResult push-command seam + its dispatch case, the ownedSession/SessionBinding shapes, the shard's brokerID.
contract — InboundMessage + FundedOrder + CancelRequest (punch types), EngineResult (settlement type).
exchange — the Inbound/Private edges + the exchange's Stop() lifecycle contract.

The genuinely new code is thin: one send primitive and one drain-and-route goroutine.

1. Point 1 — The punch path
1.1 Persistence question

Nothing. The punch is a channel send — no account state, no Postgres, no Redis. (The Phase-5 handlers that call punch mark intent dirty before punching — OrderTrack, the cancel in-flight flag, L5's reservation — so a crash can't lose an in-flight order. Those marks are Phase 5, not the primitive. Point 1 only needs to be a synchronous send so the caller gets a clean happens-before: mark-dirty → punch → return.)

1.2 Scope — the Phase-4 / Phase-5 line (decision B)

Is: one shard primitive — s.punch(sess, msg) — that performs the disciplined send of an already-built contract.InboundMessage onto sess.binding.ExchangeInbound, owning the send discipline (blocking-send safety, stop-safety, BrokerID stamping).

Isn't (all Phase 5): building the FundedOrder payload from a validated order, the reservation gate (Gate 4), OrderID assignment, OrderTrack creation, and deciding what/when to punch.

The authority doc (BROKER_ORDER_LIFECYCLE_intake_punch_settle.md §3/§4) does the punch as a raw inline channel send inside the Phase-5 handlers (handlePlaceOrder step 6, handleCancel). We chose (B): extract a thin punch primitive the Phase-5 handlers call, rather than (A): leave the sends inline. Rationale: centralizes the BrokerID stamp + the send discipline in one place instead of duplicating them across place/cancel/modify, and gives Phase 4 an actual deliverable (fitting "Phase 4 = the edges"). Cost: a mild deviation from the doc's inline style — logged (P4-D1).

1.3 The primitive
go
// punch delivers a built InboundMessage to the exchange over this session's write-only inbound
// edge. Runs ON the owner goroutine (called from a shard handler). Fire-and-forget: no reply —
// the exchange answers asynchronously on Private (Point 2).
func (s *shard) punch(sess *ownedSession, msg contract.InboundMessage) {
    msg.BrokerID = s.brokerID              // own the invariant: every punch carries THIS broker's id
    sess.binding.ExchangeInbound <- msg    // blocking send onto the exchange's inbound edge
}

Where each piece comes from:

sess.binding.ExchangeInbound — chan<- contract.InboundMessage, a write-only field on the per-session SessionBinding (Tier-0 Point 8), injected by L7 at session-install. The broker never fetches or constructs it — it's on any live session, and punch is only ever called for a live session (the caller already did sess, ok := s.sessions[aid]).
s.brokerID — contract.BrokerID, carried on the shard since Phase 0. Stamped here so no caller can forget it; the exchange echoes it back on the EngineResult for settlement routing (Point 2).
msg — built by the Phase-5 caller: Kind (SubmitOrder/CancelOrder), Instrument (the exchange ingress's routing key, required on the wrapper), and the Submit *FundedOrder / Cancel *CancelRequest payload.
1.4 The three send disciplines (the design content)

1. Blocking send, no timeout — and why that's correct. The edge is unbuffered (make(chan contract.InboundMessage) in the exchange), so <- blocks until the ingress receives. We do not wrap it in a select-with-default (that would silently drop an order) or a timeout (that needs a "what do I do on timeout" policy that is Phase-5 order semantics). A plain blocking send is right because the exchange ingress (exchange/wire/ingress.go) is a dedicated goroutine in a tight for msg := range inbound loop that only hashes Instrument and forwards — so the edge is drained continuously and the send returns immediately in practice. If the exchange's internal pipeline ever backs up, that is the exchange's backpressure to own, not something the punch papers over.

2. Stop-safety by teardown ordering, not a per-punch guard. The exchange contract requires the broker to not punch after the exchange is stopping. Exchange.Stop() closes the inbound edge (step 3 of its ordering), and the source states verbatim: "After Stop returns, sending on Inbound panics. Callers must gate their send loop on their own shutdown signal." We honor this at the lifecycle level, not inside every send: the invariant is "all sessions bound to an exchange instance are torn down before that instance's Stop()." Once a session is torn down, s.sessions[aid] misses and the Phase-5 handler rejects before it ever reaches punch — so a live session can never point at a stopped exchange. The primitive therefore does a bare send with no recover(): a post-Stop punch would mean a real teardown-ordering bug, and it should surface as a panic rather than be swallowed and hidden. Enforcement of the ordering invariant is teardown / lifecycle scope (Phase 2 SweepMarketClose + exchange-instance teardown), a cross-point note — Point 1 depends on it and documents it, it does not re-implement it.

3. Fire-and-forget — no reply channel. This is the key contrast with everything in Phase 3 (Broker.Transfer and the funnel commands are request/reply — ReplyTo chan …, block on select). The punch has no reply: it is a one-way send. The exchange's answer returns later and asynchronously on the Private stream (Point 2), correlated by OrderID + BrokerID. So punch returns nothing; the handler that called it does not wait on the exchange.

1.5 Ingress / egress & cross-layer touch

Ingress: a live *ownedSession + a caller-built contract.InboundMessage. Egress: that message on the exchange's inbound edge (sess.binding.ExchangeInbound) — nothing returned. Touch: the exchange wire seam (write-only) and the shard's brokerID. No L5 call, no Postgres, no Redis, no funnel reply. Runs on: the owner goroutine (single-writer) — punch is always called from inside a shard handler, and the blocking send happens there, safe because the ingress drains immediately.

2. Point 2 — The settlement consumer
2.1 Persistence question

Nothing. The consumer only resolves-and-routes — no account state, no Postgres, no Redis. All settlement mutation (and its mark-dirty) happens in Phase 5's handleApplyResult, on the owning shard. Point 2 is pure inbound transport, symmetric with Point 1's pure outbound transport.

2.2 Scope — the Phase-4 / Phase-5 line

Is: one broker-internal goroutine per exchange instance that drains that instance's Private stream, parses the owning AccountID out of each result's OrderID, and routes an ApplyResult push-command to that account's shard via the funnel.

Isn't (Phase 5, point 3): handleApplyResult — the six-outcome adapter that maps OutcomeMatched / OutcomeMakerFill / OutcomeDiverted / OutcomeRejected / OutcomeCancelled / OutcomeCancelRejected onto L5 Convert / Release / ReleaseExcess. Point 2 gets the result to the shard; Phase 5 settles it.

2.3 The consumer

A *Broker method (it needs b.route), run as one goroutine per wired exchange instance:

go
// runSettlementConsumer drains one exchange instance's private result stream and routes each
// result to its owning shard as an ApplyResult. One goroutine per instance. Terminates cleanly
// when the exchange closes `private` during Stop().
func (b *Broker) runSettlementConsumer(private <-chan contract.EngineResult) {
    for r := range private {                       // ends itself when the exchange closes Private
        aid, ok := accountOf(r.OrderID)            // resolve the account from the echoed OrderID
        if !ok {
            slog.Error("settlement: unroutable OrderID", "orderID", r.OrderID, "outcome", r.Outcome)
            continue                               // invariant violation — skip, keep draining
        }
        b.route(ApplyResult{AccountID: aid, Result: r})   // push command, no reply
    }
}

Where each piece comes from:

private — the exchange instance's Private <-chan contract.EngineResult edge. The consumer takes it as a handle; who starts the goroutine and with which instance's channel is the instance-wiring concern (WireInstance), which lands in Phase 7 — a cross-point note, not Point 2.
b.route — the dumb funnel ingress from Phase 0 (hash(AccountID) % N → shard inbox). ApplyResult is one of the two push-command seams Tier 0 declared; the dispatch case case ApplyResult: s.handleApplyResult(c) is wired, body deferred to Phase 5.
ApplyResult{AccountID, Result} — Tier-0 §3.2. Account() returns AccountID, so route() needs no special-casing.
2.4 The account resolution (the load-bearing part)

The result carries OrderID + BrokerID but no AccountID (confirmed in contract/result.go). Tier 0 solved this by minting OrderID as "<AccountID>:<seq>", so the consumer parses the account straight out — no cross-shard OrderID→AccountID map, no shared state on the hot path.

Parse the account as everything before the last ":", not the first:

go
func accountOf(oid contract.OrderID) (account.AccountID, bool) {
    i := strings.LastIndex(oid, ":")
    if i <= 0 { return "", false }
    return oid[:i], true
}

Rationale: seq is a uint64 (colon-free), so the last ":" is always the account/seq boundary — which keeps the parse correct even if an AccountID ever contains a ":". SplitN(oid, ":", 2)[0] would silently truncate such an account; LastIndex won't. This costs nothing and removes a latent footgun. (Cross-point: the "<AccountID>:<seq>" shape is minted at Phase-5 order intake, which must honor it — Tier-0's OrderID-syntax decision.)

Malformed OrderID (no ":" → !ok): a broker-minted-OrderID invariant violation — a broker bug, not a runtime case. We log at error and skip, continuing to drain, rather than panic. Killing the consumer over one unroutable result would halt all settlement for that market (far worse), and there is genuinely no account to route to, so continuing is the only non-destructive option. It is loud (error log) so a real breakage surfaces immediately.

2.5 Lifecycle, ordering, back-pressure
Self-terminating. The for r := range private loop ends when the exchange closes Private in Stop() (after its wg.Wait). No stop signal, no gate — symmetric with the exchange's own ingress. Tracking the goroutine's exit (a WaitGroup so broker shutdown can wait for settlement to drain) is a lifecycle/infra concern, noted, not Point 2's body.
Per-account order preserved for free. One consumer drains Private in order and routes synchronously, so a single account's results all land on its one shard inbox in arrival order, and the shard settles them serially. (Across two exchange instances an account has two consumers, but that's two different instruments — independent, and per-instrument order is what matters.)
Back-pressure. route() sends onto a shard inbox; a wedged shard back-pressures the Private drain. Same discipline as the punch — shards drain fast, acceptable, not engineered around.
2.6 Ingress / egress & cross-layer touch

Ingress: contract.EngineResults off one exchange instance's Private edge. Egress: ApplyResult push-commands onto owning shards' inboxes — nothing returned. Touch: the exchange wire seam (read-only) and the Phase-0 funnel (route). No L5, no Postgres, no reply channel. Runs on: its own goroutine (one per instance), not a shard owner — it reaches account state only by routing a command in, never touching a shard map directly (single-writer discipline intact).

3. The order loop, consolidated (end to end)
Phase 5 handler (on owning shard)                     ── builds InboundMessage (submit/cancel)
   └─ s.punch(sess, msg)                               ── Point 1
        └─ msg.BrokerID = s.brokerID
        └─ sess.binding.ExchangeInbound <- msg  ───────► exchange Inbound edge (blocking send)

                     ...exchange admits + matches, asynchronously...

exchange Private edge  ──► runSettlementConsumer (one goroutine / instance)   ── Point 2
   for r := range private:
        aid = accountOf(r.OrderID)        // parse "<AccountID>:<seq>"
        route(ApplyResult{aid, r})  ──────► owning shard inbox
                                              └─ case ApplyResult: s.handleApplyResult(c)  ── Phase 5

Out on Inbound, back on Private, correlated by OrderID+BrokerID; two decoupled one-way channels, no synchronous reply. Point 1 opens the loop, Point 2 closes it; Phase 5 fills the handler bodies at both ends.

4. Decisions made (Phase 4 — Points 1 & 2 decision log)
P4-D1 — Punch is a thin shard primitive s.punch(sess, msg) (choice B), consumed by the Phase-5 handlers; it centralizes the send + stamps BrokerID = s.brokerID. Deviates from the authority doc's inline sess.binding.ExchangeInbound <- … in handlePlaceOrder/handleCancel — the sends now route through this one primitive (cross-doc note, §7). LOCKED.
P4-D2 — Blocking send onto the unbuffered edge, no select/timeout. Safe because the exchange ingress (wire/ingress.go) is a dedicated always-draining goroutine. Silently dropping or timing out an order is wrong here. LOCKED.
P4-D3 — Stop-safety by teardown ordering, no per-punch recover. The exchange contract ("callers must gate their send loop on their own shutdown signal") is honored at the lifecycle level: all sessions bound to an exchange instance are torn down before that instance's Stop(), so no live session points at a stopped exchange, and the primitive does a bare send. Enforcement is teardown/lifecycle scope (Phase 2 + instance teardown) — cross-point. Alternative considered, not chosen: give the punch its own shutdown gate — select { case <-s.stopping: return errStopping; case sess.binding.ExchangeInbound <- msg: } — making each send self-guarding without relying on teardown ordering. Rejected for now because (a) it invents a broker-level stopping signal that isn't designed yet, (b) it pushes an error-return-on-shutdown policy onto every Phase-5 caller, and (c) a per-send gate/recover would mask a real teardown-ordering bug rather than expose it. Revisit if the "sessions down before exchange Stop" invariant proves hard to guarantee once teardown is fully designed. LOCKED (chosen approach); alternative recorded.
P4-D4 — Punch is fire-and-forget: no reply. Exchange responses return async on Private (Point 2), correlated by OrderID+BrokerID. Contrast with the request/reply funnel commands. LOCKED.
P4-D5 — The settlement consumer is one broker-internal goroutine per exchange instance, draining Private and routing ApplyResult via the funnel. Self-terminates on the exchange closing Private. Consumes the ApplyResult push-command seam Tier 0 left. LOCKED.
P4-D6 — Account resolved by parsing OrderID at the last ":" ("<AccountID>:<seq>", seq colon-free), per Tier-0's OrderID-syntax decision — no cross-shard lookup. LastIndex over SplitN so an AccountID containing ":" still parses. LOCKED.
P4-D7 — Malformed OrderID → log-and-skip, not panic. A can't-happen broker invariant violation; skipping keeps settlement flowing for the rest of the market, and there is no account to route to anyway. Loud error log so a real breakage surfaces. LOCKED.
P4-D8 — Consumer routes only; no reply, no state. Per-account settlement order is preserved by the single-drain + funnel; the six-outcome mutation is Phase 5's handleApplyResult. LOCKED.
5. Constraints / invariants honored
Single-writer per account — neither point touches shard state directly. Point 1 runs inside a shard handler (already the owner); Point 2 is a separate goroutine that reaches account state only by routing a command in, never by touching a shard map.
Write-only punch edge — the shard can only send on ExchangeInbound, never receive; results come back on the separate Private edge. The two directions never share a channel.
BrokerID/OrderID correlation — every punch stamps BrokerID and carries the broker-minted OrderID; the exchange echoes both, which is exactly what lets the settlement consumer route the result back to the right account with no shared index.
Exchange stop ordering — no punch outlives the bound exchange, enforced by tearing down sessions before the exchange instance's Stop().
Persistence-with-every-functionality — each point opened with its persistence question: both persist nothing (pure transport). The durable/candle persistence lives in the deferred market-data half (§6).
6. Deferred / carried items
Points 3, 4, 5 — the market-data component — DEFERRED to the end of the build. These are one coherent unit: the ingestor (drain exchange.Public → write the cache + persist candles), the CandleStore (the candles table + Insert + RangeRead1m), and the MarketPriceCache (BestBid/BestAsk/LastPrice per instrument, read by Tier-1 market-buy pricing and Tier-4 marks). They consume the exchange's OHLCV/tape and are a more involved sub-system than the order-seam half, so they are taken up as a block later rather than piecemeal here. Grounded starting facts already gathered (for when we return): the exchange emits its own closed 1-min candles on the tape (emit.Candle, continuous, never half-formed); QuoteState carries bid/ask/last with load-bearing Has* flags; canonical instances persist candles (INSERT … ON CONFLICT DO NOTHING), on-demand instances forward-but-never-persist; the ingestor is the single consumer of Public per instance and later also feeds the Phase-7 streaming FanOut.
Instance wiring (WireInstance) — who starts the settlement consumer (and later the ingestor) with a specific instance's Private/Public channels, and the instance's mode, is instance-lifecycle wiring that lands in Phase 7. Points 1 & 2 define the primitive and the consumer, not who spawns them.
Consumer exit tracking — a WaitGroup (or equivalent) so broker shutdown can wait for the settlement consumer(s) to drain after Private closes — a lifecycle/infra detail, not in the body here.
The P4-D3 shutdown-gate alternative — recorded in the decision log; revisit when teardown is fully designed if the ordering invariant proves hard to guarantee.
7. Cross-doc note
P4-D1 supersedes the inline punch in BROKER_ORDER_LIFECYCLE_intake_punch_settle.md §3 (handlePlaceOrder step 6) and §4 (handleCancel): the raw sess.binding.ExchangeInbound <- … sends now route through the s.punch(sess, msg) primitive defined here. Those handlers should build the InboundMessage and call s.punch, rather than send inline. Annotate the order-lifecycle doc to point here.
Point 2 implements the Tier-0 result-ingress decision (BROKER_TIER0_IMPLEMENTATION_DESIGN.md Point 3, "how results resolve the account"): the runSettlementConsumer is the "result-ingress caller" that parses AccountID from OrderID and routes ApplyResult. Not a deviation — the concrete realization of that decision.