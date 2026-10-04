Broker — Phase 3 Implementation Design (Bank Transfer)

Scope: the two Phase-3 points — the transport spine that turns a transfer request into a shard command and back (Point 1), and the two branch bodies that actually move the money (Point 2). Together they complete the one consumer endpoint POST /v1/account/transfer end to end.

Status: both points LOCKED (design + source-verified in this session).

Where it sits: after Phase 0 / Tier 0 (the funnel — BROKER_TIER0_IMPLEMENTATION_DESIGN.md), Phase 1 (wiring root & durable-store layer — BROKER_PHASE1_IMPLEMENTATION_DESIGN.md), and Phase 2 (session & account lifecycle — BROKER_PHASE2_IMPLEMENTATION_DESIGN.md, LOCKED). Phase 3 is the first place a user-facing money-moving endpoint is designed end to end.

Authority: BROKER_BANK_TRANSFER.md (both stages LOCKED). Where this doc deviates from its pseudo-code it is a source-grounded correction, flagged inline and in the decision log.

The two points:

The endpoint + Transfer command → shard — the HTTP handler, the Transfer command, the Broker.Transfer entry-point, and the dispatch case. Pure transport; persists nothing.
The two branch bodies — the in-session handleTransfer (MoveCash + MarkSnapshotDirty) and the no-session durable path (Store.DurableTransfer), plus the route-first → durable-on-miss composition and the full status mapping.

Read rule: every type / signature / field below is grounded in the real economy + connectivity source and the locked broker design docs. Reused machinery (route, the reply mechanism, MoveCash, MarkSnapshotDirty, Store.DurableTransfer, get_account_handle) is cited to where it was built.

0. Orientation — what Phase 3 adds, and what it reuses

Phase 3 adds almost no new machinery. The endpoint fans a single request into two mechanically different branches depending on whether the account is live on a shard:

In-session (a live session exists on the owning shard) → an in-memory MoveCash on the owner goroutine, mirrored to persistence via a dirty-mark.
No-session (only the Postgres row exists) → Store.DurableTransfer, the guarded single UPDATE already built in Phase 1, point 4.

What is genuinely new is thin: the HTTP endpoint, the route-first control flow, and the handleTransfer body that fills a seam Tier 0 already left. Everything else is reuse:

Tier 0 — route(), the Command interface, the buffered-1 self-addressed reply, the dispatch type-switch, ErrBrokerTimeout, the ownedSession/SessionBinding shapes.
Phase 1 — Store.DurableTransfer + get_account_handle (the idle branch).
Phase 2 — the s.sessions map (populated at session-install, cleared at teardown); Phase 3 only reads it.
L5 (economy/account) — MoveCash and the Bucket type; persistence — MarkSnapshotDirty.

Neither point adds new runtime behaviour to the funnel — Point 1 is one more caller of it, Point 2 fills a handler body inside it.

1. Point 1 — The endpoint + Transfer command → shard
1.1 Persistence question

Nothing. Point 1 is pure transport: parse a request, carry it to the owning shard, carry the reply back. No account state, no Redis, no Postgres. The money moves in Point 2's branch bodies, each with its own persistence answer (§2.1). The job here is correct delivery + correct reply/timeout handling.

1.2 The Tier-0 reconciliation (finalizing the placeholder)

Tier 0 already declared this command as a placeholder and explicitly deferred its final naming to "when Tier 3 is built" (BROKER_TIER0_IMPLEMENTATION_DESIGN.md §2, decision note). Tier 0 has:

go
type BankMove struct { AccountID AccountID; Dir BankDir; Amount int64; ReplyTo chan BankResult }
// dispatch: case BankMove: c.ReplyTo <- s.handleBankMove(c)   // Tier 3

BROKER_BANK_TRANSFER.md names it Transfer and carries From, To Bucket. Following the authority resolves all three differences:

name BankMove → Transfer, handleBankMove → handleTransfer;
direction field Dir BankDir → From, To account.Bucket (matches the in-session MoveCash, which speaks buckets);
reply type BankResult → TransferResult.

One field is not taken verbatim from the authority: the bank-transfer doc writes the reply field as Reply, but every sibling command uses ReplyTo and the Tier-0 dispatch sends via c.ReplyTo <-. The field stays ReplyTo for funnel consistency — a name match to the machinery, not a design change.

Point 1 therefore finalizes the Tier-0 placeholder and supersedes its §2 BankMove / §4 dispatch case (cross-doc note, §7).

1.3 The Transfer command + TransferResult
go
type Transfer struct {
    AccountID AccountID
    From, To  account.Bucket        // to_bank → (Available, Bank); to_trading → (Bank, Available)
    Amount    int64                 // always > 0 (handler-validated before build)
    ReplyTo   chan TransferResult   // buffered(1) — funnel convention
}
func (c Transfer) Account() AccountID { return c.AccountID }   // routing key for route()

type TransferResult struct {
    Present   bool    // false = account not live on this shard → caller falls to Stage 2
    Available int64   // on success
    Bank      int64   // on success
    Err       error   // e.g. ErrInsufficient (populated by the Point-2 body)
}

account.Bucket is L5's own type (from economy/account), so the command speaks the exact type MoveCash takes — zero conversion at the L5 seam in Point 2.

1.4 The Broker.Transfer entry-point

Mirrors Broker.PlaceOrder (Tier-0 Point 7 / §8.1) exactly — a thin shard round-trip:

go
func (b *Broker) Transfer(ctx context.Context, aid AccountID, from, to account.Bucket, amt int64) (TransferResult, error) {
    reply := make(chan TransferResult, 1)          // fresh, buffered(1) — shard never blocks sending
    b.route(Transfer{AccountID: aid, From: from, To: to, Amount: amt, ReplyTo: reply})
    select {
    case r := <-reply:  return r, nil
    case <-ctx.Done():  return TransferResult{}, ErrBrokerTimeout   // shard still runs later; result GC'd
    }
}

Reuses the funnel wholesale: route() = hash(aid) % N → that shard's inbox; ErrBrokerTimeout is Tier-0's shared timeout sentinel; the buffered(1) reply protects the shard from a caller who already timed out. Nothing new in the funnel — one more caller.

1.5 The dispatch case

Rename the seam Tier 0 already left; body deferred to Point 2:

go
case Transfer:   c.ReplyTo <- s.handleTransfer(c)   // Tier 3 — body is Point 2

Per the Tier-0 dispatch convention (c.ReplyTo <- s.handleX(c)), handleTransfer returns a TransferResult and dispatch sends it — not an internal send as the authority's pseudo-code sketched. Same outcomes, returned instead of pushed.

1.6 The HTTP endpoint

Copies the Tier-4 handler pattern (BROKER_READ_SURFACES_tier4.md §0.1, §0.5, handler-flow): no auth, explicit account_id in the body, handler owns the timeout, error→status.

POST /v1/account/transfer
body: { account_id, direction: "to_bank" | "to_trading", amount, unit }

handler (request goroutine):
  1. ctx := r.Context()                                   // carries client-disconnect
  2. parse body → account_id, direction, amount, unit
  3. validate: account_id present, direction ∈ {to_bank, to_trading}, amount > 0
        → else 400                                        // pre-funnel, Tier-4 §0.5
  4. from, to := bucketsFor(direction)                    // to_bank→(Available,Bank); to_trading→(Bank,Available)
     dir := directionFor(direction)                       // TransferDirection: toBank | toTrading — kept for Stage 2
  5. ctx, cancel = context.WithTimeout(ctx, transferTimeout)  // handler OWNS the timeout
  6. res, err := broker.Transfer(ctx, account_id, from, to, amount)
        err == ErrBrokerTimeout → 504
  7. → hand (res, dir) to the branch/response logic       // §2.3

The handler maps the one direction string into both vocabularies once: (from, to) buckets for the command (live path) and a TransferDirection for the durable call (idle path). transferTimeout comes from broker config — same knob-shape as the Tier-4 readTimeout, not hardcoded.

1.7 Status contract owned by Point 1
400 — missing account_id, illegal direction, or amount ≤ 0 (validated before the funnel).
504 — ErrBrokerTimeout (shard wedged / inbox full past deadline).

Everything else depends on branch outcomes → Point 2 (§2.4).

1.8 Ingress / egress & cross-layer touch

Ingress: the HTTP request (account_id, direction, amount, unit) + broker config (transferTimeout). Egress: a Transfer command onto the owning shard's inbox, and an HTTP response skeleton (400/504 here). Touch: the Tier-0 funnel (route, reply, ErrBrokerTimeout) and account.Bucket (the command's field type). No L5 call, no Postgres, no Redis in Point 1.

2. Point 2 — The two branch bodies
2.1 Persistence question

Both branches move money; their persistence answers are opposite, correctly:

In-session: MoveCash is pure in-memory; the branch then calls MarkSnapshotDirty — a flag, no I/O. The async Redis mirror picks it up on cadence; the eventual session_save writes to Postgres (canonical) or discards (on-demand). No durable write of its own.
No-session: Store.DurableTransfer is one Postgres UPDATE — durable the instant it commits, no Redis, no snapshot (no session ⇒ no crash-window). Built in Phase 1.
2.2 Branch A — handleTransfer (shard, owner goroutine)

Fills the seam Point 1 wired. Returns a TransferResult; dispatch sends it.

handleTransfer(c Transfer) TransferResult:                // ON the owner goroutine
  1. sess, live := s.sessions[c.AccountID]
     if !live:  return TransferResult{Present: false}     // → handler falls to Stage 2 (durable)

  2. err := sess.account.Account.Cash.MoveCash(c.From, c.To, c.Amount)   // L5 op — conserving, in-mem, fails before mutating
     if err != nil:  return TransferResult{Present: true, Err: err}      // e.g. ErrInsufficient — nothing moved

  3. s.persist.MarkSnapshotDirty(sess.binding.SessionID, c.AccountID)     // both args; mark money-state dirty

  4. return TransferResult{Present: true,
         Available: sess.account.Account.Cash.Available,
         Bank:      sess.account.Account.Cash.Bank}

Grounded correctness points:

The account is reached via sess.account.Account.Cash, not sess.account.Cash. The Tier-0 ownedSession holds a LoadedAccount{ Account *account.Account; OwnerType; OwnerID } (refined from the doc's earlier bare *Account), so the raw account is sess.account.Account. The bank-transfer doc's sess.account.Cash shorthand is corrected here.
The mutation sticks. s.sessions is map[AccountID]*ownedSession (pointer-valued, Tier-0 §"What Tier 0 guarantees" pt 5), so sess is a pointer to the real session and MoveCash mutates it in place. The LoadedAccount.Account pointer is a second layer of the same guarantee. (Had the session been held by value, the transfer would have written to a throwaway copy — hence the care.)
MoveCash legal edges + guards, source-verified (economy/account/cash.go): four buckets (Available, Reserved, Settled, Bank); the two bank edges are Available→Bank and Bank→Available; guards amount > 0, source ≥ amount, and it fails before mutating. It touches only the two named buckets — never Reserved — which is why a pull mid-position is fine.
MoveCash must run on the owner — satisfied, we're inside the shard's serial loop.
Three distinct outcomes, and Present:true + Err ≠ Present:false. A live-path failure (insufficient) is Present:true, Err:set — the account was live, so we do not fall to durable; we surface the error (→ 422). Only a genuine !live miss is Present:false (→ durable).
MarkSnapshotDirty is unconditional (mode-blind). On-demand isolation is enforced later at session_save via PersistOnClose=false, not by gating the mark here.
SessionID type seam: MarkSnapshotDirty(sessionID realize.SessionID, accountID account.AccountID); realize.SessionID is a type alias for string (type SessionID = string). The broker's SessionID must be alias-compatible (= string / = realize.SessionID) so sess.binding.SessionID passes with no conversion; otherwise a trivial realize.SessionID(...) cast at the call.
2.3 Branch B — the no-session path + the composition (HTTP handler)

The route-first → durable-on-miss glue lives in the handler, picking up from Point 1's broker.Transfer(...):

res, err := broker.Transfer(ctx, aid, from, to, amount)   // Point 1 (shard round-trip)

  err == ErrBrokerTimeout                    → 504                         // Point 1's timeout

  res.Present == false:                                                    // idle → Stage 2
      avail, bank, dErr := store.DurableTransfer(ctx, aid, dir, amount)    // Phase-1 primitive; takes DIRECTION, not buckets
        dErr == nil             → 200 { avail, bank }
        dErr == ErrInsufficient → 422
        dErr == ErrNoAccount    → 404
        dErr == ErrBadTransfer  → 400   (defensive; direction/amount already validated in Point 1)
        dErr (db unreachable)   → 503 / 500

  res.Present == true && res.Err != nil:                                   // live path failed
        res.Err == ErrInsufficient → 422
        else                       → 500

  res.Present == true && res.Err == nil      → 200 { res.Available, res.Bank }   // live success

Grounded notes:

Store.DurableTransfer (Phase-1 §4.3) signature: DurableTransfer(ctx, accountID, direction TransferDirection, amount int64) (available, bank int64, err error), TransferDirection = toTrading | toBank. It takes the direction the handler retained in Point 1 (step 4) — the two-vocabulary seam we locked (buckets for live MoveCash, direction for the durable UPDATE). It internally reuses get_account_handle as the 0-rows existence probe to split ErrNoAccount from ErrInsufficient — already built, nothing new here.
Response shape is mode-blind. DurableTransfer returns (available, bank) where available == trading_cash off-session (XOR ⇒ no reservations, trading_cash == available, Phase-1 §4.4). So the { available, bank } body is identical whether it came from live Cash.Available/Bank or the durable row.
No 409 is ever emitted (see D8): Present:false is caught internally and routed to durable; the client only ever sees 200 or a Stage-2 error.
2.4 The endpoint's full status contract (consolidated)
Status	When	Owner
200	success — either branch — body { available, bank }	P2
400	bad input (missing account_id, illegal direction, amount ≤ 0); defensive ErrBadTransfer	P1 / P2
422	insufficient funds — both branches, mode-blind (D7)	P2
404	no such durable account (Stage 2 only; the live path is, by definition, live)	P2
504	broker timeout (shard wedged)	P1
500 / 503	internal / DB-unreachable failure	P2
409	never emitted — Present:false is internal → durable (D8)	—
2.5 Edge cases (from the authority, grounded)
Mid-position transfer → allowed; MoveCash never touches Reserved.
Concurrent transfer + order on one account → serialized on the single shard loop; no race.
On-demand (isolated) live session → MoveCash works on the isolated copy, mark-dirty still fires (mode-blind), never reaches Postgres (handled at session_save, PersistOnClose=false).
Durable transfer vs a session starting for the same account → no TOCTOU under XOR + the retry model (Phase-1 §4.5).
Caller timed out but shard ran the command → a 504 is not "nothing happened"; the money may still have moved. The client reconciles via a balance read, same rule as PlaceOrder (Tier-0 Point 7).
2.6 Ingress / egress & cross-layer touch

Touch: economy/account.MoveCash + persistence.MarkSnapshotDirty (live branch); the Phase-1 Store.DurableTransfer + get_account_handle (idle branch). Ingress: the Transfer command on the owner (live); the Present:false verdict + retained direction (idle). Egress: a TransferResult (live); (available, bank) or a typed error (idle) → the HTTP response.

3. The transfer path, consolidated (end to end)
POST /v1/account/transfer
  handler: validate → 400 on bad input
           map direction → (from,to buckets) + TransferDirection
           ctx = WithTimeout(transferTimeout)
           res, err = Broker.Transfer(ctx, aid, from, to, amount)
                        └─ route(Transfer{…, ReplyTo}) → owning shard → select(reply | ctx)
  err timeout ───────────────────────────────────────────────── 504

  shard handleTransfer:
     !live                       → TransferResult{Present:false} ┐
     MoveCash err                → {Present:true, Err}           │
     ok                          → {Present:true, Available,Bank}│
                                                                 │
  handler on res:                                                │
     Present:false ── DurableTransfer(ctx, aid, dir, amount) ────┘  → 200 {avail,bank} | 422 | 404 | 400 | 503/500
     Present:true & Err          → 422 (insufficient) | 500
     Present:true & ok           → 200 { Available, Bank }

One endpoint, one response shape, mode-blind to the caller; live and idle accounts move money through mechanically different paths that converge on the same { available, bank } result.

4. Decisions made (Phase 3 decision log)
P3-D1 — Transfer finalizes Tier-0's BankMove placeholder. Name Transfer / handleTransfer; field From, To account.Bucket (supersedes Dir BankDir); reply TransferResult via ReplyTo (funnel convention, not the doc's Reply). Supersedes BROKER_TIER0_IMPLEMENTATION_DESIGN.md §2/§4. LOCKED.
P3-D2 — Command carries buckets, handler retains direction. The live MoveCash speaks buckets, the durable Store.DurableTransfer speaks TransferDirection; the handler maps the one request direction into both, once. LOCKED.
P3-D3 — Request timeout → 504. Per the Tier-4 HTTP convention (§0.5, "Every handler") and the bank-transfer authority. The Tier-0 inline comment ErrBrokerTimeout // maps to HTTP 503 is a stale note predating the Tier-4 convention and is superseded (cross-doc note, §7). LOCKED.
P3-D4 — Broker.Transfer is a thin shard round-trip mirroring Broker.PlaceOrder; the durable-on-miss orchestration lives in the HTTP handler, not folded into a broker method. LOCKED.
P3-D5 — handleTransfer returns TransferResult (dispatch sends), per the Tier-0 dispatch convention. Three outcomes: Present:false (idle → durable) / Present:true+Err (live failure) / Present:true+balances (success). LOCKED.
P3-D6 — Account reached via sess.account.Account.Cash (the LoadedAccount refinement); corrects the authority's sess.account.Cash. The mutation sticks because s.sessions is map[AccountID]*ownedSession (pointer-valued); the inner Account pointer is a second layer. LOCKED.
P3-D7 — Insufficient funds → 422 in both branches (mode-blind). Resolves the authority's open 422-vs-400 item: 400 is for malformed input (Point 1 owns it); a well-formed request the bucket can't cover is unprocessable → 422. LOCKED.
P3-D8 — Present:false is internal-only → durable; no client 409 is ever emitted. This diverges from the Tier-4 reads, which do return 409 on Present:false because they have no durable fallback; a transfer does, so it falls through to Stage 2 instead. LOCKED.
P3-D9 — MarkSnapshotDirty fires unconditionally (mode-blind); on-demand isolation is enforced later at session_save (PersistOnClose=false), not gated in the handler. LOCKED.
P3-D10 — SessionID type seam. MarkSnapshotDirty takes realize.SessionID, a type alias for string. The broker's SessionID must be alias-compatible (= string / = realize.SessionID) so sess.binding.SessionID passes with no conversion; otherwise a trivial realize.SessionID(...) cast at the call. LOCKED.
5. Constraints / invariants honored
Single-writer per account — the in-session branch runs on the owner goroutine only; the s.sessions read/MoveCash mutate/MarkSnapshotDirty sequence is serialized by the funnel.
XOR (live ⊕ off-session) — the route-first check makes the live/idle decision authoritative and exclusive; the client is never trusted to declare liveness (a wrong declaration could fire a durable UPDATE on a live account and corrupt it).
One-writer principle — off-session, Store.DurableTransfer is the sole mutator of the cash columns (Phase-1 §4.4); in-session, the owner is the sole mutator of Cash. The two are mutually exclusive by XOR, so conservation holds.
Mode-blind endpoint — one response shape { available, bank }, one insufficient status (422), regardless of live vs idle; canonical vs on-demand invisible to the caller.
Persistence-with-every-functionality — each point opened with its persistence question: Point 1 persists nothing (transport), Point 2's live branch marks-dirty (async mirror) and its idle branch is durable-on-commit with no crash-window.
6. Deferred / carried items
unit vocabulary — the request unit field inherits Tier-4's unit convention; the exact vocabulary is a cosmetic item finalized with the Tier-4 read surfaces, not here.
Snapshot-flush cadence — when/how a shard drains its dirty set (set by MarkSnapshotDirty) and writes snapshots is the money-handling / teardown work, not Phase 3.
transferTimeout value — a retunable config knob (same shape as Tier-4 readTimeout); value left to config, not fixed here.
Agent surface — the same endpoint is reachable by agents (separate process, one request goroutine per call, identical mechanism); no separate design needed, noted for completeness.
7. Cross-doc note
P3-D1 supersedes BROKER_TIER0_IMPLEMENTATION_DESIGN.md §2 (BankMove / Dir BankDir / BankResult) and §4 (case BankMove: … handleBankMove). The command is finalized as Transfer / From,To account.Bucket / TransferResult / handleTransfer. The Tier-0 doc should carry an annotation pointing here so the two stay consistent for a cold reader.
P3-D3 supersedes the Tier-0 inline comment ErrBrokerTimeout // maps to HTTP 503. The HTTP-facing status for a broker request timeout is 504 (Tier-4 §0.5, echoed by the bank-transfer authority). ErrBrokerTimeout remains the funnel's error value; only its HTTP mapping is corrected.
P3-D6 corrects BROKER_BANK_TRANSFER.md's sess.account.Cash to sess.account.Account.Cash (the LoadedAccount refinement); no design change, a field-path correction grounded in the Tier-0 ownedSession shape and the economy/lifecycle source.