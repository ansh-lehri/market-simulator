Broker — Account & Session Lifecycle (B9, Tier 3)

Scope: the broker's orchestration of L5's lifecycle flows at session spin-up and tear-down — ensuring durable accounts exist, loading them into live in-memory sessions on the owning shard (canonical + on-demand), tearing them down (save/discard), and the parked crash-recovery slice. This is the driver that sits between L7's start/stop signals and the L5 lifecycle methods.

Status: Stages 1–4 LOCKED. Stage 5 (crash recovery) PARKED — inclusion undecided.

Nature of this tier: assembly, not invention. Every flow B9 drives already exists and is locked in L5 (LAYER_5_2_LIFECYCLE_FLOWS.md, LAYER_5_4_REALIZATION.md). L5 is part of the broker and stays in L5; B9 sequences its methods on the funnel and injects the seam state L5 does not hold (SessionBinding, participant).

Depends on:

L5 lifecycle: open_account, session_load, ondemand_session_load, session_save, session_discard, crash_recovery_reload (LAYER_5_2_LIFECYCLE_FLOWS.md).
L5 realization: STL-3 auto-square (LAYER_5_4_REALIZATION.md §5, item 41).
Broker Tier 0: the funnel (hash(AccountID) % N), the command model, route() + reply helper, ownedSession, SessionBinding (BROKER_CONCURRENCY_AND_ROUTING.md).
L7: admission + teardown contracts (LAYER7_TIER2_ADMISSION.md, LAYER7_TIER3_TEARDOWN.md, LAYER7_TIER0_REGISTRY_AND_IDENTITY.md).
0. The L7 ↔ broker seam (the joint contract this tier defines)

L7 is the conductor and is money-blind; the broker owns every money touch. L7↔broker is direct in-process Go calls, never HTTP. L7 fills these commands from its registry; the broker owns their shape and their delivery onto the owning shard.

go
// Ensure a durable account exists (both admission flows call this first).
broker.EnsureAccount(ownerType OwnerType, ownerID string) → (AccountID, error)

// On-demand money gate — L7 asks "qualify?", broker answers yes/no (money-blind for L7).
broker.QualifyOnDemand(accountID AccountID) → (ok bool, err error)

// Start a live session (canonical join OR on-demand seal). Mode branches inside.
type SessionStartCmd struct {
    AccountID       AccountID
    SessionID       SessionID              // L7-minted
    Mode            SessionType            // Canonical | OnDemand
    ExchangeID      ExchangeID
    ExchangeInbound chan<- InboundMessage  // the market's one inbound edge → the SessionBinding
    Participant     ParticipantID          // L7-minted; stamped on every FundedOrder
}
broker.StartSession(cmd SessionStartCmd) → error

// Canonical teardown is NOT an L7 per-session signal — it is the broker's own per-market
// close sweep (§ Stage 4). L7 signals the broker only for on-demand end (discard):
type StopOnDemand struct {
    AccountID AccountID
    SessionID SessionID
}
broker.StopOnDemand(cmd StopOnDemand) → error   // on-demand only → session_discard + drop

Structural split that runs through the whole tier: slow store I/O (session_load / ondemand_session_load) runs off-shard on the caller's (L7's) goroutine; only the finished *Account is handed to the shard via a small Command that does an in-memory map write. Canonical teardown is different — it is a broker-driven per-market sweep over the accounts the broker holds (square + save every account bound to a closing market), not a per-session action gated on an L7 signal (see §4).

Stage 1 — Account existence (EnsureAccount) — LOCKED

What it is. Every session entry — canonical join and on-demand start — begins by ensuring the participant has a durable account. Get-or-create; fronts L5's open_account and the broker-owned get_account_handle.

Surface. L7-fronted in-process Go call; no HTTP (a future signup/auth layer would add one — L7 Tier 0 §9, deferred). Runs inline on the caller's goroutine; no shard hop, no broker goroutine — it touches zero in-memory Account state (Postgres only), so nothing for the funnel to serialize. The account first acquires a shard owner at session_load (Stage 2). Concurrency safety is the uq_accounts_owner UNIQUE constraint + the pgx pool.

broker.EnsureAccount(ownerType, ownerID):
  1. capital, bank := <broker economy-config source>     // OPEN: flat dial, value TBD (BET-1). L7 passes nothing.
  2. accountID, err := open_account(db, ownerType, ownerID, capital, bank)   // L5 item 18
  3. switch err:
       nil               → return accountID, nil
       ErrAccountExists  → return get_account_handle(db, ownerType, ownerID)
       _                 → return "", err                 // ErrCreateFailed / ErrBadOwnerType / ErrBadOwnerID / ErrNegative*

broker.get_account_handle(db, ownerType, ownerID):        // broker owns this — L5 deferred it (lifecycle §9)
  SELECT account_id FROM accounts WHERE owner_type=$1 AND owner_id=$2
  row → (account_id, nil) ; no row → ("", ErrNoAccount) ; query err → ("", ErrLoadFailed)

ownerType is parameterized so A15 (agents) reuses it verbatim (OwnerAgent). Starting capital/bank policy stays broker-side; L7 receives only the handle → stays money-blind.

Edge cases.

Concurrent create, same (ownerType, ownerID) → constraint arbitrates: one wins, the other gets ErrAccountExists → get_account_handle → same handle. Never pre-check.
The ErrAccountExists → get_account_handle window is safe: a second INSERT on a unique index blocks until the first commits/rolls back, so the follow-up SELECT always sees the committed row.
ErrAccountExists then get_account_handle returns ErrNoAccount (row deleted — no such path exists today) → retry EnsureAccount once, then fail.
DB unreachable at open_account → ErrCreateFailed bubbles to L7; admission fails cleanly (pre-XOR, pre-Session → nothing to roll back).
DB error at get_account_handle → ErrLoadFailed bubbles; account exists but handle unfetched; user retries.
Invalid inputs → ErrBadOwnerType / ErrBadOwnerID (empty). L7 trusts and never validates the userID packet, so a malformed one is an upstream bug — return the error, never fabricate.
Negative capital/bank → ErrNegativeStartingCapital / ErrNegativeStartingBank. Comes from broker config → a misconfigured dial; dev-fatal, loud log.
Returning participant / agent re-spawn → ErrAccountExists → existing handle, carrying accumulated durable trading_cash (why canonical agents "revive" with P&L intact).
Idempotent by participant, not by call — safe to call at every session entry.
Deliberately does NOT: touch Redis, create an in-memory Account, route to a shard, or check balance (that's the on-demand gate, Stage 3).
Clean failure boundary — EnsureAccount is step 1 of both admission flows; its failure means no session/reservation/byUser slot was created.

Persistence. One Postgres statement per call (INSERT or SELECT). No Redis, no snapshot.

Open for Stage 1: the broker economy-config source for startingCapital / startingBank — un-named, flat for the prototype, value is a BET-1 dial.

Stage 2 — Canonical session load — LOCKED

What it is. Bring a durable account into a live in-memory session on its owning shard. Two steps: build the *Account off-shard (slow store I/O), then route one Command that installs it into the shard (instant in-memory write). Routing is the broker's normal hash(AccountID) % N — a pure, stateless function, so the owning shard is computable before the account is loaded.

Surface. L7 handles POST /session/canonical, runs EnsureAccount (Stage 1) + its XOR check, then makes the direct Go call carrying SessionStartCmd.

go
// Shard command (funnel unit — routed by hash(AccountID)):
type InstallSession struct {
    AccountID   AccountID
    Account     *Account
    Binding     SessionBinding
    Participant ParticipantID
    Reply       chan error       // buffered-1, per Tier-0 reply pattern
}
func (c InstallSession) Account() AccountID { return c.AccountID }
broker.StartSession(cmd) where cmd.Mode == Canonical:     // OFF-shard, on L7's admission goroutine
  1. if cmd.ExchangeInbound == nil → ErrBadBinding ; if cmd.Participant == "" → ErrBadParticipant
  2. acct, err := session_load(db, rds, cmd.SessionID, cmd.AccountID, Canonical)   // L5 item 19 — slow I/O
        err != nil → return err
  3. binding := SessionBinding{ SessionID: cmd.SessionID, ExchangeID: cmd.ExchangeID,
                                ExchangeInbound: cmd.ExchangeInbound }
  4. reply := make(chan error, 1)
     b.route(InstallSession{ AccountID: cmd.AccountID, Account: acct,
                             Binding: binding, Participant: cmd.Participant, Reply: reply })
     return waitReply(ctx, reply)                          // context deadline → ErrBrokerTimeout → 503

shard.handleInstallSession(c):                             // ON the owner goroutine — pure in-memory
  a. if _, live := s.sessions[c.AccountID]; live { c.Reply <- ErrSessionAlreadyLive; return }  // XOR guard
  b. s.sessions[c.AccountID] = &ownedSession{ account: c.Account, binding: c.Binding, participant: c.Participant }
  c. c.Reply <- nil

Why the I/O is off-shard (settled, and required by the design). A shard owns many accounts (a hash partition across all markets + on-demand). Running session_load's Postgres+Redis round-trips on the shard would freeze every other account on that partition — including live order flow. And loads arrive in bulk at market open + A15 agent population, so on-shard I/O would saturate shards and stall the market. The single-writer property protects s.sessions, which is why the install (map write) must be a shard command; it does not protect the not-yet-existent account, which is why the build can run anywhere.

Edge cases.

ErrNoAccount → bug/wrong-ID (EnsureAccount guaranteed existence; nothing deletes accounts) → hard-fail, loud log, nothing installed.
ErrLoadFailed (Postgres mid-load) → fail; nothing installed; nothing to roll back; user retries.
ErrSnapshotWriteFailed → canonical HARD-FAILS. A session with no snapshot is unrecoverable on crash; the durable account is untouched (read-only) → fail, bubble, retry.
Orphaned snapshot — session_load succeeded but InstallSession failed/timed out. Policy: TTL-only reap (persistence layer already TTLs stale session keys; keys are SessionID-scoped so they can't shadow a future session). StartSession returns the error.
Double load, same account → XOR should prevent it; the install handler's ErrSessionAlreadyLive guard refuses rather than overwrite.
Nil ExchangeInbound → rejected off-shard (ErrBadBinding) before any I/O.
Empty Participant → rejected off-shard (ErrBadParticipant); STP + the FundedOrder.Participant stamp key on it.
Install while market is Live but still PhaseClosed (L7's accepted transient) — install succeeds; punches bounce at the exchange's phase gate until open. The broker does not gate on phase (it doesn't track the exchange clock).

Persistence. session_load: 1 Postgres read + 1 Redis snapshot write, both off-shard; canonical hard-fails on snapshot-write error. InstallSession: in-memory only.

Does NOT: balance-check (canonical join is unconditional), move money, or start order flow. nextOrderID needs nothing special (fresh load → empty split, empty tracks).

Stage 3 — On-demand session load (gate + seal) — LOCKED

What it is. Start an isolated, throwaway session seeded read-only from the user's real durable balance, with no write-back (PersistOnClose = false). Two broker pieces: a gate (durable balance check L7 runs before the expensive spin-up) and the seal (ondemand_session_load + install). B9 moves no money in either piece.

Surface. On-demand admission (L7) touches the broker at two points, in order: EnsureAccount → gate (fully in front of spin-up; a fail wastes nothing) → on yes, L7 reserves/builds/wires → seal.

The gate must be a broker method — L7 is money-blind and can't read accounts; it only gets the boolean, never the balance or threshold (L7 A2.8, LOCKED).

broker.QualifyOnDemand(accountID):                        // OFF-shard; no live account exists (XOR)
  SELECT trading_cash FROM accounts WHERE account_id = $1     // durable read (Store wrapper, per Tier 4)
    no row      → false, ErrNoAccount                       // EnsureAccount ran first; missing = bug
    query error → false, ErrLoadFailed
    row         → if row.trading_cash < 0 → (false, nil)    // Rule 1: debt never qualifies (explicit)
                  else                    → (row.trading_cash > onDemandMinimum, nil)   // Rule 2: strict >

onDemandMinimum is the broker/economy threshold (L7 A2.8: "the broker's/economy's number"). It is an un-valued economy dial today (same category as starting capital); the economy §3.5 rule "0 or negative → blocked" is its guaranteed floor, the dial can sit above. The gate is a pure read — returns a boolean, moves nothing.

Gate semantics (Stage-D locks, see BROKER_PHASE2_IMPLEMENTATION_DESIGN.md P2-2·D4/D5 — authoritative; this doc tracks):
- Rule 1 (debt): `trading_cash < 0` → false, independent of onDemandMinimum. Explicit guard so a future validation relaxation (negative floors / incentives) cannot silently let a debtor through.
- Rule 2 (strict): `trading_cash > onDemandMinimum`, NOT `>=`. Equal-to-floor does NOT qualify. Zero min therefore means "any strictly-positive balance qualifies," not "any non-negative balance qualifies."
The earlier `>= onDemandMinimum` shorthand in this doc's edge-cases list below is superseded by the above rules.

broker.StartSession(cmd) where cmd.Mode == OnDemand:      // OFF-shard, on L7's spin-up goroutine
  1. if cmd.ExchangeInbound == nil → ErrBadBinding ; if cmd.Participant == "" → ErrBadParticipant
  2. acct, err := ondemand_session_load(db, rds, cmd.SessionID, cmd.AccountID)   // L5 item 20 — slow I/O
        err != nil → return err
     // acct: fresh *Account, ID = durable accountID; Cash{Available: trading_cash, Reserved:0,
     //       Settled:0, Bank: bank_cash}; Positions empty; drip copied; PersistOnClose = false
  3. binding := SessionBinding{ SessionID: cmd.SessionID, ExchangeID: cmd.ExchangeID,
                                ExchangeInbound: cmd.ExchangeInbound }
  4. reply := make(chan error, 1)
     b.route(InstallSession{ AccountID: cmd.AccountID, Account: acct,
                             Binding: binding, Participant: cmd.Participant, Reply: reply })
     return waitReply(ctx, reply)

The install handler is identical to Stage 2; PersistOnClose = false (carried on the *Account) is the only thing that makes it on-demand, read only by teardown (Stage 4) and realization (5.4).

Verified facts (from ondemand_session_load, §4): the bank pool is copied (Bank: row.bank_cash; "starts from trading not bank" means Available = trading_cash only, bank rides along for drip/buffer mechanics); it reuses the durable AccountID so it routes to the same shard — under XOR the slot is free.

Edge cases.

trading_cash > onDemandMinimum (strict) → yes. Equal-to-floor fails (gate is `>`, not `>=`).
trading_cash <= onDemandMinimum and >= 0 → no. B9 returns false; whatever the user does next (top up, retry, or not) is entirely outside this flow. On a retry, the seal reads the then-current balance.
trading_cash < 0 (debt) → no, independent of onDemandMinimum (Rule 1: explicit debt-rejection guard; see decision log P2-2·D4).
ErrNoAccount at gate → can't happen legitimately → bug → bubble, loud log.
ErrLoadFailed at gate (Postgres down) → bubble; no state created.
ErrNoAccount at seal → even less likely (gate confirmed existence) → log, bubble.
ErrLoadFailed at seal → fail; nothing installed; L7 tears down the half-built ephemeral market (Shape-C cleanup).
Snapshot write fails at seal → TOLERABLE (on-demand relaxation): log and continue with the in-memory copy; session still starts (a crash-loss costs nothing — never writes back).
Install fails/times out → same as Stage 2 edge 4; orphaned snapshot TTL-reaped; StartSession returns error → L7 tears down the ephemeral market.
Double live session, same account → XOR forbids; install guard ErrSessionAlreadyLive.
Nil ExchangeInbound / empty Participant → rejected off-shard (here the channel is the ephemeral market's inbound).

Gate→seal consistency: the seal re-reads durable trading_cash fresh, so it always seeds from a self-consistent value; no revalidation of the gate result needed.

Persistence. Gate: 1 Postgres read, no writes. Seal: 1 Postgres read + 1 Redis snapshot write (tolerable on fail), off-shard. Zero Postgres writes, ever. The seal establishes the property that makes realization (5.4) skip the write-behind buffer for on-demand trades → nothing reaches participant_trades. On-demand P&L is real in-session, invisible after.

Does NOT: move money (gate is a pure read; seal reads-and-copies). No canonical write-back path exists — teardown routes to session_discard.

Stage 4 — Teardown (session_save / session_discard) — LOCKED

What it is. End sessions cleanly — auto-square then write back (canonical) or discard (on-demand), drop the ownedSession + binding. The key point: canonical teardown is per-market and broker-driven over the accounts the broker holds — not per-session, and not gated on an L7 signal. The broker owns every account in s.sessions; it squares and saves them itself when their market closes. L7's session records are a separate concern (a human's membership) and have nothing to do with whether the broker can touch its own in-memory account.

The three teardown cases (LAYER7_TIER3_TEARDOWN.md):

Canonical market close — the real teardown event, per-market. The broker, on its own close handling for a market, sweeps every account it holds bound to that market and squares + saves each. Attached and left (Shape-A) users are handled identically, because the broker owns the account regardless of whether L7 still has a session record for it.
Canonical leave (Shape A) — L7 drops its own session record (frees the XOR slot) and signals the broker nothing. The account stays live on its shard, untouched, until its market closes (case above). L7 explicitly flags that the broker's close sweep tears down all canonical accounts "regardless of attachment."
On-demand (Shape C) — per-user and ephemeral, so this one is per-session: on user-leave or timeout, L7 signals stop → broker session_discard + drop.
go
// Per-market canonical close — broadcast to all shards on the broker's close handling.
type SweepMarketClose struct {
    ExchangeID ExchangeID
    Reply      chan error   // buffered-1 (one per shard)
}
func (c SweepMarketClose) Account() AccountID { return "" }   // fan-out, not account-routed

// Per-session on-demand teardown (L7-signalled).
type TeardownSession struct {
    AccountID AccountID
    SessionID SessionID
    Reply     chan error   // buffered-1
}
func (c TeardownSession) Account() AccountID { return c.AccountID }
market X closes → broker broadcasts SweepMarketClose{ExchangeID: X} to all shards

shard.handleSweepMarketClose(X):                           // each shard, ITS OWN accounts
  for aid, sess := range s.sessions:
    if sess.binding.ExchangeID != X { continue }
    auto-square sess.account                                // broker owns it; squares it (internals parked)
    err := session_save(db, rds, sess.binding.SessionID, sess.account)   // canonical persist
    if err != nil { continue }                              // keep live/recoverable on save fail (edge 4)
    delete(s.sessions, aid)                                 // drop ownedSession + binding

shard.handleTeardownSession(c):                            // on-demand only, per-session
  1. sess, live := s.sessions[c.AccountID]
     if !live → c.Reply <- ErrNoLiveSession ; return         // idempotency
  2. session_discard(rds, c.SessionID, sess.account)          // Redis DEL only, no Postgres
  3. delete(s.sessions, c.AccountID)
  4. c.Reply <- nil

Auto-square is the broker's, keyed on the market: it flattens every canonical position it holds for the closing market before saving. The mechanism (how each position is squared) is tracked separately and not expanded here — the point is that the broker does it, because it owns the accounts, needing no per-account L7 signal.

L5 methods driven (LAYER_5_2_LIFECYCLE_FLOWS.md): session_save §5 (guards PersistOnClose==false → ErrNotCanonical; asserts flat else panic; reconciles trading_cash = Available + Settled; one txn UPDATE; snapshot discarded last, not on write-fail). session_discard §6 (no db handle; guards PersistOnClose==true → panic; Redis DEL + in-memory drop).

Edge cases.

ErrNoLiveSession (on-demand teardown for an account with no live session) → already torn down / never loaded → reply cleanly.
ErrNotCanonical (on-demand mis-routed to session_save) → refuse, loud log.
session_discard panic on PersistOnClose==true (canonical mis-routed to discard) → dev-fatal deliberately.
session_save Postgres write fails during the sweep → do not discard the snapshot; the account is not dropped (the sweep skips the delete for it) → stays live and recoverable rather than vanishing with unsaved money. The one place teardown intentionally leaves an account live.
Sweep is idempotent — a second SweepMarketClose for the same market finds the accounts already gone (or re-squares an already-flat account harmlessly).
Teardown races a punch/result in flight → same shard serial loop, can't interleave; a result after the map-drop hits B3's not-found path and is dropped.
Shape-A leaver → no broker signal at leave; the account persists live on the shard and is swept by handleSweepMarketClose at market close, exactly like an attached account.

Persistence. Canonical: auto-square closing trades → write-behind buffer (→ participant_trades); session_save → 1 Postgres UPDATE (1 txn) + Redis snapshot DEL last. On-demand: zero Postgres; Redis snapshot DEL only.

Does NOT: decide market open/close hours (L7 owns the clock; the broker acts on close). The canonical square+save is not driven by a per-session L7 signal — it is the broker's own per-market sweep over the accounts it owns.

Stage 5 — Crash recovery (crash_recovery_reload) — PARKED

Status: parked; inclusion undecided. Parking it costs nothing — the Redis snapshots are already written by Stages 2–4 as part of L5's normal load/mirror flow; Stage 5 would only add the driver that consumes them.

Why it is largely moot in the current design. The whole sim is one process (L7, exchange, broker, L2, L6 in-process). A crash takes all in-memory state down together — the exchange's order books, L7's registry, the broker's shards — leaving only Postgres (durable accounts/trades) and Redis (session snapshots). A restored session's reservations point at orders on an exchange book that no longer exists, so re-hydrating them buys nothing. L7 has no persistence by design (Tier 0) and restarts clean; the broker inherits this: a crash means a clean restart, and the broker runs no recovery. L5 itself marks crash_recovery_reload's cross-layer coordination as OPEN (the deferred joint L5+L2+engine pass).

If ever wired (the specified slice, ready-not-built):

per live snapshot found in Redis:                          // OFF-shard I/O, then install
  1. acct, err := crash_recovery_reload(rds, sessionID, accountID, sessionType)  // L5 item 23 — full split verbatim
        ErrNoSnapshot → canonical: fall back to session_load (fresh from Postgres);
                        on-demand: gone (no durable record) → drop
        err           → log, skip session
  2. re-inject seam state L5 does NOT hold:
        binding     = fresh SessionBinding from the rebuilt market   // NOT from snapshot
        participant = re-minted / re-supplied by L7                  // NOT from snapshot
        tracks      = reconstruct from acct's restored reservations  // see OrderTrack note
  3. resume nextOrderID past the max restored OrderID
  4. route InstallSession{ acct, binding, participant } → shard      // same command as Stage 2

OrderTrack on recovery (decision, deferred). OrderTrack (Status, CancelInFlight, IsMarketBuy) is broker-side and not in the L5 snapshot. Since the driver isn't wired, no OrderTrack persistence is introduced now (B12's snapshot payload unchanged). When wired, the intended path is reconstruct from the restored reservation (a restored reservation ⇒ Working; CancelInFlight=false); the only lossy field is IsMarketBuy, resolved by the cross-layer pass then.

nextOrderID resume — the one open input. On reload the counter must resume past the max restored OrderID so a post-recovery order can't collide with an in-flight one. Where the resume logic sits follows B2's counter home: per-shard (self-routing OrderID) → each shard resumes past its own restored max inside InstallSession; broker-global → one atomic counter resumes past the global max. Open: confirm B2's OrderID counter home (per-shard vs broker-global).

Edge cases (for the specified slice). ErrNoSnapshot → canonical falls back to session_load, on-demand dropped. No stale binding possible (it was never snapshotted — the reason it's kept off the snapshot). On-demand post-crash → moot (ephemeral market gone, nothing durable). Partial/corrupt snapshot → reload error → log, skip (no half-account). Recovery can't race normal traffic (runs at restart, before any market/order exists).

Persistence. None added. OrderTrack persistence not introduced.

Status summary
Stage	Item	Status
1	EnsureAccount (open_account + get_account_handle)	LOCKED
2	Canonical session load (off-shard build + InstallSession)	LOCKED
3	On-demand load (QualifyOnDemand gate + ondemand_session_load seal)	LOCKED
4	Teardown — broker-driven per-market close sweep (square + session_save); on-demand session_discard per-session	LOCKED
5	Crash recovery (crash_recovery_reload slice)	PARKED

Open inputs across the tier:

Stage 1: broker economy-config source for startingCapital/startingBank (un-named dial, BET-1 value).
Stage 3: onDemandMinimum value (un-named economy dial).
Stage 5 (if included): B2's OrderID counter home (per-shard vs broker-global).

Related B9-adjacent items (separate stages/responsibilities, not this doc): A7 session start/stop action surface (the command structs here are its seam), A6 bank pull/push (the user-driven transfer; a gate-fail sends the user there, but B9 never orchestrates it), A15 agent lifecycle at population scale (reuses EnsureAccount(OwnerAgent, …)), B11 handle-injection mechanics (done here via the SessionBinding in InstallSession), B12 async persistence coordination (rides the snapshot writes here).