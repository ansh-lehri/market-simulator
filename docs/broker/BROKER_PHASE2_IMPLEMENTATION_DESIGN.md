Broker — Phase 2 Implementation Design (Session & Account Lifecycle, Tier 3)

Surface exposed: the in-process L7 → broker calls. Primary authority docs: BROKER_ACCOUNT_SESSION_LIFECYCLE.md (the locked Tier-3 design) and BROKER_EXPECTATIONS_FROM_L7.md. Sits on locked Phase 1 (wiring root + durable Store) and Tier 0 (the single-writer funnel). Character of the tier: assembly, not invention — every flow already exists and is locked in L5 (economy/lifecycle); Phase 2 sequences those L5 methods on the funnel and injects the seam state L5 doesn't hold.

All type signatures below were read from source this session (economy, contract, connectivity tarballs), not recalled.

0. Point status
Point	Name	Status
1	EnsureAccount	LOCKED
2	QualifyOnDemand	LOCKED
3	StartSession (+ InstallSession)	LOCKED
4	Teardown	MODEL LOCKED / IMPL OPEN
5	Handle injection (B11) + snapshot on close (B12)	ABSORBED / PARKED

Shared id types (all string aliases, source-verified): account.AccountID, contract.ParticipantID, broker-local SessionID / ExchangeID (Tier-0 aliases). contract.InboundMessage is the exchange's inbound submit/cancel struct.

1. Point 1 — EnsureAccount — LOCKED
Intent

The get-or-create for a durable account. Every session entry (canonical + on-demand) begins here. Off-shard, on L7's goroutine, Postgres-only — does not touch the funnel.

Design

Create-first, not check-first: try L5's birth writer; on a duplicate, fall back to the Phase-1 lookup. The INSERT is the race arbiter (uq_accounts_owner UNIQUE), so there is no check-then-create window.

Broker.EnsureAccount(ctx, ownerType lifecycle.OwnerType, ownerID string) (AccountID, error):
  capital, bank := b.cfg.StartingCapital, b.cfg.StartingBank                 // BrokerConfig dial
  aid, err := b.store.lifecycle.OpenAccount(ctx, ownerType, ownerID, capital, bank)
  switch {
    err == nil:                                        return aid, nil        // created
    errors.As(err, &lifecycle.ErrAccountExistsFor{}):
        h, herr := b.store.AccountHandle(ctx, ownerType.String(), ownerID)    // raw read, Phase 1
        switch herr {
          nil:            return h, nil
          ErrNoAccount:   retry EnsureAccount once; then return "", ErrNoAccount
          ErrLoadFailed:  return "", herr
        }
    default:                                           return "", err         // ErrBadOwnerType/ID, ErrNegative*, ErrCreateFailed
  }

Never returns "not found" — the method's job is to make the account exist. Errors are only genuine failures. Type plumbing: OpenAccount takes the typed lifecycle.OwnerType; AccountHandle takes ownerType.String() (Phase-1 decision P1-D5 — the Store speaks the DB's column language).

The Store's birth writer (wiring delta)

To reuse L5's OpenAccount (which owns the account-row shape — drip_active, drip_progress — and the id-mint scheme), the Store holds its own lifecycle.Service, backed by an unstarted persistence.Component on the Store's own pool:

type Store struct {
    pool      *pgxpool.Pool       // existing — AccountHandle, trade reads, durableTransfer
    lifecycle *lifecycle.Service  // NEW — off-shard birth (OpenAccount) + session loads (Point 3)
}

// wiring root, Phase-1 §1.4 step 3:
storePool    := connectivity.db.NewPostgres(infra)
storeRedis   := connectivity.db.NewRedis(infra)              // NewComponent requires it
_, storeCost := cost.NewModule(cfg.Country)                  // NewComponent requires it
storePersist := persistence.NewComponent(storePool, storeRedis, storeCost, persistCfg) // NOT Started
store        := NewStore(storePool, lifecycle.NewService(storePersist))

OpenAccount is pure-Postgres (source: "No Redis, no session"), so the component is never Started — no drainer. This consciously supersedes Phase-1 §2.2's "Store = pool only, no Redis": the Store gains a redis handle (idle for birth, active for session snapshots once Point 3 lands). Same shared Redis DB, so snapshots land where the owning shard recovers from. The idle redis is closed at shutdown.

Persistence / ingress / egress

One Postgres statement per call (INSERT, or SELECT on fallback). No Redis, no snapshot for birth itself. Ingress (ownerType, ownerID) from L7; egress (AccountID, error).

Decision log
P2-1·D1 — create-first get-or-create (OpenAccount → on ErrAccountExists, AccountHandle); INSERT is the sole race arbiter. LOCKED
P2-1·D2 — birth reuses L5's OpenAccount, not a raw broker INSERT, so drip_* + id-mint stay L5-owned. Chosen over a raw Store INSERT; rationale is schema-coupling, not one-writer (the Store already writes accounts directly via durableTransfer). LOCKED
P2-1·D3 — the birth writer lives on the Store as its own unstarted persistence.Component + lifecycle.Service on the Store's pool. Supersedes Phase-1 §2.2 "pool-only Store"; Store gains a redis handle (later active for snapshots). LOCKED
P2-1·D4 — starting capital/bank are BrokerConfig fields, read once at bring-up, applied to every account. Value is an open dial; the home is fixed. LOCKED
2. Point 2 — QualifyOnDemand — LOCKED
Intent

The on-demand money gate. Before L7 pays for on-demand spin-up, it asks the broker "does this account clear the bar?" The broker reads the durable balance, compares to the economy threshold, hands back a bare yes/no — never the balance, never the threshold (L7 stays money-blind, per L7 decision A2.8). Pure read, off-shard, no state, no money moved, placed by L7 in front of spin-up so a false wastes nothing.

Design

Needs a durable trading_cash-by-account read. Phase 1 has none that fits (its five accessors read participant_trades; durableTransfer is a write). So Point 2 adds one small Store read primitive; the gate wraps it with the threshold.

func (s *Store) TradingCash(ctx, accountID AccountID) (int64, error)
// SELECT trading_cash FROM accounts WHERE account_id = $1
//   pgx.ErrNoRows → 0, ErrNoAccount   |   other → 0, wrap(ErrLoadFailed)   |   row → trading_cash, nil

Broker.QualifyOnDemand(ctx, accountID) (ok bool, err error):     // OFF-shard, pure read
  cash, err := b.store.TradingCash(ctx, accountID)
  switch {
    err == nil:
        if cash < 0 { return false, nil }                        // Rule 1: debt never qualifies
        return cash > b.cfg.OnDemandMinimum, nil                 // Rule 2: strict > (NOT >=)
    errors.Is(err, ErrNoAccount): return false, ErrNoAccount     // EnsureAccount ran first → missing = bug
    default:                       return false, ErrLoadFailed    // Postgres down; nothing created
  }

The gate has two conjoined rules (locked at Stage D):

Rule 1 — debt never qualifies. `trading_cash < 0` returns false regardless of OnDemandMinimum. OnDemandMinimum is non-negative at bringup (BrokerConfig validate >= 0), so the strict comparison alone would also reject debt — but the rule is asserted EXPLICITLY so a future validation relaxation (negative floors, incentives, etc.) does not silently let a debtor through. Load-bearing property of the gate, not an implementation detail of the min.

Rule 2 — strict greater-than. `trading_cash > OnDemandMinimum`. A balance equal to the floor does NOT qualify — the user must be strictly above the bar. Zero min therefore means "any strictly-positive balance qualifies," NOT "any non-negative balance qualifies." Earlier shorthand of `cash >= min` in this doc is superseded by P2-2·D4 / D5.

ok=false, err=nil on a legit under-balance (or a debt) is not an error — it's a valid "no." trading_cash is signed (debt is legal, no CHECK(>=0)); Rule 1 catches debt explicitly, Rule 2 catches the "insufficient-but-non-negative" case. Exactly one Postgres read, zero writes, no Redis.

Decision log
P2-2·D1 — broker method returning a bare boolean; L7 never sees balance or threshold (honors L7 A2.8). Pure read, off-shard, no state, no money moved. LOCKED
P2-2·D2 — new Store read primitive TradingCash(accountID); reuses ErrNoAccount / ErrLoadFailed. LOCKED
P2-2·D3 — onDemandMinimum is a BrokerConfig dial (value open, home fixed); economy's "≤0 → blocked" is a separate guaranteed floor the dial may sit above. LOCKED
P2-2·D4 — explicit debt-rejection guard. `trading_cash < 0` returns (false, nil) BEFORE the min comparison, independent of OnDemandMinimum's value. Rationale: the design intent "no debtor qualifies" must survive any future relaxation of the min's non-negative invariant; a strict > with min >= 0 covers debt mathematically today, but the explicit guard pins the semantics in code where readers find them. LOCKED
P2-2·D5 — strict greater-than, not greater-or-equal. `cash > min`, not `cash >= min`. A balance equal to the floor does NOT qualify; the user must be strictly above the bar. Zero min therefore means "any strictly-positive balance qualifies." Supersedes the earlier shorthand `cash >= min` in the Design pseudocode above (and in the earlier BROKER_ACCOUNT_SESSION_LIFECYCLE.md §Stage-3 sketch; the implementation + this doc are now the authority). LOCKED
3. Point 3 — StartSession (+ InstallSession) — LOCKED
Intent

Turn a durable account into a live in-memory session on its owning shard. First Phase-2 point that touches the Tier-0 funnel. One method, Mode branches inside: Canonical (a normal market join, unconditional) or OnDemand (the seal after Point 2's gate passed).

Types (source-bound)
go
type SessionStartCmd struct {                          // L7 → broker, off-shard
    AccountID       account.AccountID
    SessionID       SessionID
    Mode            SessionType                         // lifecycle.SessionType: Canonical | OnDemand
    ExchangeID      ExchangeID
    ExchangeInbound chan<- contract.InboundMessage      // the market's inbound edge
    Participant     contract.ParticipantID              // stamped on every FundedOrder
}

type InstallSession struct {                           // NEW — first command on the funnel
    AccountID   account.AccountID
    Loaded      lifecycle.LoadedAccount                 // whole LoadedAccount, not a bare *Account
    Binding     SessionBinding                          // Tier-0 Point 8 type
    Participant contract.ParticipantID
    Reply       chan error                              // buffered-1
}
func (c InstallSession) Account() account.AccountID { return c.AccountID }  // lets route() hash it

The reconcile (resolved): Tier-0's ownedSession holds a LoadedAccount, and both L5 loaders return LoadedAccount{Account *account.Account, OwnerType, OwnerID}. So InstallSession carries the whole LoadedAccount (the Phase-2 doc's bare *Account was loose shorthand). Confirmed downstream: SessionSave / SessionDiscard (Point 4) also take LoadedAccount.

StartSession — off-shard orchestrator
Broker.StartSession(ctx, cmd SessionStartCmd) error:           // ON L7's goroutine
  1. if cmd.ExchangeInbound == nil → ErrBadBinding
     if cmd.Participant == ""       → ErrBadParticipant
  2. switch cmd.Mode {                                          // the ONLY divergence
       Canonical: loaded, err = b.store.lifecycle.SessionLoad(ctx, cmd.SessionID, cmd.AccountID)
       OnDemand:  loaded, err = b.store.lifecycle.OndemandSessionLoad(ctx, cmd.SessionID, cmd.AccountID)
     }
     if err != nil → return err
  3. binding := SessionBinding{ SessionID: cmd.SessionID, ExchangeID: cmd.ExchangeID,
                                ExchangeInbound: cmd.ExchangeInbound }
  4. reply := make(chan error, 1)
     b.route(InstallSession{ AccountID: cmd.AccountID, Loaded: loaded,
                             Binding: binding, Participant: cmd.Participant, Reply: reply })
     return b.waitReply(ctx, reply)                             // ctx deadline → ErrBrokerTimeout

Both loaders are real lifecycle.Service methods, run on the Store's service (off-shard, concurrency-safe — the load path is LoadAccount (Postgres read) + WriteSnapshot (pure Redis write); it never touches persistence's unlocked dirty map, which only MarkSnapshotDirty — a live-mutation call — touches).

Snapshot-fail policy needs no broker branch: it lives inside the L5 loaders (SessionLoad returns ErrSnapshotWriteFailed; OndemandSessionLoad tolerates the same miss and returns success). The broker propagates uniformly.

handleInstallSession — on the owner goroutine (pure in-memory)
shard.handleInstallSession(c):
  a. if _, live := s.sessions[c.AccountID]; live { c.Reply <- ErrSessionAlreadyLive; return }  // XOR
  b. s.sessions[c.AccountID] = &ownedSession{ loaded: c.Loaded, binding: c.Binding, participant: c.Participant }
  c. c.Reply <- nil

Nothing else — no order flow, no nextOrderID special case (fresh load ⇒ empty split, empty tracks). Single-writer protects s.sessions, which is why the install must be a command while the load can run anywhere.

Error map & edge cases
Source	Canonical	On-demand
ErrNoAccount from loader	bug (EnsureAccount guaranteed existence) → hard-fail, loud log, nothing installed	same (gate ran too)
ErrLoadFailed (Postgres mid-load)	fail, nothing installed, retry	same
WriteSnapshot miss	loader returns ErrSnapshotWriteFailed → hard-fail (no snapshot = unrecoverable); durable row untouched	loader tolerates → session starts on in-memory copy (never writes back)
Install failed/timed out (orphaned snapshot)	TTL-only reap (keys SessionID-scoped, can't shadow a future session); StartSession returns error	same
Double live session, same account	XOR should prevent; handler ErrSessionAlreadyLive refuses	same
Nil ExchangeInbound / empty Participant	rejected off-shard before any I/O	same
Install while market Live but PhaseClosed	install succeeds; punches bounce at the exchange phase gate; broker never gates on phase	same

Persistence: load = 1 Postgres read + 1 Redis snapshot write, off-shard. Install = in-memory only. On-demand writes zero Postgres, ever — the property that later makes realization skip the write-behind buffer so on-demand trades never reach participant_trades.

Decision log
P2-3·D1 — one StartSession(SessionStartCmd); Mode selects SessionLoad vs OndemandSessionLoad; install handler identical, distinguished only by PersistOnClose on the *Account. LOCKED
P2-3·D2 — InstallSession carries the whole lifecycle.LoadedAccount; ownedSession stores it as-is. Resolves the Tier-0/Phase-2 shorthand mismatch. LOCKED
P2-3·D3 — off-shard load runs on the Store's lifecycle.Service for both modes (concurrency-safe: load path never touches the unlocked dirty map). LOCKED
P2-3·D4 — snapshot-failure policy is not re-implemented in the broker; it lives in the L5 loaders and StartSession propagates their error uniformly. LOCKED
P2-3·D5 — InstallSession is the first command on the Tier-0 funnel; handleInstallSession = XOR guard + map write + reply, nothing more. LOCKED
4. Point 4 — Teardown — MODEL LOCKED / IMPL OPEN

Implementation deliberately left open this session. What follows is the settled model (a reconciliation of a real conflict between the broker doc and the L7 teardown doc) plus the one unresolved fork that blocks impl. No handler pseudo-code is designed here on purpose.

The conflict that was reconciled

The broker Phase-2 doc (BROKER_ACCOUNT_SESSION_LIFECYCLE.md, Stage 4) modelled canonical teardown as a broker-driven per-market sweep — one SweepMarketClose{ExchangeID} broadcast to all shards, broker enumerates its own s.sessions, "not per-session, not gated on an L7 signal." The L7 teardown doc (LAYER7_TIER3_TEARDOWN.md, §2.2, Shape B) modelled it as L7-driven per-session — L7 loops market.members, sends the broker one canonical stop per member, waits for N save-acks. These contradicted each other on who enumerates, the wire shape, and Shape-A leaver handling.

Settled model (this session)
P2-4·D1 — teardown orchestration is L7's, per-session. On market close, L7's own per-market timer loops market.members and sends the broker one canonical-stop signal per member (→ save), then waits for N acks. The all-in-one-go per-market SweepMarketClose broadcast is superseded (dropped over signalling issues); the broker instead receives a per-session canonical-stop command, account-routed to the owning shard like InstallSession. SETTLED
P2-4·D2 — the broker does settlement only, not orchestration: per canonical-stop signal → SessionSave (write-back) + cleanup + drop the ownedSession. Broker owns the accounts and does all account-level work; L7 orchestrates and counts acks. SETTLED
P2-4·D3 — squaring is NOT part of teardown. Auto-square is a broker-internal mechanism that runs during the market's closing, so by the time L7's per-session save-signals arrive (after market time) positions are already flat. The teardown handler is save + cleanup + drop only and assumes flat — it drops the "auto-square inside the handler" step the broker doc's Stage-4 pseudo-code showed. (The L7 doc agrees: its barrier note says positions are already flat.) SETTLED
P2-4·D4 — on-demand teardown is unchanged and clean: L7 signals StopOnDemand → account-routed TeardownSession{AccountID, SessionID} → SessionDiscard (Redis DEL only, no Postgres) + drop. Idempotent (ErrNoLiveSession when already gone). SessionDiscard panics on a canonical account (PersistOnClose==true) — dev-fatal guard against silently losing money progress. SETTLED

Real L5 signatures (source): SessionSave(ctx, sessionID string, loaded LoadedAccount) error (guards PersistOnClose==false → ErrNotCanonical; reconciles trading_cash = Available + Settled; one txn UPDATE; snapshot DEL last, not on write-fail) and SessionDiscard(ctx, sessionID string, loaded LoadedAccount) error.

OPEN — blocks impl
P2-4·OPEN-1 — the Shape-A leaver. A canonical user who attached then left mid-market is removed from market.members, so L7's per-member loop won't signal them at close. Two candidate resolutions:
(B1) L7 also sends a per-session save-signal at leave time → broker saves + drops immediately; nothing is ever detached-but-live; no straggler. (Implies the broker doc's "Shape-A leave → account stays live till close" is stale.)
(B2) a canonical detach really leaves the account live on the shard till close (rejoin reuses it); L7's loop misses it → broker needs a straggler catch (save+drop its own remaining accounts for that market at exchange-stop).
This fork decides whether Point 4 is just the per-session handler (B1) or that plus a straggler sweep (B2). UNRESOLVED.
Also open: the exact per-session canonical-stop command shape (the broker doc only defined a per-market SweepMarketClose; the settled model needs a per-session canonical command). Deferred with the impl.
5. Point 5 — Handle injection (B11) + snapshot on close (B12) — ABSORBED / PARKED

Roadmap Point 5 is two tracker items; neither is open design work:

Exchange-handle injection (B11). Tracker: carrying model done in Tier 0 (SessionBinding co-located on ownedSession); only the injection mechanics were deferred to "Tier 3, session load." Those mechanics are exactly Point 3 — StartSession builds the SessionBinding from L7's ExchangeInbound and installs it on ownedSession. Absorbed into Point 3; nothing left.
Snapshot on close (B12). Tracker: the broker re-persists nothing new — cash, reservation records, and the close-accumulator already ride L5's snapshot (5.2 / 5.4). The only genuinely-new payload candidate is the thin OrderTrack (order Status / CancelInFlight), and that matters only if it must survive a crash. Crash recovery (the lifecycle doc's Stage 5) is PARKED, so B12 parks with it. No design until crash recovery is picked up.

No decision log — no new decisions.

Appendix — consolidated decision log
Code	Decision	Status
P2-1·D1	Create-first get-or-create; INSERT is the race arbiter	LOCKED
P2-1·D2	Birth reuses L5 OpenAccount (schema-coupling, not one-writer)	LOCKED
P2-1·D3	Birth writer on the Store (unstarted Component + lifecycle.Service); supersedes Phase-1 §2.2 pool-only	LOCKED
P2-1·D4	Starting capital/bank are BrokerConfig dials (value open, home fixed)	LOCKED
P2-2·D1	QualifyOnDemand returns a bare boolean; L7 money-blind (A2.8)	LOCKED
P2-2·D2	New Store read TradingCash(accountID); reuses ErrNoAccount/ErrLoadFailed	LOCKED
P2-2·D3	onDemandMinimum is a BrokerConfig dial; economy floor is separate	LOCKED
P2-2·D4	Explicit debt-rejection guard: cash < 0 → (false, nil) BEFORE the min comparison, independent of OnDemandMinimum	LOCKED
P2-2·D5	Strict greater-than, not greater-or-equal: cash > min (equal-to-floor does NOT qualify); supersedes the earlier cash >= min shorthand	LOCKED
P2-3·D1	One StartSession; Mode selects loader; install handler identical	LOCKED
P2-3·D2	InstallSession carries whole LoadedAccount	LOCKED
P2-3·D3	Off-shard load on the Store's lifecycle.Service; concurrency-safe	LOCKED
P2-3·D4	Snapshot-fail policy lives in L5 loaders; broker propagates	LOCKED
P2-3·D5	InstallSession first funnel command; handler = guard + map write + reply	LOCKED
P2-4·D1	Teardown orchestration is L7's, per-session; per-market broadcast superseded	SETTLED (impl open)
P2-4·D2	Broker does settlement only (SessionSave + cleanup + drop)	SETTLED (impl open)
P2-4·D3	No squaring at teardown; auto-square runs during market close; handler assumes flat	SETTLED (impl open)
P2-4·D4	On-demand teardown unchanged: TeardownSession → SessionDiscard + drop; idempotent	SETTLED (impl open)
P2-4·OPEN-1	Shape-A leaver: (B1) save-at-leave vs (B2) straggler-catch at exchange-stop	UNRESOLVED
B11	Handle injection — absorbed into Point 3	DONE
B12	Snapshot on close — parks with crash recovery (Stage 5)	PARKED