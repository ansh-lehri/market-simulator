Layer 5.1 — Account State Read Operations (Tier-1 Hands)

Scope: the three Tier-1 read primitives of 5.1 — items 14, 15, 16 on the Layer-5 action list. These are the "hands" that read account state, the counterpart to the six write primitives (MoveCash, MovePosition, CreditPosition, RemovePosition, CreditCash, DebitCash) specified in LAYER_5_1_OPERATIONS.md. A read never mutates, never locks, never computes policy — it returns current state.

What this document is NOT: it is not the Tier-2 session lifecycle (session load/save/discard/crash reload, account creation) — those are flows that establish and persist the state these reads consume, and live in LAYER_5_2_LIFECYCLE_FLOWS.md. It is not the write primitives (LAYER_5_1_OPERATIONS.md). It is not the C12 external query surface (get P&L / trade history / cost breakdown / recent_streak), which is a downstream HTTP-faced Tier-2 concern, not a hand.

Status: all three reads LOCKED. The store-source model was corrected during design (see §1) — the earlier draft in LAYER_5_1_OPERATIONS.md §2 had the seed reading Postgres; the corrected and locked model is Redis-first for session state. This document supersedes that section.

0. The two tiers (orientation)

Everything here is Tier 1: the hands — dumb, single-responsibility, in-process functions that read one account's state. They are called by Tier-2 flows; they never orchestrate, hit a store on the hot path, or make policy decisions.

TIER 2 (flows): session_load / ondemand_session_load / session_save / session_discard /
                crash_recovery_reload / open_account ... the 5.2 methods (reserve_if_available, ...)
        │  establish + persist state, then call ↓
TIER 1 (hands): READS  → read_available_cash, read_available_position, read_balance_snapshot
                WRITES → MoveCash, MovePosition, Credit/RemovePosition, Credit/DebitCash
        │  read / mutate ↓
        in-memory working Account (the live split)  ◄──seeded/mirrored──►  Redis / Postgres
1. The store-source model (the whole subtlety) — corrected & locked

There are up to three copies of an account's data at any moment, and they are not the same data:

┌─────────────────────────────────────────────────────────────────────┐
│ POSTGRES  — durable capital only (source of truth, cross-session)    │
│   accounts row: trading_cash (SIGNED), bank_cash, drip_active,       │
│                 drip_progress                                        │
│   NO available/reserved/settled split. Survives restart.             │
├─────────────────────────────────────────────────────────────────────┤
│ REDIS  — current-session snapshot (crash recovery; discarded on close)│
│   l5:snap:{session_id}:acct:{account_id}:cash =                      │
│       { available, reserved, settled, bank_cash,                     │
│         drip_active, drip_progress }                                 │
│   ...:pos:{instrument_id} = { available_units, reserved_units,       │
│                               open_lots:[{entry_price, entry_at,     │
│                                           remaining_qty}] }          │
│   ...:resv:{order_id} = { participant, side, is_market,             │
│                           reserved_cash|reserved_units, remaining_qty}│
├─────────────────────────────────────────────────────────────────────┤
│ IN-MEMORY  — the live working Account, in the session goroutine      │
│   the authoritative, current-to-the-tick split/positions/lots.       │
│   Redis lags it (mirrored on cadence); Postgres lacks the split.     │
└─────────────────────────────────────────────────────────────────────┘

The standing rule (locked by the user): the first read of any datum comes from a store (Redis or Postgres, by data type); thereafter in-memory processing takes over, with writes flowing back to the stores in sequence or asynchronously by data/process. In-memory is never the first source; it is the working truth after the boundary read primes it.

The Redis-first resolution for session state. For live session money state (the cash split and positions), the first read comes from Redis, not Postgres. This works because the Tier-2 session bootstrap (session_load, in LAYER_5_2_LIFECYCLE_FLOWS.md) reads Postgres accounts once at session creation, synthesizes the fresh split, and writes it into the Redis snapshot before the session accepts any order. From that point:

read_balance_snapshot (16) — the session-init seed read — reads Redis (...:cash).
read_available_cash / read_available_position (14/15) — the per-order hot-path reads — read the in-memory working copy that the seed established. They do not hit a store per order (a network hop on the single-goroutine matching hot path is structurally forbidden — it would serialize matching behind store latency).

So the store is read at the boundary (session init → Redis, itself seeded from Postgres once at bootstrap); the hot path reads the local copy that boundary populated. Redis-first for the current session; Postgres only at the creation/bootstrap and session-end boundaries. (This Redis-first ruling is scoped to session state; other reads — e.g. durable trade history — will choose their store by data type when specified.)

Why in-memory is safe and always current. Reads and writes touch the same Account struct — no copy, no cache. A buy that mutates Cash.Available from 200 → 150 (via MoveCash) leaves 150 in the struct; the next read_available_cash returns 150. Single-goroutine-per-session serialization guarantees a read and a write from the same participant never interleave, so a read never catches a half-applied mutation. In-memory is ahead of Redis (which lags by the mirror cadence) and is the truth; the hot path never reads Redis, so that lag is irrelevant to correctness.

Exact schema/key names below are taken verbatim from LAYER_5_PERSISTENCE.md (§3.1 accounts, §4.1 Redis snapshot keys). They are not paraphrased.

2. Shared state model

The reads operate on the same in-memory state model as the writes (defined in full in LAYER_5_1_OPERATIONS.md §0.4; reproduced here for a self-contained read spec):

go
type AccountID    = string
type InstrumentID = string
type SessionID    = string

// Account is the in-memory working state for ONE participant during ONE live session.
// Seeded at session start from the Redis snapshot (itself bootstrapped from Postgres once),
// it is the authoritative, current-to-the-tick copy for the whole session.
type Account struct {
    ID        AccountID
    Cash      Cash
    Positions map[InstrumentID]*Position

    // set at load time by the Tier-2 lifecycle flow (session type):
    //   canonical -> true  (durable figure written back to Postgres at session end)
    //   on-demand -> false (isolated copy, discarded at close, never written back)
    PersistOnClose bool

    // durable drip state, carried for the (HELD) drip_tap flow:
    DripActive   bool
    DripProgress *int64 // nullable, mirrors accounts.drip_progress
}

// Cash — the live trading-account split plus the live bank copy. This split exists ONLY in
// memory and in its Redis mirror; Postgres holds one signed trading_cash the split reconciles to.
type Cash struct {
    Available int64 // free trading-account cash, reservable. MAY be negative (Option B — see writes doc §1.6).
    Reserved  int64 // cash locked behind resting buy orders. >= 0.
    Settled   int64 // realized/spent trading-account cash this session. >= 0.
    Bank      int64 // live copy of the bank/buffer pool. NOT trading-visible; >= 0.
}

// Position — live per-instrument holding. Session-scoped; never persisted to Postgres.
type Position struct {
    Available int64 // units held, free to sell. >= 0.
    Reserved  int64 // units locked behind a resting sell. >= 0.
    OpenLots  []Lot // FIFO queue in arrival order.
}

type Lot struct {
    EntryPrice   int64     // integer ticks — maker price of the buy fill
    EntryAt      time.Time // fill time — sole source of position_age
    RemainingQty int64     // units of this lot not yet sold
}
3. read_available_cash — hot-path buy fundability (item 14)

Purpose. Return spendable trading-account cash for a buy reservation decision. This is the read reserve_if_available blocks on for every buy order, synchronously, inline in the engine's single goroutine.

Source. The in-memory Account.Cash.Available — the live split established at session init (which read it from Redis). No store hop on the hot path.

Returns the raw signed value — no clamp. After Option B (DebitCash may drive Available negative on fees), this read can return a negative number. That is correct and intended: reserve_if_available compares read_available_cash() >= required, and a negative Available fails any positive requirement — exactly the "no buying power while underwater" broker behavior. Clamping to 0 here would lie to the reserve check and mask fee-driven debt.

go
// read_available_cash returns live spendable trading-account cash for a buy fundability check.
// Reads the in-memory working copy primed at session init (from Redis). NO store hop on the hot
// path. May return a NEGATIVE value (Option B) — returned raw; the caller judges.
func (a *Account) read_available_cash() int64 {
    // 1. PRECONDITION — working state must be primed.
    //    The engine only routes an order to the gate AFTER session init seeded this Account.
    //    A nil Account here = order reached the gate before init completed = ordering bug in
    //    the wiring. Dev-fatal, not a swallow-able error.
    if a == nil {
        panic("read_available_cash: working state not primed (order before session init)")
    }

    // 2. READ + RETURN raw. May be < 0 (Option B). Caller judges; read never clamps.
    return a.Cash.Available
}

Why panic, not return-error. A missing working copy on the hot path is not a recoverable runtime state — it is an ordering violation (an order reached the gate before the session was primed). Returning an error would invite the caller to "handle" an impossible case and mask the bug. Panic surfaces it in dev, where it belongs — the same philosophy as the conservation-assert panics in the write primitives.

Storage. Zero I/O. The store read that makes this safe already happened once, at session init.

4. read_available_position — hot-path sell fundability (item 15)

Purpose. Return sellable units of one instrument for a sell reservation decision. The sell-side twin of §3; reserve_if_available blocks on it for every sell order.

Source. The in-memory Position.Available for the instrument — session-scoped live state (never in Postgres; positions die at close). Same load-once basis as §3.

Missing-instrument → 0. If the account holds no position in the instrument, the map key is absent — a legitimate "nothing to sell here," not an error. Returns 0, and the sell reserve refuses correctly (0 < sell_qty). This is load-bearing: it makes "you can't sell what you don't hold" fall out naturally, so the reserve flow needs no special check.

Always ≥ 0. No primitive drives position Available negative (MovePosition is sufficiency-guarded, RemovePosition burns from Reserved, CreditPosition only adds), so — unlike cash — there is no signed-value concern.

go
// read_available_position returns live sellable units of one instrument for a sell fundability
// check. Reads the in-memory working copy primed at session init. NO store hop on the hot path.
// Missing instrument -> 0 (hold none). Always >= 0.
func (a *Account) read_available_position(instrumentID InstrumentID) int64 {
    // 1. PRECONDITION — working state primed (same as §3).
    if a == nil {
        panic("read_available_position: working state not primed (order before session init)")
    }

    // 2. RESOLVE the per-instrument position.
    pos, exists := a.Positions[instrumentID]

    // 3. BRANCH — absent key is a legitimate "nothing to sell", not an error.
    if !exists {
        return 0
    }

    // 4. READ + RETURN. Always >= 0.
    return pos.Available
}

Storage. Zero I/O. Safe because of the boundary seed.

5. read_balance_snapshot — session-init seed read (item 16)

What it is. The one read that actually hits a store on the normal path. It reads the session's cash snapshot from Redis to seed (or, for on-demand, to build the read-only isolated copy). It is the "first read" that primes in-memory; §3/§4 ride the copy it establishes.

Source. Redis l5:snap:{session_id}:acct:{account_id}:cash. Fields per persistence §4.1: available, reserved, settled, bank_cash, drip_active, drip_progress. This key was populated at session bootstrap from Postgres accounts — that bootstrap write is a separate Tier-2 operation (session_load), not this read.

Why Redis, not Postgres, here. The snapshot already carries the split (available/reserved/settled). Postgres has only trading_cash (one signed figure). By the time read_balance_snapshot runs, the split lives in Redis, so this read gets it whole, with no reconciliation. (A crash-recovery reload reads this exact same key — the snapshot shape is identical whether freshly bootstrapped or mid-session.)

On-demand. Same read, read-only — the isolated copy is built from it and never written back (the single canonical/on-demand storage difference). Read-onlyness is the caller's PersistOnClose=false tag, not a different query.

go
// snapshotCash mirrors the Redis snapshot cash fields (persistence §4.1).
type snapshotCash struct {
    available    int64
    reserved     int64
    settled      int64
    bankCash     int64
    dripActive   bool
    dripProgress *int64 // nullable, mirrors accounts.drip_progress
}

// read_balance_snapshot reads the session's live cash snapshot from Redis to seed in-memory
// working state. This is the "first read" that primes the hot path; §3/§4 read the in-memory
// copy thereafter. Runs at session init (NOT the per-order hot path); one Redis GET.
func read_balance_snapshot(rds Redis, sessionID SessionID, accountID AccountID) (*Cash, error) {
    // 1. READ the session cash snapshot from Redis. Exact key/fields per persistence §4.1.
    //    KEY: l5:snap:{session_id}:acct:{account_id}:cash
    snap, err := rds.GetCashSnapshot(sessionID, accountID)

    // 2. BRANCH on result.
    switch {
    case err == ErrKeyMissing:
        // The snapshot MUST exist by this point — session bootstrap (session_load) writes it
        // (from Postgres accounts) before the session accepts orders. A missing key means init
        // ran out of order (read before bootstrap wrote). Hard stop; never fabricate a balance.
        return nil, ErrSnapshotMissing{SessionID: sessionID, AccountID: accountID}
    case err != nil:
        // Redis unreachable/timeout at init. Never seed a session from a failed money read.
        return nil, ErrLoadFailed{AccountID: accountID, Cause: err}
    }

    // 3. HYDRATE the in-memory working split directly from the snapshot (no reconciliation math —
    //    the snapshot already holds the live split, unlike Postgres which holds only trading_cash).
    c := &Cash{
        Available: snap.available,
        Reserved:  snap.reserved,
        Settled:   snap.settled,
        Bank:      snap.bankCash,
    }
    return c, nil
}

Storage. One Redis read, at session init only (off the hot path; latency irrelevant). No Postgres read here — the Postgres→Redis seeding is session_load's job, upstream.

6. Cross-cutting properties (all three reads)
No mutation, no lock, no policy. A read returns state; it never changes it, never takes a lock, never decides anything. Decisions (can this order reserve?) belong to the Tier-2 caller.
Determinism / reproducibility. §3 and §4 are pure functions of the in-memory struct — same state → same return, no clock, no randomness, no external read. §5 reads Redis once at the boundary.
Signedness passes through. read_available_cash may return negative (Option B); it is returned raw and never clamped. read_available_position is always ≥ 0.
Failure philosophy. A missing working copy on the hot path is a programming error → panic (loud, dev-fatal), not a returned error. A missing/failed store read at init is a returned error (ErrSnapshotMissing / ErrLoadFailed) → the caller fails the session start cleanly rather than fabricating a balance.
7. Deferred / corrected / abandoned (with reasons)
CORRECTED — seed store was Postgres, now Redis (locked). The earlier draft (LAYER_5_1_OPERATIONS.md §2) had read_balance_snapshot/load_account reading Postgres for the session seed. This was corrected to Redis-first: session bootstrap reads Postgres once and writes the Redis snapshot; all session-state reads then come from Redis (16) or the in-memory copy it seeds (14/15). Reason: the user's standing rule ("Redis holds current-session state; first read from a store, then in-memory") plus the persistence model (Redis snapshot carries the split; Postgres does not). Postgres is touched only at the bootstrap and session-end boundaries. Scope: this Redis-first ruling is for session state only; other reads pick their store by data type when specified.
ABANDONED — per-order store reads on the hot path. Considered making 14/15 read Redis on every order (to satisfy "reads come from a store" literally). Abandoned: a network hop on the single-goroutine matching hot path is structurally forbidden (serializes matching behind store latency), and Redis lags in-memory so it would return staler data than the local copy. Resolved by load-once: the store read happens at the boundary; the hot path reads the primed in-memory copy.
DEFERRED — C12 external read/query surface (item 17). Get balance / live P&L / trade history / cost breakdown / recent_streak. Not a Tier-1 hand — it is the HTTP-faced, Postgres-backed query API consumed by agent REACT (L2) and the human UI, composing these hands plus 5.4's computed P&L. Reason: it is a downstream Tier-2 surface; the persistence schema/indexes were built to serve it, but the surface itself is out of scope for the hands. To be designed with the Tier-2 read/HTTP work.
DEFERRED — crash-recovery read of the full working split. The read that rebuilds the complete in-memory state (cash split + positions + lots + reservation records) from the Redis snapshot after a crash lives in the lifecycle doc (crash_recovery_reload), not here — it is a Tier-2 flow, and its cross-layer capture coordination is itself deferred to the joint L5+L2+engine infra pass.