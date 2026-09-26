Layer 5 — Session Lifecycle Flows (Tier-2 Orchestration)

Scope: the Tier-2 session-lifecycle flows of Layer 5 — items 18–23 on the action list: account creation, session load (canonical + on-demand), session teardown (save + discard), and crash-recovery reload. These are flows, not hands: each makes policy decisions, performs store I/O (Postgres and/or Redis), and orchestrates — they establish, persist, and restore the in-memory working state that the Tier-1 hands (LAYER_5_1_OPERATIONS.md, LAYER_5_1_READ_OPERATIONS.md) operate on.

What this document is NOT: it is not the Tier-1 primitives (reads/writes — separate docs), not the 5.2 reservation methods (reserve_if_available, convert, release, release_excess — items 24–27, the orchestration core, specified separately), and not the persistence-layer store operations proper (write-behind buffer, drainer, snapshotter — items 46–54).

Status: items 18–23 LOCKED at design depth, with the noted OPEN items (chiefly the cross-layer snapshot-capture coordination inside crash recovery) explicitly deferred. Exact schema/key names are taken verbatim from LAYER_5_PERSISTENCE.md (§3.1 accounts, §4.1 Redis snapshot keys).

0. Orientation — the two tiers, and where these flows sit
TIER 2 (this doc): open_account, session_load, ondemand_session_load,
                   session_save, session_discard, crash_recovery_reload
        │  decide, hit stores (Postgres + Redis), call ↓
TIER 1 (hands):    reads (read_available_cash/position, read_balance_snapshot)
                   writes (MoveCash, MovePosition, Credit/RemovePosition, Credit/DebitCash)
        │  read / mutate ↓
        in-memory working Account  ◄──seed/mirror/writeback──►  Redis (session) / Postgres (durable)

The lifecycle arc these flows form:

open_account (18) ──INSERT──► Postgres accounts row              [durable birth, once per lifetime]
                                      │
session entry ──► session_load (19) / ondemand_session_load (20)
                     read Postgres accounts (once)  →  hydrate in-memory split  →  write Redis snapshot
                                      │
                during session: hands mutate in-memory; snapshot mirrored to Redis on cadence;
                                closed trades → Redis write-behind buffer → Postgres (async)
                                      │
clean end ──► session_save (21, canonical)  : reconcile split → UPDATE accounts (1 txn) → discard snapshot
         └──► session_discard (22, on-demand): discard snapshot; NO Postgres write, ever
                                      │
crash mid-session ──► crash_recovery_reload (23): rebuild FULL split from Redis snapshot
                                                  (cross-layer coordination = OPEN)

The store-choice spine (from LAYER_5_PERSISTENCE.md): Postgres = durable money source-of-truth, touched at the creation/load/save boundaries only. Redis = current-session snapshot (crash recovery), written at load, mirrored during, discarded at clean close, read at crash recovery. In-memory = the authoritative live working truth during the session. The single storage-level difference between canonical and on-demand is one boolean, PersistOnClose — canonical writes back at end, on-demand suppresses it.

1. Shared types
go
type AccountID    = string
type InstrumentID = string
type SessionID    = string

type OwnerType int
const (
    OwnerAgent OwnerType = iota // 'agent'
    OwnerHuman                  // 'human'
)

type SessionType int
const (
    Canonical SessionType = iota
    OnDemand
)

// Account — the in-memory working state (full definition in LAYER_5_1_OPERATIONS.md §0.4).
// Reproduced fields relevant to lifecycle:
type Account struct {
    ID             AccountID
    Cash           Cash
    Positions      map[InstrumentID]*Position
    PersistOnClose bool          // canonical -> true; on-demand -> false
    DripActive     bool
    DripProgress   *int64        // nullable
}

type Cash struct {
    Available int64 // MAY be negative (Option B)
    Reserved  int64 // >= 0
    Settled   int64 // >= 0
    Bank      int64 // >= 0
}

The durable Postgres row this cluster reads/writes (schema §3.1, verbatim):

sql
CREATE TABLE accounts (
    account_id     TEXT        PRIMARY KEY,
    owner_type     TEXT        NOT NULL,              -- 'agent' | 'human'
    owner_id       TEXT        NOT NULL,              -- agent_id or user_id
    trading_cash   BIGINT      NOT NULL,              -- SIGNED (Option 4), coins
    bank_cash      BIGINT      NOT NULL DEFAULT 0,    -- >= 0
    drip_active    BOOLEAN     NOT NULL DEFAULT FALSE,
    drip_progress  BIGINT,                            -- nullable; NULL when inactive
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_accounts_owner UNIQUE (owner_type, owner_id),
    CONSTRAINT ck_bank_nonneg    CHECK (bank_cash >= 0)
);

The Redis session-snapshot keys this cluster writes/reads/deletes (schema §4.1, verbatim):

l5:snap:{session_id}:acct:{account_id}:cash =
    { available, reserved, settled, bank_cash, drip_active, drip_progress }
l5:snap:{session_id}:acct:{account_id}:pos:{instrument_id} =
    { available_units, reserved_units, open_lots:[{entry_price, entry_at, remaining_qty}] }
l5:snap:{session_id}:acct:{account_id}:resv:{order_id} =
    { participant, side, is_market, reserved_cash|reserved_units, remaining_qty }
2. open_account (item 18) — participant account creation

What it is. The one operation that brings an account into existence. Called at agent CREATE (Layer 2, when an agent is spawned) and human signup (platform layer). Writes one row into Postgres accounts and returns a handle (account_id) the caller stores. Every money operation FKs to this account_id, so nothing about a participant's money can happen until this has run. There is no session here — this writes the durable record directly to Postgres, before any session loads it.

Static starting values come from config, not hardcoded. Starting capital and starting bank (and any other field the economy doc calls "static") are resolved by the caller from platform config and passed in. open_account is policy-free — it records what it is handed. Absent config → 0 (schema default). Reason: the economy doc leaves these as basis-fixed, value-tunable dials (flat for the prototype, menu-selectable in the product version); hardcoding would bake a prototype number into the creation primitive and block the menu path.

Signature

go
func open_account(db DB, ownerType OwnerType, ownerID string,
                  startingCapital int64, startingBank int64) (AccountID, error)

THE FLOW

open_account(db, ownerType, ownerID, startingCapital, startingBank):

 1. VALIDATE inputs
    ├─ ownerType not in {agent,human}   → return "", ErrBadOwnerType
    ├─ ownerID == ""                    → return "", ErrBadOwnerID
    ├─ startingCapital < 0              → return "", ErrNegativeStartingCapital
    │     // trading_cash is signed for LATER debt (Option 4), NEVER at birth. Negative start = bug.
    ├─ startingBank < 0                 → return "", ErrNegativeStartingBank
    │     // bank_cash has CHECK (>= 0); the bank never goes negative, least of all at birth.
    └─ ok

 2. GENERATE account_id
        account_id = new opaque unique id (UUID/ULID)   // PK is TEXT; generated here, returned as handle

 3. WRITE to Postgres — INSERT the durable row (single atomic statement)
        INSERT INTO accounts
            (account_id, owner_type, owner_id, trading_cash, bank_cash, drip_active, drip_progress)
        VALUES
            ($1,         $2,         $3,       $4,           $5,        FALSE,       NULL)
        // trading_cash = startingCapital; bank_cash = startingBank (config-sourced, 0 if none)
        // drip_active FALSE, drip_progress NULL — no account drips at birth
        // created_at/updated_at default to now()

 4. BRANCH on the write result
    ├─ success                              → return account_id, nil
    ├─ UNIQUE violation on uq_accounts_owner → return "", ErrAccountExists{ownerType, ownerID}
    │     // "one account per participant" firing at the DB level — NOT a silent no-op, NOT a
    │     // second account. Caller decides (usually: fetch the existing handle).
    └─ any other DB error                    → return "", ErrCreateFailed{cause}

Why each step

Step 1 birth-time floors. trading_cash is signed so debt can accrue during trading (Option 4), but an account is never born in debt — a negative start is always a caller bug. bank_cash carries a DB CHECK (>= 0); the bank never goes negative at all.
Step 3 direct Postgres, no Redis. Creation is a durable event with no session; Redis (session-scoped) has no role. The Redis snapshot appears later, when a session loads this account.
Step 4 UNIQUE-as-arbiter (no check-then-insert). We do not pre-check existence then insert — that races (two concurrent creates both pass the check). The DB constraint uq_accounts_owner (owner_type, owner_id) is the single arbiter; we translate its violation into ErrAccountExists. The caller (agent re-spawn / re-login) then fetches the existing handle rather than treating it as fatal. This is the same "structural, not conventional" discipline as the hands' legal-move sets.

Should account creation cache to Redis? No — and no check-then-DB. An account is durable, cross-session data — by definition Postgres-owned; Redis is explicitly "not source of truth" and gets wiped on close, so caching the durable row there would put truth in the wrong store. And the account row is read only ~twice per session (load, save), both off the hot path — there is no high-frequency read to cache away from. A Redis existence pre-check would also reintroduce the exact check-then-write race the UNIQUE constraint already eliminates, adding a stale-cache failure mode for zero correctness gain (negative engineering). Existence is enforced by the Postgres constraint at write time and resolved by a Postgres lookup at read time (a small get_account_handle the caller runs on ErrAccountExists).

Persistence. Reads: none (attempt the insert; let UNIQUE reject a duplicate). Writes: exactly one Postgres INSERT (atomic; no txn wrapper needed). Redis: untouched. Write-behind buffer: not involved (that is for high-frequency closed-trade rows; creation is rare and durability-critical → straight to Postgres).

Concurrency / idempotency. Two simultaneous creates for the same participant → one wins, the other gets the UNIQUE violation → ErrAccountExists; no double account, no lost write. Not idempotent by account_id (each call mints a new id) but idempotent by participant via the constraint — you cannot end with two accounts for one participant. Correct caller pattern on ErrAccountExists: look up and use the existing handle.

3. session_load (item 19) — bootstrap a durable account into a live canonical session

What it is. The bootstrap that turns a durable Postgres account into a live, in-memory working session. Runs once per session start, before the session accepts any order — triggered when a participant (human or agent) enters or tries to enter a session. It reads the durable accounts row from Postgres, hydrates the in-memory working split, and writes the initial Redis snapshot — the exact snapshot read_balance_snapshot (16) later reads. This is what makes "first read from a store, then in-memory" true: it is the boundary that primes the entire read model.

Hydration synthesizes the fresh split. Postgres holds only trading_cash (one signed figure), no available/reserved/settled split. A fresh session has nothing reserved or realized yet, so the split is synthesized deterministically — this happens here, once, and its result is written to Redis. From then on the split evolves in-memory and is mirrored back; it is never re-synthesized. (Contrast crash_recovery_reload (23), which restores an already-evolved split from Redis and does not synthesize.)

Signature

go
func session_load(db DB, rds Redis, sessionID SessionID,
                  accountID AccountID, sessionType SessionType) (*Account, error)

THE FLOW

session_load(db, rds, sessionID, accountID, sessionType):

 1. READ durable account from Postgres (exact cols per §3.1)
        SELECT trading_cash, bank_cash, drip_active, drip_progress
        FROM accounts WHERE account_id = $1

 2. BRANCH on read result
    ├─ row found                → go to 3
    ├─ no row (ErrNoRows)       → return nil, ErrNoAccount{accountID}
    │     // NO lazy-create (locked). Account must exist (open_account, 18).
    │     // canonical: should exist → real error. on-demand path uses §4, same no-create rule.
    └─ query error              → return nil, ErrLoadFailed{accountID, cause}
          // never seed a session from a failed money read. Session bootstrap fails; no working state.

 3. HYDRATE in-memory working state (SYNTHESIZE the fresh split; positions empty)
        a := &Account{
            ID: accountID,
            Cash: Cash{
                Available: row.trading_cash,   // all durable cash starts available
                Reserved:  0,                  // fresh session — nothing locked
                Settled:   0,                  // fresh session — nothing realized
                Bank:      row.bank_cash,
            },
            Positions:    make(map[InstrumentID]*Position),   // born flat (positions never persist)
            DripActive:   row.drip_active,
            DripProgress: row.drip_progress,
        }

 4. TAG write-back policy (the ONE canonical/on-demand difference)
        a.PersistOnClose = true   // canonical: session-end writes back to Postgres (item 21)

 5. WRITE the initial Redis snapshot (this is what read_balance_snapshot/16 reads)
        SET l5:snap:{sessionID}:acct:{accountID}:cash = {
            available: a.Cash.Available, reserved: 0, settled: 0,
            bank_cash: a.Cash.Bank, drip_active: a.DripActive, drip_progress: a.DripProgress,
        }
        // NO position keys yet — account is born flat; l5:snap:...:pos:{instrument_id} appears
        // only when a position is first opened.

 6. BRANCH on snapshot write result
    ├─ success            → return a, nil
    └─ Redis write error  → return nil, ErrSnapshotWriteFailed{sessionID, accountID, cause}
          // a session with no snapshot is unrecoverable on crash. Refuse to start THIS participant's
          // session rather than run it silently-unrecoverable. Durable account untouched (read-only
          // here) → nothing lost; the participant simply retries. Blast radius = one session entry.

Why each step

Step 1 Postgres, not Redis. For a fresh session Redis has nothing yet (the snapshot is written in step 5). So the seed must come from Postgres. This reconciles the two docs: fresh-start seed = Postgres (persistence §5); the Redis-first reads (16/14/15) work because step 5 populates Redis from this Postgres read.
Step 2 no lazy-create. A missing row is ErrNoAccount (creation is open_account's sole job); a store failure is ErrLoadFailed. Both fail the start cleanly, never fabricate a balance.
Step 3 synthesize once. The split is born here from the single durable figure — a computation, so it lives in this flow, not a hand. Positions start empty (intraday; every session begins flat, STL-1).
Step 4 the write-back tag. Both session types read Postgres identically and both get a Redis snapshot; the only storage-level difference is this boolean.
Step 5 establish the snapshot → unblock the reads. This is the precondition for the entire read path: without it, read_balance_snapshot (16) finds a missing key and hard-stops. Order: Postgres-read → hydrate → Redis-write.
Step 6 hard-fail scope. Failure fails that one participant's session start (not system-wide). The durable account is untouched (read-only in this flow), so nothing is corrupted or lost — the participant retries. (On-demand may relax this — see §4.)

Persistence. Reads: one Postgres read (off hot path). Writes: one Redis write (initial snapshot). No Postgres write (the durable row already exists from creation). Sequence: Postgres-read → in-memory hydrate → Redis-write (last, because it mirrors the hydrated state).

Concurrency / idempotency. Single-active-session-per-participant (locked) means no concurrent load for the same account. session_load assumes it runs exactly once per session; enforcing "one load per session" is the session-manager's responsibility (same layer that enforces single-active-session), above L5.

4. ondemand_session_load (item 20) — isolated read-only session init

What it is. The on-demand variant of session load: reads the durable account read-only, builds an isolated, throwaway working copy, and runs the session with no write-back path to Postgres. At session end everything is discarded (item 22); the durable account never learns the session happened. This is "on-demand starting balance = live snapshot of the trading account, read-only" (economy §206) and "the single suppressed write-back is the only storage-level difference" (persistence §5).

Signature

go
func ondemand_session_load(db DB, rds Redis, sessionID SessionID,
                           accountID AccountID) (*Account, error)

No sessionType parameter — this flow is the on-demand path.

Seed source — Postgres durable figure (Option A, locked). The seed comes from the Postgres accounts row (the participant's real balance), read-only. Considered and set aside: seeding from a live canonical session's in-flight state (Option B). Reason A was chosen: on-demand is a practice/isolated mode; seeding from the clean durable figure is deterministic and dependency-free, and "live snapshot" (§206) most plausibly means "a snapshot of your real balance" (vs a fake fixed practice balance), not "must reflect an in-flight canonical session's unrealized P&L." Under the locked single-active-session rule a participant cannot have a canonical session live while starting on-demand, so A and B collapse anyway. (If on-demand is ever made concurrent-with-canonical and required to reflect its live state, add B's branch.)

THE FLOW

ondemand_session_load(db, rds, sessionID, accountID):

 1. READ durable account from Postgres — READ-ONLY (identical query to §3; exact cols §3.1)
        SELECT trading_cash, bank_cash, drip_active, drip_progress
        FROM accounts WHERE account_id = $1
        // "read-only" is enforced by step 4 (PersistOnClose=false), not by the query.

 2. BRANCH on read result
    ├─ row found          → go to 3
    ├─ no row (ErrNoRows) → return nil, ErrNoAccount{accountID}
    │     // on-demand REQUIRES an existing durable account to snapshot from. Missing = caller bug
    │     // (can't practice-trade an account that was never created). No lazy-create.
    └─ query error        → return nil, ErrLoadFailed{accountID, cause}

 3. HYDRATE an ISOLATED working copy (freshly allocated — aliases nothing)
        a := &Account{                             // NEW struct — shares no pointer with any canonical copy
            ID: accountID,
            Cash: Cash{
                Available: row.trading_cash, Reserved: 0, Settled: 0, Bank: row.bank_cash,
            },
            Positions:    make(map[InstrumentID]*Position),   // born flat
            DripActive:   row.drip_active,
            DripProgress: row.drip_progress,
        }

 4. TAG write-back policy — SUPPRESSED (the defining on-demand property)
        a.PersistOnClose = false   // session-end DISCARDS (item 22); NO Postgres write-back, ever.

 5. WRITE the initial Redis snapshot (crash recovery within the on-demand session's own life)
        SET l5:snap:{sessionID}:acct:{accountID}:cash = { available, reserved:0, settled:0,
            bank_cash, drip_active, drip_progress }
        // on-demand IS snapshotted (Redis-recoverable within life; persistence §251).

 6. BRANCH on snapshot write result
    ├─ success            → return a, nil
    └─ Redis write error  → TOLERABLE for on-demand: an on-demand crash-loss costs nothing (never
                            writes back, isn't a record). Log and continue with the in-memory copy.
                            (Ties back to the item-19 hard-fail decision; on-demand is the relaxation.)

Two isolation guarantees (structural, not conventional).

No shared memory during the session — step 3 news up a fresh *Account; it shares no pointer with any canonical working state, so an on-demand trade cannot reach the real balance (no aliasing path). Every hand it calls mutates this struct.
No write-back after the session — PersistOnClose=false routes session-end to discard (22), not save (21). Even the result of on-demand trading never reaches Postgres.

On-demand ⇒ no durable trade record (rule originates here). On-demand closed trades must not write to participant_trades (durable Postgres). The realization path (5.4) must check PersistOnClose (or session type) and skip the write-behind buffer enqueue for on-demand trades. The trade still computes — the participant sees practice P&L in-session (from in-memory/Redis while live) — but it never reaches the durable log. So on-demand P&L is real within the session and invisible after it. (Nailed down fully in the realization/persistence write path, items 44/49; the rule starts here.)

Persistence. Reads: one Postgres read (read-only). Writes to Postgres: none, ever (not at load, during, or close). Writes to Redis: initial snapshot + live mirroring, all discarded at close. Write-behind buffer: skipped for on-demand trades.

Concurrency. If on-demand is ever exempt from single-active-session (runs alongside canonical), the two working copies use different session_ids → distinct snapshot keys (l5:snap:{session_id}:...), no collision; plus fresh-struct isolation → no interference. If not exempt, moot (one session at a time).

5. session_save (item 21) — canonical write-back

What it is. The canonical session's teardown: writes the final durable money state (trading_cash, bank_cash, drip) from the in-memory working copy back into Postgres accounts, in one transaction, then discards the Redis snapshot. The operation on-demand suppresses. Runs once, at clean session end, off the hot path.

Precondition — the session is already flat. By the time save runs, auto-square (STL-3, 5.4) has already run: every open position is squared to zero and each closing trade already written to the write-behind buffer (→ participant_trades). So save deals with no positions — they're gone, never persist to Postgres anyway (§3.4), and their P&L already landed in the trade log. What remains is the durable capital only. Ordering is load-bearing: auto-square → trades flushed → THEN session_save. Save is the last teardown step; if it ran before auto-square, trading_cash would miss the final settlement.

Reconciliation — the split collapses to one number. In-memory cash is a split; Postgres holds one signed trading_cash. At clean close the session is flat, so Reserved == 0, and:

trading_cash (durable) = Available + Settled     // Reserved is 0 at clean close (asserted)

Reserved must be zero at clean close — a non-zero value means a resting order survived teardown (a bug), asserted rather than silently folded in.

Signature

go
func session_save(db DB, rds Redis, sessionID SessionID, a *Account) error

THE FLOW

session_save(db, rds, sessionID, a):

 1. GUARD — only canonical sessions write back
    ├─ a.PersistOnClose == false → return ErrNotCanonical (caller should route on-demand to discard)
    │     // defense-in-depth: on-demand must NEVER reach a Postgres write. If mis-routed here, refuse
    │     // rather than corrupt the durable account.
    └─ ok

 2. ASSERT clean-close precondition (session is flat)
    ├─ a.Cash.Reserved != 0                       → panic("session_save: reserved cash at close")
    ├─ any position with Available|Reserved > 0   → panic("session_save: unsquared position at close")
    │     // auto-square (STL-3) must have run first. Non-flat at save = ordering bug. Dev-fatal.
    └─ ok

 3. RECONCILE the durable figure from the split
        final_trading_cash  = a.Cash.Available + a.Cash.Settled   // Reserved == 0 (asserted)
        final_bank_cash     = a.Cash.Bank
        final_drip_active   = a.DripActive
        final_drip_progress = a.DripProgress   // nullable

 4. WRITE back to Postgres in ONE transaction
        BEGIN
          UPDATE accounts
             SET trading_cash  = $1,   -- SIGNED, may be negative (Option 4) — no floor/clamp
                 bank_cash     = $2,
                 drip_active   = $3,
                 drip_progress = $4,
                 updated_at    = now()
           WHERE account_id   = $5
        COMMIT

 5. BRANCH on the write result
    ├─ commit success  → go to 6
    └─ txn error       → return ErrSaveFailed{accountID, cause}
          // do NOT discard the snapshot on a failed save (skip step 6): the snapshot is the only
          // remaining copy of the final state. Losing save AND snapshot would lose the session.

 6. DISCARD the Redis snapshot (clean close — the safety net is obsolete once the durable write committed)
        DEL l5:snap:{sessionID}:acct:{a.ID}:cash
        DEL l5:snap:{sessionID}:acct:{a.ID}:pos:*     // any position keys (should be none post-square)
        DEL l5:snap:{sessionID}:acct:{a.ID}:resv:*    // any reservation keys (should be none)
        // discard failure is non-fatal — stale session keys can expire via TTL.

 7. return nil

Why each step

Step 1 guard — on-demand suppression enforced structurally at the write boundary; a mis-route can never corrupt the durable account.
Step 2 assert flat — the ordering contract with 5.4. Save is last; auto-square precedes it. Reserved cash or a live position at save time means the sequence broke → dev-fatal.
Step 3 reconcile — collapses the three-way split into the single signed durable figure (Available + Settled, Reserved zero by assert). This is where the session's entire realized outcome lands in one number.
Step 4 one transaction — durability-critical; a single atomic UPDATE of all four durable fields. trading_cash may be negative (Option 4) — no floor, no clamp.
Step 5 don't-discard-on-failure — if the Postgres write fails, the Redis snapshot is the only surviving copy; keeping it preserves recoverability/retry. Discarding it on a failed save is the one move that actually loses money state.
Step 6 discard last, non-fatal — the snapshot is obsolete only after the durable write commits; Postgres before Redis-discard, always.

Persistence. Reads: none from a store (reads the in-memory copy — authoritative and ahead of both stores). Writes: one Postgres transaction (accounts UPDATE) + Redis snapshot deletion. No write-behind buffer (trades already flushed during the session). Sequence: in-memory reconcile → Postgres commit → Redis discard.

6. session_discard (item 22) — on-demand teardown

What it is. The on-demand session's teardown: drops the in-memory copy and the Redis snapshot, and writes nothing to Postgres. session_save with the entire durable-write half removed. The isolated session vanishes without a trace; the durable account is exactly as before.

Signature

go
func session_discard(rds Redis, sessionID SessionID, a *Account) error

No db parameter — deliberately no Postgres access, so "writes nothing durable" is structural: the flow cannot write to Postgres (no handle).

THE FLOW

session_discard(rds, sessionID, a):

 1. GUARD — only on-demand sessions discard without saving
    ├─ a.PersistOnClose == true → panic("session_discard: canonical must save, not discard")
    │     // a canonical session reaching discard would silently LOSE money progress (snapshot
    │     // deleted too, no error) = irreversible data loss. Dev-fatal — loudest possible signal.
    └─ ok

 2. DISCARD the Redis snapshot (no durable write — the whole point)
        DEL l5:snap:{sessionID}:acct:{a.ID}:cash
        DEL l5:snap:{sessionID}:acct:{a.ID}:pos:*
        DEL l5:snap:{sessionID}:acct:{a.ID}:resv:*

 3. DROP the in-memory working copy
        // caller releases its reference to `a`; GC reclaims it. Nothing to persist.

 4. return nil

Why the guard is a panic (not a returnable error). The two teardown guards are deliberately asymmetric. session_save catches a mis-routed on-demand session with a returnable guard — refusing to write is safe. session_discard catches a mis-routed canonical session with a panic — because discarding a canonical session silently throws away real money progress with no recovery (irreversible data loss). Together (save refuses on-demand, discard panics on canonical) the routing is bidirectionally safe.

Completes the on-demand no-footprint promise. Combined with the realization path skipping the buffer enqueue for on-demand (rule from §4), the on-demand session leaves zero durable footprint: no account update, no trade rows, no cost rows. It computed real P&L in-session (visible while live) and all of it evaporates at discard.

Persistence. Reads: none. Writes to Postgres: none, ever (no handle). Writes to Redis: only deletions (snapshot cleanup).

7. crash_recovery_reload (item 23) — restore a live session from Redis — partial; coordination OPEN

What it is. After a process crash mid-session, rebuilds the full in-memory working state from the last Redis snapshot so the session can resume. Counterpart to session_load (19), but sources Redis, not Postgres, and restores the complete live working split (cash split + positions + open lots + reservation records) — not a fresh synthesized split. This is the one read that legitimately treats Redis as its primary source (the crash case the read-model rule carves out).

Status: the L5 slice is specifiable; the cross-layer coordination is OPEN. This flow restores L5's state from L5's snapshot keys correctly in isolation. What is not closeable here: ensuring L5's restored cash/positions/reservations are mutually consistent with the engine's rebuilt order book and L2's agent population, all captured at one consistent tick. The persistence doc explicitly defers that multi-layer capture choreography to the joint L5+L2+engine infra pass (§4.1 note, §5).

Difference from session_load — restore, don't synthesize. session_load reads one durable number and synthesizes a fresh split (Available=trading_cash, Reserved=0, Settled=0, positions empty). crash_recovery_reload reads the already-evolved split verbatim: mid-session Reserved/Settled may be non-zero, positions and lots populated, resting orders' reservation records present. It restores exactly the state at the last snapshot cadence tick before the crash. No synthesis, no reconciliation — the snapshot is the working state.

Signature

go
func crash_recovery_reload(rds Redis, sessionID SessionID, accountID AccountID,
                           sessionType SessionType) (*Account, error)

THE FLOW (L5 slice)

crash_recovery_reload(rds, sessionID, accountID, sessionType):

 1. READ the cash snapshot from Redis (exact key/fields §4.1)
        GET l5:snap:{sessionID}:acct:{accountID}:cash
            → { available, reserved, settled, bank_cash, drip_active, drip_progress }

 2. BRANCH on result
    ├─ found        → go to 3
    ├─ key missing  → return nil, ErrNoSnapshot{sessionID, accountID}
    │     // no snapshot to recover from; session cannot resume. Caller policy (not this flow's call):
    │     //   canonical  → fall back to a FRESH session_load from Postgres (lose in-session progress,
    │     //                recover the durable account)
    │     //   on-demand  → session is simply gone (nothing durable was at stake)
    └─ redis error  → return nil, ErrLoadFailed{accountID, cause}

 3. RESTORE the cash split VERBATIM (no synthesis — this is the evolved mid-session state)
        a := &Account{
            ID: accountID,
            Cash: Cash{ Available: snap.available, Reserved: snap.reserved,
                        Settled: snap.settled, Bank: snap.bank_cash },
            Positions:    make(map[InstrumentID]*Position),
            DripActive:   snap.drip_active,
            DripProgress: snap.drip_progress,
        }

 4. READ + RESTORE all position snapshots for this session/account
        SCAN keys l5:snap:{sessionID}:acct:{accountID}:pos:*
        for each pos:{instrument_id}:
            GET → { available_units, reserved_units, open_lots:[{entry_price, entry_at, remaining_qty}] }
            a.Positions[instrument_id] = &Position{
                Available: available_units, Reserved: reserved_units,
                OpenLots:  open_lots,          // FIFO queue restored in arrival order (verbatim)
            }

 5. READ + RESTORE all reservation records (5.2 state) for this session/account
        SCAN keys l5:snap:{sessionID}:acct:{accountID}:resv:*
        for each resv:{order_id}:
            GET → { participant, side, is_market, reserved_cash|reserved_units, remaining_qty }
            restore into 5.2's reservation-record map (keyed by order_id)
        // L5 can rebuild its OWN reservation records. Wiring them back to the engine's rebuilt
        // resting orders is part of the DEFERRED cross-layer coordination.

 6. TAG write-back policy (same as load)
    ├─ Canonical → a.PersistOnClose = true
    └─ OnDemand  → a.PersistOnClose = false

 7. return a, nil    // L5 state restored. Cross-layer consistency = OPEN (deferred joint pass).

Why each step

Step 1/3 restore, not synthesize — the snapshot holds the live split as of the last cadence write; copied verbatim. This is why the snapshot carries reserved/settled (which a fresh load zeroes): mid-session they are non-zero, and recovery must preserve them or resting orders and spent principal would be lost.
Step 4 positions + lots verbatim, FIFO preserved — open_lots restored in arrival order so FIFO matching resumes correctly. One structure serving position + position_age + FIFO (§4.1), recovered in one read per instrument.
Step 5 reservation records restored, wiring deferred — L5 rebuilds its own map from resv:* keys; lining those up with the engine's restored book is the cross-layer problem (OPEN).

The snapshot cadence gap (stated honestly). The snapshot is written on a cadence, not per mutation (the hands do zero I/O). So a crash loses everything between the last snapshot tick and the crash; recovery restores to the last snapshot, not the exact crash instant. Inherent and accepted (the persistence model). How tight the cadence is, is a tuning/infra question in the deferred pass.

Persistence. Reads: Redis only — one cash key, N position keys (scan), M reservation keys (scan). No Postgres (the snapshot supersedes the durable row for the live session). Writes: none in this flow (it rebuilds in-memory; the resumed session then continues mirroring to Redis as normal).

8. Cluster summary (18–23)
#	Flow	Trigger	Postgres	Redis	In-mem	Key property
18	open_account	agent CREATE / human signup	INSERT accounts	—	—	UNIQUE-as-arbiter; no Redis; config-sourced starts
19	session_load	canonical session entry	SELECT accounts (1×)	write initial snapshot	synthesize fresh split	Postgres→hydrate→Redis; hard-fail scoped to one entry
20	ondemand_session_load	on-demand session entry	SELECT accounts (read-only)	write initial snapshot	isolated fresh copy	PersistOnClose=false; no durable footprint
21	session_save	clean canonical end	UPDATE accounts (1 txn)	discard snapshot	reconcile split→number	assert flat; Postgres before Redis-discard; don't-discard-on-fail
22	session_discard	clean on-demand end	none (no handle)	discard snapshot	drop copy	canonical→discard = panic (data loss)
23	crash_recovery_reload	crash mid-session	none	read snapshot (restore)	rebuild full split	restore≠synthesize; cross-layer coordination OPEN
9. Deferred / abandoned / open (with reasons)

OPEN — cross-layer snapshot-capture coordination (in item 23). L5, engine, and L2 snapshotting at one consistent tick, so restored L5 cash/positions/reservations agree with the engine's rebuilt book and L2's population. Deferred to the joint L5+L2+engine infra pass (persistence §4.1/§5). crash_recovery_reload restores the L5 slice correctly; the multi-layer join is not designed here.

OPEN — snapshot cadence / capture trigger (write side). How often the Redis snapshot is written, and the mechanism (items 51/53 — the snapshotter). This lifecycle doc covers the read/restore side (23) and the initial write at load (19/20) and discard (21/22); the periodic mid-session snapshot write is a separate persistence-layer operation. Recovery is "last snapshot," not "last mutation," because of this.

OPEN — post-recovery caller policy (in item 23). On ErrNoSnapshot, whether canonical falls back to a fresh Postgres session_load (recovering the durable account but losing in-session progress) is a caller decision, noted not decided here.

DEFERRED — get_account_handle lookup (touched in item 18). The Postgres SELECT account_id FROM accounts WHERE owner_type=? AND owner_id=? a caller runs on ErrAccountExists (agent re-spawn / human re-login) to fetch the existing handle. A small durable read; noted as the correct pattern, its own spec belongs with the durable-read/query work, not the creation flow.

ABANDONED — Redis caching of the account row / check-then-DB existence test (in item 18). Considered storing the created account in Redis and checking Redis-then-Postgres for existence. Abandoned: the account is durable data (Postgres-owned; Redis is not source-of-truth and is wiped on close), it is read only ~twice per session (no hot read to cache away from), and a Redis pre-check reintroduces the exact check-then-write race the uq_accounts_owner UNIQUE constraint already eliminates — adding a stale-cache failure mode for zero correctness gain. The DB constraint is the sole arbiter of one-account-per-participant.

Snapshot-write-failure relaxation for on-demand (item 20 vs 19). Canonical session_load hard-fails the session start if the initial snapshot write fails (unrecoverable-on-crash is unacceptable for durable money). On-demand tolerates it (log-and-continue), because an on-demand crash-loss costs nothing (never writes back, isn't a record). This is a deliberate, reasoned divergence, not an inconsistency.

Seed-source Option B (item 20). Seeding on-demand from a live canonical session's in-flight state was considered and set aside in favor of Option A (Postgres durable figure), which is correct under single-active-session and defensible regardless. B is re-openable only if on-demand becomes concurrent-with-canonical and must reflect its live state.