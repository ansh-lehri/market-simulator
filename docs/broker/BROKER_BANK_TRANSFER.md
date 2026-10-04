Broker — Bank Transfer (A6, Tier 3)

Scope: the user moving cash between their trading account and their bank — push (Available → Bank, "bank profit") and pull (Bank → Available, "pull buffer"), in both directions. One consumer-facing HTTP endpoint, two internal branches: an in-session transfer on a live account (via L5 MoveCash, on the shard) and a no-session durable transfer on the Postgres row (for the on-demand-qualify pull and any idle transfer).

Status: both stages LOCKED.

Depends on:

L5 MoveCash(from, to Bucket, amount int64) error — the cash-transfer primitive (LAYER_5_1_OPERATIONS.md §1.1, LOCKED). Conserving; legal set includes Available↔Bank; fails before mutating; runs on the owner goroutine.
L5 economy §3.4 — bank transfers are user-initiated, no limits, allowed anytime including mid-position, no artificial friction.
Broker Tier 0 funnel (hash(AccountID) % N, route() + buffered-1 reply) and the Tier-4 HTTP handler pattern (no auth, explicit account_id, handler-owns-timeout, error→status).
Persistence MarkSnapshotDirty(sessionID, accountID) (economy/persistence).

Key structural fact (from LAYER7_TIER0 T0.12 + broker read-split): an account is live on a shard only during a session. Off-session there is no in-memory Account — only the durable Postgres row. So a transfer has two mechanically different paths, and which one runs is decided by whether the account is live, which is shard-owned state — so the endpoint routes to the shard first and only falls to the durable path on a "not live" miss.

The endpoint (shared by both stages)
POST /v1/account/transfer
body: { account_id, direction: "to_bank" | "to_trading", amount, unit }

No auth; account_id in the body (Tier-4 convention). amount is a raw positive integer with a unit indicator. direction maps to MoveCash buckets: to_bank → (Available, Bank) (push), to_trading → (Bank, Available) (pull).

Control flow — route-first, durable-on-miss:

HTTP handler:
  validate (account_id present, direction legal, amount > 0) → else 400
  route(Transfer{...}) → shard                       // ask the owner: are you live?
  on reply:
    Present:true   → 200 { available, bank }          // Stage 1 handled it live
    Present:false  → durableTransfer(...) (Postgres)  // Stage 2 — off-shard

The route is not wasted work — it is how the server authoritatively learns the account is off-session. The client can't be trusted to declare "I'm session-less" (a stale client would trigger a durable write on a live account, corrupting it). Only the shard holds the liveness truth, so we always ask it first. The "route and miss" is a channel send + map lookup + reply — microseconds, no I/O.

Stage 1 — In-session transfer (via MoveCash)

The account is live on a shard, so the transfer is one MoveCash command on the owner.

go
type Transfer struct {
    AccountID AccountID
    From, To  Bucket
    Amount    int64
    Reply     chan TransferResult      // buffered-1
}
func (c Transfer) Account() AccountID { return c.AccountID }
shard.handleTransfer(c):                              // ON the owner goroutine
  1. sess, live := s.sessions[c.AccountID]
     if !live → c.Reply <- TransferResult{Present:false} ; return   // → Stage 2
  2. err := sess.account.Cash.MoveCash(c.From, c.To, c.Amount)      // L5 op 1 — conserving, in-memory
        err != nil → c.Reply <- TransferResult{Err:err} ; return     // returns BEFORE mutating on failure
  3. s.persist.MarkSnapshotDirty(sess.binding.SessionID, c.AccountID) // mark money-state dirty (both args)
  4. c.Reply <- TransferResult{Present:true,
        Available: sess.account.Cash.Available, Bank: sess.account.Cash.Bank}

MoveCash runs on the owner (its requirement), touches only Available/Bank (never Reserved), so no open order's reservation is disturbed — which is why economy §3.4 allows a pull mid-position. MarkSnapshotDirty takes both sessionID and accountID (per the economy/persistence seam) — the broker marks here because MoveCash is a raw hand that persists nothing itself (unlike reserve/realize, which mark their own state).

Response mapping: Present:false → 409 (→ Stage 2); ErrInsufficient → 422; other Err → 400/500; ctx timeout → 504; success → 200 { available, bank }.

Edge cases:

No live session → 409 → falls to Stage 2. The Stage-1/Stage-2 boundary.
Insufficient funds (pull > bank, or push > available) → MoveCash ErrInsufficient, no mutation → 422.
Amount ≤ 0 / bad direction → 400 at the handler, before the funnel.
Mid-position transfer → allowed; MoveCash never touches Reserved.
Concurrent transfer + order, same account → both are shard commands on one serial loop → serialized, no race.
Live session is on-demand (isolated) → MoveCash works on the isolated copy; real in-session, but never persists (PersistOnClose=false). No special handling.
Wedged/slow shard → handler ctx deadline → 504; the buffered-1 reply means the shard's later send never blocks it.

Persistence: in-memory MoveCash + MarkSnapshotDirty (a flag, no I/O). Canonical: the eventual session_save writes the moved balances to Postgres. On-demand: never persists. No new durable write of its own.

Stage 2 — No-session durable transfer

No live account exists (chiefly the on-demand-qualify pull: the gate bounced an idle user, and under XOR nothing of theirs is in memory). MoveCash needs a live account, so this path is a direct, conserving mutation on the Postgres row — off-shard, in the family of EnsureAccount / QualifyOnDemand. Both directions supported off-session.

This is NEW — L5 has no durable transfer method (its transfer primitive is the in-memory MoveCash; L5's durable writes are open_account and session_save only). Broker-owned, alongside get_account_handle:

broker.durableTransfer(accountID, direction, amount int64) → (available, bank int64, err error):
  validate amount > 0, direction legal → else ErrBadTransfer

  to_trading (Bank → Available):
     UPDATE accounts
        SET bank_cash    = bank_cash    - $amount,
            trading_cash = trading_cash + $amount
      WHERE account_id = $id AND bank_cash >= $amount        -- guard: can't overdraw the bank
      RETURNING trading_cash, bank_cash
  to_bank (Available → Bank):
     UPDATE accounts
        SET trading_cash = trading_cash - $amount,
            bank_cash    = bank_cash    + $amount
      WHERE account_id = $id AND trading_cash >= $amount      -- can't bank money you don't have
      RETURNING trading_cash, bank_cash

  0 rows affected → distinguish: account missing → ErrNoAccount ; guard failed → ErrInsufficient
  row → (trading_cash, bank_cash), nil

One SQL statement = atomic + conserving by construction. Both columns move in one UPDATE, so the sum is preserved with no transaction wrapper; the >= $amount predicate in the WHERE makes guard-and-move a single atomic check-and-move — no read-then-write race. This is the durable analogue of MoveCash's "conserving, fails before mutating."

Edge cases:

ErrNoAccount — no durable row. Can't happen on the qualify path (EnsureAccount ran first); a raw bad account_id → 404. Distinguished from ErrInsufficient by a follow-up existence check (both surface as 0 rows).
ErrInsufficient (pull > bank / push > trading) — the WHERE predicate matches 0 rows → 422.
Amount ≤ 0 / illegal direction → 400, before the SQL.
DB unreachable → 500/503; nothing moved (single statement, all-or-nothing); retry.
Durable transfer vs a session starting for the same account — no TOCTOU: the transfer is one committed UPDATE; a later on-demand seal reads trading_cash and sees the post-transfer value (the retry model — the transfer is a completed call before the retry's gate runs). And a durable transfer can't race a live session's MoveCash, because under XOR the account can't be both live and off-session at once.
trading_cash signed (debt legal) — a push still requires trading_cash >= amount (can't bank money you don't have); the WHERE guard covers it.

Persistence: one Postgres UPDATE. No Redis, no snapshot — there's no session, so no crash-window to protect (the value is durable the instant the UPDATE commits). This is the opposite of Stage 1's Redis mirror, and correctly so.

How the two stages compose
POST /v1/account/transfer
   └─ route(Transfer) → shard
        live?  yes → MoveCash on owned account         (Stage 1) → 200 { available, bank }
               no  → durableTransfer on Postgres row    (Stage 2) → 200 { available, bank }

One endpoint, one response shape, mode-blind to the caller. The on-demand gate-fail → pull → retry loop resolves entirely through the Stage-2 branch, with no L7 orchestration and no in-memory account. B9's on-demand gate never moves money; A6 is where the user moves it, as their own action.

Notes banked
Conservation of the durable UPDATE relies on no other off-session durable writer touching trading_cash/bank_cash — and there is none (session_save runs only on a live account; open_account only at birth). So the UPDATE is the sole mutator in that window.
Off-session ≠ never-existed — ErrNoAccount (404) is distinct from ErrInsufficient (422), though both show as "0 rows."
No coupling to on-demand — Stage 2 is a plain transfer; it knows nothing about the gate. The gate re-check happens on the user's next start attempt (the retry model).
Settled / open
Settled: the shared endpoint, route-first control flow, Stage 1 (MoveCash on the shard, MarkSnapshotDirty(sessionID, accountID)), Stage 2 (single-UPDATE durable transfer, both directions), all edge cases.
Open (cosmetic): the unit vocabulary (inherit Tier-4's) and the exact status for insufficient funds (422 vs 400).