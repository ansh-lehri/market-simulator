# broker/transfer — Bank Transfer (Phase 3)

<!-- Provisional path. This spans the HTTP layer + the funnel + a shard handler; whether it's its
     own file-set or folds into an http package + the funnel is orientation's call. Content is
     load-bearing, path is not.
     READ ORDER (all apply together):
       1) /CLAUDE.md                                         — root constitution
       2) broker/CLAUDE.md                                   — the broker (funnel) constitution
       3) THIS file
       4) docs/broker/BROKER_PHASE3_IMPLEMENTATION_DESIGN.md — the spec (both points LOCKED)
          docs/broker/BROKER_BANK_TRANSFER.md                — the authority (both stages LOCKED) -->

## Spec & authority
`BROKER_PHASE3_IMPLEMENTATION_DESIGN.md` is the spec; `BROKER_BANK_TRANSFER.md` the authority.
**Phase 3 adds almost no new machinery** — it reuses the funnel, the Phase-1 `Store.DurableTransfer`,
the Phase-2 `s.sessions` map (read-only), and L5 `MoveCash`. **Code wins over doc**; the few places
this corrects the authority's pseudo-code are flagged below and are source-grounded.

## What it IS (one line)
The one bank↔trading transfer endpoint, `POST /v1/account/transfer`, **route-first**: if the account
is live on its owning shard → in-memory `MoveCash` on the owner (+ snapshot-dirty); else →
`Store.DurableTransfer` (the Phase-1 guarded `UPDATE`). One **mode-blind** `{available, bank}` result.

## THE BRIDGE (doc ↔ built)
- The command **finalizes Tier 0's `BankMove` placeholder**: `BankMove`→**`Transfer`**,
  `handleBankMove`→**`handleTransfer`**, `Dir BankDir`→**`From, To account.Bucket`**,
  `BankResult`→**`TransferResult`** (reply field stays **`ReplyTo`** for funnel consistency, not the
  doc's `Reply`).
- The live account is reached via **`sess.account.Account.Cash`**, not `sess.account.Cash` — the
  `ownedSession` holds a `LoadedAccount{Account *account.Account, …}` (the mutation sticks because
  `s.sessions` is pointer-valued and `Account` is a second pointer layer).
- **`SessionID` must be alias-compatible with `realize.SessionID` (`= string`)** so
  `sess.binding.SessionID` passes to `MarkSnapshotDirty` with no cast.

## Owns / public surface
```go
// POST /v1/account/transfer   body: { account_id, direction: "to_bank"|"to_trading", amount, unit }
type Transfer struct { AccountID; From, To account.Bucket; Amount int64; ReplyTo chan TransferResult }
func (c Transfer) Account() AccountID
type TransferResult struct { Present bool; Available, Bank int64; Err error }
func (b *Broker) Transfer(ctx, aid AccountID, from, to account.Bucket, amt int64) (TransferResult, error)
// shard.handleTransfer(c Transfer) TransferResult   — on the owner goroutine
```
Errors reused: `ErrBrokerTimeout`, `ErrInsufficient`, `ErrNoAccount`, `ErrBadTransfer`.

## Load-bearing rules
- **Route-first → durable-on-miss** is the spine, and it is **authoritative, never client-declared**:
  `Broker.Transfer` routes the `Transfer` command to the owning shard; `handleTransfer` returns
  `Present:false` iff the account is **not live** → the HTTP handler then calls `DurableTransfer`.
  A wrong client liveness claim could fire a durable `UPDATE` on a live account and corrupt it — so
  liveness is decided by the shard, not the caller.
- **Three distinct outcomes, and `Present:true+Err` ≠ `Present:false`.** A live-path failure
  (insufficient) is `Present:true, Err:set` → surface it (**422**), do **not** fall to durable. Only a
  genuine `!live` miss is `Present:false` → durable. **`Present:false` is internal-only; no client
  `409` is ever emitted.**
- **Two-vocabulary seam** (P3-D2): the live path speaks **buckets** (`MoveCash(From,To,Amount)`), the
  durable path speaks **`TransferDirection`**; the HTTP handler maps the one request `direction` into
  **both**, once.
- **`handleTransfer`** (owner goroutine): read `s.sessions` → `MoveCash` on `sess.account.Account.Cash`
  (conserving, in-mem, **fails before mutating**) → `MarkSnapshotDirty(binding.SessionID, accountID)`
  **unconditionally** (mode-blind) → return `{Available, Bank}`.
- **`MoveCash` edges** (source-verified): the two bank edges are `Available→Bank` and `Bank→Available`;
  guards `amount>0`, `source≥amount`, fails-before-mutate; touches only the two named buckets — **never
  `Reserved`** (so a pull mid-position is fine). Must run on the owner — it does.
- **`Broker.Transfer`** is a thin shard round-trip (mirror of `PlaceOrder`): fresh buffered(1) reply,
  `select(reply | ctx)` → `ErrBrokerTimeout`. The durable-on-miss orchestration lives in the **HTTP
  handler**, not folded into this method (P3-D4).
- **HTTP handler owns the timeout** (`transferTimeout`, a config dial). Validate **before the funnel**:
  `account_id` present, `direction ∈ {to_bank, to_trading}`, `amount > 0` → else **400**.
- **Status contract (mode-blind):** `200 {available,bank}` (either branch) · `400` bad input · `422`
  insufficient (**both** branches) · `404` no durable account (Stage 2 only) · `504` broker timeout ·
  `500/503` internal/DB · **never `409`**.
- **`MarkSnapshotDirty` unconditional** — on-demand isolation is enforced later at `session_save`
  (`PersistOnClose=false`), never gated in the handler.

## Deps & seam
Funnel (`route`, buffered-1 reply, `ErrBrokerTimeout` — Phase 0); `Store.DurableTransfer` +
`get_account_handle` (Phase 1); `s.sessions` (Phase 2, **read-only**); L5 `account.MoveCash` + `Bucket`
+ `persistence.MarkSnapshotDirty`.

## Do NOT
- trust the client to declare liveness (route-first is authoritative);
- emit a `409`; gate `MarkSnapshotDirty` on mode; touch `Reserved`;
- fold the durable-on-miss orchestration into a broker method (it lives in the HTTP handler);
- use `sess.account.Cash` (it is `sess.account.Account.Cash`).

## Flags (surface in orientation)
- **Edits the funnel:** finalize Tier 0's `BankMove` placeholder → `Transfer`/`TransferResult`/
  `handleTransfer`/`From,To account.Bucket`/dispatch case; supersede the Tier-0 inline comment
  `ErrBrokerTimeout // 503` (HTTP mapping is **504**).
- `transferTimeout` is a config dial (value open, home fixed).

## Tests
Integration (reuse the `l5_test` harness) — **no auditor**; assertions trace to the doc's **status
table**, not the code:
- in-session: live transfer both directions → `{available,bank}`; insufficient → `Present:true+Err` →
  422; mid-position (Reserved untouched) → allowed.
- no-session: idle → `Present:false` → `DurableTransfer` → 200 | insufficient 422 | no-account 404.
- route-first: a live account never falls to durable; an idle one always does; **no `409` ever**.
- HTTP: 400 bad input; 504 timeout; identical `{available,bank}` shape both branches.
- concurrent transfer + order on one account → serialized on the shard loop; `-race` standing gate.