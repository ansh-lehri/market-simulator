# broker/lifecycle — Session & Account Lifecycle (Tier 3)

<!-- IMPLEMENTATION LOCATION (resolved at Phase-2 Stage D):
     The three off-shard methods (EnsureAccount, QualifyOnDemand, StartSession) and the
     first funnel command (InstallSession + handleInstallSession + waitReply + the three
     new sentinels ErrBadBinding / ErrBadParticipant / ErrSessionAlreadyLive) all live in
     broker/funnel/. Reasons:
       (1) Go method receivers — the methods bind to *funnel.Broker; receivers must be
           declared in the receiver type's package. Moving them here would require
           either breaking the (b *Broker).* surface or exporting funnel internals.
       (2) Unexported funnel internals — route(), shard, Command, dispatch are all
           package-private; StartSession must call route(), handleInstallSession must
           live on *shard, and InstallSession must satisfy the Command interface. A
           sibling package cannot reach any of these without weakening Tier-0's
           single-ingress rule.
       (3) Phase 2's character is assembly, not invention — no new domain concept
           warrants a new package boundary.
     This folder is DOC-ONLY: no .go files land here. If a future increment ever needs
     one (unlikely), re-open this decision then. -->

<!-- READ ORDER (all apply together):
       1) /CLAUDE.md                                            — root constitution
       2) broker/CLAUDE.md                                      — the broker (funnel) constitution
       3) THIS file                                             — what the lifecycle flows own (impl lives in broker/funnel/)
       4) docs/broker/BROKER_PHASE2_IMPLEMENTATION_DESIGN.md    — the spec (Points 1-3 LOCKED; Point 4 deferred)
          docs/broker/BROKER_ACCOUNT_SESSION_LIFECYCLE.md       — the locked Tier-3 design
          docs/broker/BROKER_EXPECTATIONS_FROM_L7.md            — the L7 → broker call seam -->

## Spec & authority
`BROKER_PHASE2_IMPLEMENTATION_DESIGN.md` is the spec. **Character of this tier: assembly, not
invention** — every money flow already exists and is locked in L5 (`economy/lifecycle`); this layer
**sequences** those L5 methods on the funnel and injects the seam state L5 doesn't hold. **Code wins
over doc:** all signatures are source-bound to the built `economy`/`contract`/`connectivity` — where
a doc sketch diverges, follow the built code and flag it.

## What it IS (one line)
The session & account lifecycle: **get-or-create** a durable account (`EnsureAccount`), the
**on-demand money gate** (`QualifyOnDemand`), and **session start** (`StartSession` → the funnel's
`InstallSession`) that turns a durable account into a live on-shard session. **Teardown is NOT in
this build** (see Flags).

## THE BRIDGE (doc ↔ built L5)
- `InstallSession` carries the **whole `lifecycle.LoadedAccount`** (`{Account *account.Account,
  OwnerType, OwnerID}`), not a bare `*Account` — both L5 loaders return `LoadedAccount`, and
  `ownedSession` stores it as-is. (The Phase-2 doc's bare `*Account` was shorthand.)
- `Mode` is `lifecycle.SessionType` (`Canonical | OnDemand`).
- `ownerType` is the **typed `lifecycle.OwnerType`** up here (what `OpenAccount` takes); it is
  `.String()`-ed only at the Store seam (`AccountHandle` — Phase-1 P1-D5).

## Owns / public surface
```go
func (b *Broker) EnsureAccount(ctx, ownerType lifecycle.OwnerType, ownerID string) (AccountID, error)  // Point 1, OFF-shard
func (b *Broker) QualifyOnDemand(ctx, accountID AccountID) (ok bool, err error)                         // Point 2, OFF-shard read
func (b *Broker) StartSession(ctx, cmd SessionStartCmd) error                                           // Point 3, OFF-shard orchestrator

type SessionStartCmd struct { AccountID; SessionID; Mode SessionType; ExchangeID;
                              ExchangeInbound chan<- contract.InboundMessage; Participant contract.ParticipantID }
type InstallSession struct  { AccountID; Loaded lifecycle.LoadedAccount; Binding SessionBinding;
                              Participant contract.ParticipantID; Reply chan error }   // FIRST command on the funnel
func (c InstallSession) Account() AccountID                                             // lets route() hash it
// shard.handleInstallSession(c)  — on the owner goroutine, pure in-memory
```
Errors: `ErrBadBinding`, `ErrBadParticipant`, `ErrSessionAlreadyLive`, `ErrBrokerTimeout`; reuses
`ErrNoAccount` / `ErrLoadFailed`.

## Load-bearing rules
**EnsureAccount (Point 1)** — off-shard, on L7's goroutine, **Postgres-only, never touches the
funnel.** **Create-first**, not check-first: call L5 `OpenAccount`; on `ErrAccountExistsFor`, fall
back to `Store.AccountHandle`. **The INSERT is the sole race arbiter** (`uq_accounts_owner`) — no
check-then-create window. On fallback `ErrNoAccount`, **retry `EnsureAccount` once**, then return
`ErrNoAccount`. **Never returns "not found"** — the method's job is to make the account exist.
Capital/bank come from `BrokerConfig` dials. Birth reuses L5 `OpenAccount` (keeps `drip_*` + id-mint
L5-owned), run on the **Store's own unstarted `lifecycle.Service`**.

**QualifyOnDemand (Point 2)** — off-shard **pure read**, zero writes, no state. Reads
`Store.TradingCash`, returns a **bare boolean** `cash >= cfg.OnDemandMinimum`. **L7 stays
money-blind** — never return the balance or the threshold. An under-balance is `(false, nil)` — a
valid "no," **not an error**. `trading_cash` is **signed** (debt legal), so a negative balance fails
the `>=` cleanly. `ErrNoAccount` → `(false, ErrNoAccount)` (missing = bug; `EnsureAccount` ran
first). Placed by L7 **in front of** spin-up so a `false` wastes nothing.

**StartSession (Point 3)** — off-shard orchestrator on L7's goroutine. Validate **before any I/O**:
nil `ExchangeInbound` → `ErrBadBinding`; empty `Participant` → `ErrBadParticipant`. **`Mode` is the
only divergence**: `Canonical` → `SessionLoad`, `OnDemand` → `OndemandSessionLoad`, both on the
**Store's `lifecycle.Service`** (off-shard, concurrency-safe — the load path is `LoadAccount` +
`WriteSnapshot`, never the unlocked `dirty` map). Build the `SessionBinding`, `route` the
`InstallSession` command, `waitReply` (ctx deadline → `ErrBrokerTimeout`).
- **Snapshot-fail policy is NOT re-implemented in the broker** — it lives in the L5 loaders
  (canonical `SessionLoad` → `ErrSnapshotWriteFailed` **hard-fail**; `OndemandSessionLoad`
  **tolerates** the miss, starts on the in-memory copy, never writes back). The broker **propagates
  uniformly**, no broker branch.
- **Mode is distinguished downstream ONLY by `PersistOnClose`** on the `*Account` (canonical true,
  on-demand false) — set by *which loader* ran. **On-demand writes zero Postgres, ever** (the
  property that later lets realization skip the write-behind buffer).

**InstallSession (Point 3, on the owner goroutine)** — the **first command on the Tier-0 funnel**.
`handleInstallSession` is **exactly**: XOR guard (already live → `ErrSessionAlreadyLive`) + map write
(`s.sessions[aid] = &ownedSession{loaded, binding, participant}`) + reply. **Nothing else** — no
order flow, no `nextOrderID` special case (a fresh load ⇒ empty split, empty tracks). Single-writer
protects `s.sessions` — which is **why install is a command while the load can run anywhere.**

## Deps & seam
- L5 `lifecycle.Service` on the **Store** (`OpenAccount`, `SessionLoad`, `OndemandSessionLoad`);
  `Store.AccountHandle` + `Store.TradingCash`.
- The funnel (Phase 0): `route`, `waitReply`, `ownedSession`, `SessionBinding`.
- Consumes the in-process **L7 → broker** calls (`EnsureAccount`, `QualifyOnDemand`, `StartSession`).

## Do NOT
- check-then-create (the INSERT is the arbiter); return "not found" from `EnsureAccount`;
- expose balance or threshold to L7;
- branch snapshot-fail policy in the broker (L5 owns it — propagate);
- square, run order flow, or special-case `nextOrderID` in `handleInstallSession`;
- touch `s.sessions` anywhere but the owner goroutine;
- build **teardown** (deferred — see Flags).

## Flags (surface in orientation, don't silently resolve)
- **Teardown (Point 4) is OUT of this build.** Model settled, impl open: the Shape-A-leaver fork
  (B1 save-at-leave vs B2 straggler-sweep) and the exact per-session canonical-stop command shape
  are unresolved, and L7 (the orchestrator) is being rebuilt. Teardown is its own later increment.
- **Supersedes Phase-1 Store rule (P2-1·D3):** `broker/store/CLAUDE.md` says "pool only, no Redis" —
  **edit it.** The Store gains a `lifecycle *lifecycle.Service` field on an **unstarted**
  `persistence.Component` (needs redis + a cost module), idle for birth, active for session
  snapshots. Also add the `TradingCash(accountID)` read.

## Tests
First **full funnel + L5 + Store integration** (reuse the `l5_test` harness + migrations). **No
auditor, no golden-first** — assembly/control-flow, objective; **assertions trace to the Phase-2
doc's error-map table**, not to the code:
- `EnsureAccount`: create-new → aid; duplicate → fallback returns the **same** aid; concurrent same
  owner → both get the same aid (one created, one fell back); the `ErrNoAccount` retry-once path;
  genuine errors (`ErrBadOwnerType`, `ErrCreateFailed`) propagate.
- `QualifyOnDemand`: `cash>=min` → true; `cash<min` → `(false,nil)`; debt (negative) → false;
  `ErrNoAccount` → `(false,ErrNoAccount)`; Postgres down → `(false,ErrLoadFailed)`.
- `StartSession`/`InstallSession`: canonical → live session with `PersistOnClose=true`; on-demand →
  `false` and **zero Postgres**; double-install same account → `ErrSessionAlreadyLive` (XOR); nil
  binding / empty participant → rejected **off-shard** before any I/O; snapshot-fail → canonical
  hard-fails, on-demand tolerates; ctx timeout → `ErrBrokerTimeout`.
- `go test ./... -race` standing gate (the `InstallSession` command crossing to the shard +
  concurrent off-shard loads).