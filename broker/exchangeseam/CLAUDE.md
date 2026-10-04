# broker/exchangeseam — Exchange Wire Seam, Points 1 & 2 (Phase 4)

<!-- Provisional path (avoids clashing with the top-level exchange/ package). The punch is a shard
     method and the consumer is a Broker method — whether they sit in the funnel package or their own
     is orientation's call. Content is load-bearing, path is not.
     READ ORDER (all apply together):
       1) /CLAUDE.md                                         — root constitution
       2) broker/CLAUDE.md                                   — the broker (funnel) constitution
       3) THIS file
       4) docs/broker/BROKER_PHASE4_IMPLEMENTATION_DESIGN.md — the spec (Points 1-2 LOCKED; 3-5 deferred)
          docs/exchange/Exchange_Broker_Contract.md          — the wire-seam contract (§3, punch)
          docs/broker/BROKER_ORDER_LIFECYCLE_intake_punch_settle.md — punch/settle context (handlers = Phase 5) -->

## Spec & authority
`BROKER_PHASE4_IMPLEMENTATION_DESIGN.md` Points 1 & 2 is the spec. Punch authority:
`Exchange_Broker_Contract` §3 + the order-lifecycle doc. Settlement authority: the **Tier-0
result-ingress decision** (Point 3) + the order-lifecycle doc. **NOT** `backend_stream_consumer_design`
— that is the Redis chat-pipeline consumer, a different subsystem. **Code wins over doc**; all edge
types are source-bound to `contract`/`exchange`.

## What it IS (one line)
The broker↔exchange **order-loop wiring**: the **punch** primitive (outbound — a built
`InboundMessage` onto a session's inbound edge) and the **settlement consumer** (inbound — drain the
exchange's `Private` stream, parse the account out of the echoed `OrderID`, route an `ApplyResult` to
its shard). **Pure transport, persists nothing.** First phase that physically connects broker↔exchange;
**no external surface.**

## THE BRIDGE (the three edges, the loop)
Exchange exposes three channel edges: **`Inbound chan<- InboundMessage`** (broker writes → Point 1),
**`Private <-chan EngineResult`** (broker reads → Point 2), **`Public <-chan emit.TapeEvent`** (broker
reads → Point 3, **deferred**). The loop: submit/cancel leaves on `Inbound`, the answer returns
**asynchronously** on `Private`, correlated by **`OrderID` + `BrokerID`** — **no synchronous reply**.
**The six-outcome mutation (`handleApplyResult`) is Phase 5, NOT here.**

## Owns / public surface
```go
func (s *shard) punch(sess *ownedSession, msg contract.InboundMessage)   // Point 1 — owner goroutine
func (b *Broker) runSettlementConsumer(private <-chan contract.EngineResult)  // Point 2 — one goroutine/instance
func accountOf(oid contract.OrderID) (account.AccountID, bool)           // parse "<AccountID>:<seq>"
```
Reuses the `ApplyResult{AccountID, Result}` command + its dispatch case (declared in Tier 0; **body is
Phase 5**).

## Load-bearing rules
**Punch (Point 1)** — on the owner goroutine; only ever called for a **live** session (the caller
already did `s.sessions[aid]`). Delivers an **already-built** `InboundMessage`.
- **Stamp `msg.BrokerID = s.brokerID`** here so no caller can forget it — the exchange echoes it back
  on `EngineResult`, which is what lets Point 2 route the result.
- **Blocking send, no timeout, no `select`-default.** The edge is unbuffered and the exchange ingress
  is a dedicated always-draining goroutine, so the send returns immediately in practice. Silently
  dropping (default) or timing out an order is **wrong** — backpressure is the exchange's to own.
- **Stop-safety by teardown ordering, no per-punch `recover()`.** Bare send. The invariant "all
  sessions bound to an instance are torn down before that instance's `Stop()`" means a live session can
  never point at a stopped exchange; a post-`Stop` punch would be a **real teardown-ordering bug** and
  must surface as a panic, not be swallowed. Enforcement is lifecycle scope, not this primitive.
- **Fire-and-forget — no reply.** The exchange answers async on `Private`. (Contrast the request/reply
  funnel commands.)
- **Scope line:** punch *only moves the message*. Building the `FundedOrder`, Gate-4 reservation,
  `OrderID` mint, `OrderTrack`, and *deciding what/when* to punch are **Phase 5**.

**Settlement consumer (Point 2)** — a `*Broker` method run as **one goroutine per exchange instance**.
`for r := range private` → `accountOf(r.OrderID)` → `b.route(ApplyResult{aid, r})`. **Routes only — no
reply, no state.**
- **Account resolution:** parse everything before the **last** `":"` (`OrderID = "<AccountID>:<seq>"`,
  `seq` colon-free) via `LastIndex`, **not** `SplitN` — an `AccountID` containing `":"` still parses.
  No cross-shard lookup, no shared index.
- **Malformed `OrderID` → log-at-error-and-SKIP, not panic.** A broker-minted-OrderID invariant
  violation; killing the consumer would halt **all** settlement for that market, and there is no account
  to route to. Loud error log so a real breakage surfaces.
- **Self-terminating:** the range ends when the exchange closes `Private` during `Stop()`. Per-account
  order is preserved for free (single drain + funnel → serial on the one shard).
- **Scope line:** the consumer *resolves and routes*. The six-outcome `handleApplyResult` adapter
  (`Convert`/`Release`/`ReleaseExcess` mapping) is **Phase 5, point 3**.

**Single-writer intact:** Point 1 runs *inside* a shard handler (already the owner); Point 2 is a
separate goroutine that reaches account state **only** by routing a command in — it never touches a
shard map directly.

## Deps & seam
Phase 0 (`route`, the `ApplyResult` command + dispatch seam, `SessionBinding`, `shard.brokerID`);
`contract` (`InboundMessage`, `FundedOrder`, `CancelRequest`, `EngineResult`); the exchange edges
(`Inbound`, `Private`) + `Exchange.Stop()` ordering contract. **Who spawns the consumer** with a
specific instance's `Private` (and later the ingestor with `Public`) is `WireInstance` — **Phase 7**.

## Do NOT
- wrap the punch in a `select`-default or a timeout (drops/stalls an order);
- add a per-punch `recover()` (masks a teardown bug);
- give the punch a reply (it is fire-and-forget);
- build the `FundedOrder` / Gate-4 / `OrderID` / `OrderTrack` (Phase 5);
- build `handleApplyResult` — the six-outcome adapter (Phase 5);
- panic on a malformed `OrderID` (log-and-skip);
- touch a shard map from the consumer goroutine (route a command in);
- use `SplitN` for the account parse (use `LastIndex`).

## Flags (surface in orientation)
- **Points 3–5 (the market-data component — ingestor + `CandleStore` + `MarketDataCache`) are
  DEFERRED to end of build.** Not this phase.
- `WireInstance` (who starts the consumer/ingestor per instance) = **Phase 7**; this phase defines the
  primitive + the consumer, not who spawns them.
- Consumer-exit tracking (a `WaitGroup` so shutdown waits for settlement to drain after `Private`
  closes) is an infra/lifecycle detail, noted not built here.
- **P4-D1 supersedes the order-lifecycle doc's inline punch** — the Phase-5 handlers will build the
  `InboundMessage` and call `s.punch`, not send inline. Annotate that doc.

## Tests
**No auditor** — pure transport, objective. (The one auditor-worthy piece in this area, the six-outcome
settlement mapping, is **Phase 5**.)
- `punch`: fake inbound channel → message arrives with `BrokerID` stamped; blocking-send semantics.
- `accountOf`: table tests — normal, `AccountID` containing `":"` (LastIndex), malformed → `!ok`.
- settlement consumer: fake `Private` feeding `EngineResult`s → `ApplyResult` lands on the **right**
  shard (routed by parsed account); malformed `OrderID` → logged-and-skipped, draining continues;
  closing `Private` → consumer exits.
- per-account order: several results for one account arrive on its shard in arrival order.
- optional integration: wire a real exchange instance (`NewExchange`/`Start`), hand-build a minimal
  `InboundMessage`, punch it, assert the `EngineResult` returns on `Private` and the consumer routes
  `ApplyResult` to the shard (the shard's `ApplyResult` handler is a no-op seam this phase).
- `go test ./... -race` standing gate (consumer goroutine + funnel).