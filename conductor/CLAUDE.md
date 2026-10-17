# layer7 — The Conductor (L7 domain constitution + L7-0 scaffold)

<!-- Package decided by orientation (L7 is a TOP-LEVEL sibling of broker/ and exchange/, NOT under
     broker/ — it is a consumer of the broker, a separate concern). Provisional name: layer7.
     This file is BOTH the L7 domain constitution AND the L7-0 (scaffold) package rules, since the
     scaffold defines the Conductor/Registry/ports that ARE the domain — same way broker/CLAUDE.md
     was written with Phase 0.
     READ ORDER (all apply together):
       1) /CLAUDE.md                 — root constitution (Go, int64, single-writer, logging, testing, workflow)
       2) THIS file                  — the L7 (Conductor) constitution + scaffold rules
       3) layer7/<pkg>/CLAUDE.md      — per-flow rules added by each later L7 phase
       4) The six L7 impl docs + their Tier theory docs (see Spec & authority) -->

## Spec & authority
The six implementation docs are the spec for the flows (each builds on this scaffold):
`LAYER7_CANONICAL_SPINUP_IMPL.md`, `LAYER7_CANONICAL_JOIN_AND_WS_IMPL.md`,
`LAYER7_ONDEMAND_SPINUP_IMPL.md`, `LAYER7_CANONICAL_TEARDOWN_IMPL.md`,
`LAYER7_ONDEMAND_TEARDOWN_IMPL.md`, `LAYER7_READ_AND_ACCOUNT_ENDPOINTS_IMPL.md`. Their theory tier
docs (`LAYER7_TIER0_REGISTRY_AND_IDENTITY`, `..._TIER1_MARKET_CONSTRUCTION_AND_WIRING`,
`..._TIER2_ADMISSION/SEEDING/HTTP_SURFACE`, `..._TIER3_TEARDOWN`) are background only. **Code wins
over doc; a later doc wins over an earlier one** (the teardown docs explicitly correct Tier-3). Every
external signature in these docs was verified against the shipped broker source this session.

All docs are present on path docs/conductor/

## What L7 IS (one line)
The **Conductor**: the session + market orchestration layer that sits **above** the broker. It brings
canonical markets up on a timer, admits humans/agents into sessions, stands up on-demand instances,
wires each exchange instance's edges to the broker (settlement) and market-data (tape), tears markets
down, and serves the pre-session read doors. It **holds no money and does no matching** — it drives
the shipped broker + market-data + exchange and stays out of their internals.

## The two axes + the one hand-off (frame)
- **Account/money axis** — keyed by `AccountID`; lives in `broker/funnel` + shards. L7 touches it only
  through `EnsureAccount`/`StartSession`/`QualifyOnDemand` (+ the later `EndSession`/`FinalizeMarket`).
- **Market-data axis** — keyed by `ExchangeID`+`InstrumentID`; lives in `broker/marketdata`. L7 touches
  it only through `Market.WireInstance` (per instance) and the `/v1/ws` dispatcher.
- The one controlled hand-off is the **`CacheRegistry`** (inside `Runtime.Market`, already injected
  into the broker as its `MarketPriceCache`): market-data writes per-instrument snapshots, the account
  side reads prices. L7 wires both axes at bring-up, then stays out.

## THE RULE (the load-bearing concurrency discipline)
The registry has **one mutex**. Take it **only** for the microsecond reserve / finalize / state-flip
steps — **never across a call-out** (`EnsureAccount`, `StartSession`, `BRING_UP`, `WireInstance`,
`exch.Start/Stop`, `EndSession`, `FinalizeMarket`) and never across the `BRING_UP` wait or a WS
`ServeHTTP` delegate. The pattern everywhere: lock→reserve/copy handles→unlock→slow off-lock work→
lock→finalize→unlock. A lock held across a call-out freezes the whole registry; the join doc §9 shows
why (a WS connection blocks its request goroutine for its whole life).

## Two walls every endpoint respects (from Tier-0 / HTTP-surface)
- **Money-blind** — the conductor may **not** read the money tables. `account/open` returns only the
  `account_id` handle; balance/trades/P&L are the broker's shipped `/v1/account/*` reads the frontend
  calls directly. The **pre-session cash view** (user signed up/logged in but has not joined a market
  yet, so the live `/v1/account/balance` would 409) is also a broker read:
  **`GET /v1/account/cash?account_id=X` → `{trading_cash, bank_cash}`** — pure durable Postgres read in
  `broker/reads`, 404 on an unknown account_id, 503 on store failure. Keeps money-blind intact: the
  conductor never touches the money tables, even for the pre-session case.
- **H.8 — L7 returns session/market *identity + metadata*, never market data.** Catalog, instrument
  list, session windows = L7's. Prices (LTP/quote/depth) stream over the shipped market-data WS; L7
  never serves them.

## No auth (trusted identity packet)
No authentication anywhere. The frontend sends `{ user: { email, user_id } }`; `user_id` is the
identity key (→ `ownerID`), `email` is attribution only. L7 **trusts, never validates** it.

---

## L7-0 scope — the scaffold (this phase)
Build the foundation every flow doc sits on; **no flow logic, no session/market endpoints yet.**

### Pieces
- **`Conductor`** — L7's top object: holds the `*Registry`, the injected ports, the config source
  (`cfgDir`/`loadConfig`/`listConfiguredMarkets`), an own `ohlcv *pgxpool.Pool` read handle (for
  `readReference` in L7-1), `now func() time.Time` (injectable clock), `log *slog.Logger`, and the
  timer lifecycle fields (`timerQuit chan struct{}`, `wg sync.WaitGroup`) — fields present now, the
  timer goroutines themselves are L7-1.
- **`Registry`** — `markets map[MarketID]*Market`, `sessions map[SessionID]*Session`,
  `byUser map[UserID]*Session`, under **one `sync.Mutex`**. The state enums: `MarketState{Provisioning,
  Live, TearingDown}`, `SessionState{Starting, Active, Stopping, Ended, Failed}` (`Ended` vestigial —
  sessions are deleted on reap, never tombstoned; `Failed` is on-demand setup-failure), `Mode{Canonical,
  OnDemand}`, `AccountType{Durable, Ephemeral}`. The `Market`/`Session` structs per the docs (spin-up
  §5 + join §4 + on-demand §4.3), including `Market.{fanout, inbound, exch, closeAt, warmupEndsAt,
  members}` and `Session.{Market, Account, Participant, AccountType, State, FailReason}`.
- **Config** — `MarketConfig` + sub-structs (spin-up §5), `loadConfig(tag)` + `listConfiguredMarkets()`
  reading `cfgDir`. One config per market tag.
- **Ports (interfaces)** — defined here, concrete bindings asserted at compile time (below).
- **Construction / DI / routing** — a constructor taking the shipped `bringup.Runtime` (binds
  `Broker`, `Market`, `Mux`), opening/accepting the `ohlcv` pool, injecting the ports; and the
  mount point that registers L7 routes on `Runtime.Mux` (no routes added in L7-0 beyond, at most, a
  trivial liveness probe to prove the handoff — endpoints are later phases).

### Verified shipped bindings (type the ports against THESE exact shapes — code-checked this session)
```go
// from bringup (the handle L7 is constructed around):
type bringup.Runtime struct { Broker *funnel.Broker; Store *store.Store; Market *marketdata.MarketData; Mux *http.ServeMux }
func bringup.BringUp(ctx context.Context, cfg bringup.BrokerConfig) (*bringup.Runtime, error)

// funnel (money axis) — SHIPPED:
func (*funnel.Broker) EnsureAccount(ctx, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
func (*funnel.Broker) StartSession(ctx, cmd funnel.SessionStartCmd) error
func (*funnel.Broker) QualifyOnDemand(ctx, accountID funnel.AccountID) (bool, error)
type funnel.SessionStartCmd struct { AccountID; SessionID; Mode lifecycle.SessionType;
                                     ExchangeID; ExchangeInbound chan<- contract.InboundMessage; Participant }
// funnel id aliases (all = string): AccountID, SessionID, ExchangeID, ParticipantID, InstrumentID, OrderID, BrokerID
// lifecycle: OwnerHuman/OwnerAgent (OwnerType); Canonical/OnDemand (SessionType)

// marketdata (market-data axis) — SHIPPED:
func (*marketdata.MarketData) WireInstance(exchangeID marketdata.ExchangeID, tape <-chan emit.TapeEvent,
                                           market string, mode marketdata.Mode) *marketdata.FanOut
func (*marketdata.MarketData) WSConfig() marketdata.WSConfig
func marketdata.NewWSHandler(fanOut *marketdata.FanOut, cfg marketdata.WSConfig) http.Handler
// marketdata.ExchangeID = string ; marketdata.Mode{ModeCanonical, ModeOnDemand}

// exchange (simulator/exchange/wire) — SHIPPED (per the L7 docs' verified contract; not in the broker tarball):
func wire.NewExchange(numShards int, schedule clock.SessionSchedule, instruments []wire.InstrumentSpec) *wire.Exchange
func (*wire.Exchange) Start(tickInterval time.Duration) ; func (*wire.Exchange) Stop()
// edges: Inbound chan<- contract.InboundMessage ; Public <-chan emit.TapeEvent ; Private <-chan contract.EngineResult
```

### The unbuilt-seam rule (load-bearing — keeps every phase compiling)
Three funnel methods the flow docs call **do NOT exist in the broker yet** (verified absent):
`funnel.Broker.WireInstance(exchangeID, private)` (L7-1), `funnel.Broker.EndSession(SessionEndCmd)`
and `funnel.Broker.FinalizeMarket(exchangeID)` (L7-5/6). Therefore:
- **L7-0's `BrokerPort` interface lists ONLY the shipped methods** (`EnsureAccount`, `StartSession`,
  `QualifyOnDemand`). Add a compile-time `var _ BrokerPort = (*funnel.Broker)(nil)` — it must pass now.
- Each later phase **grows `BrokerPort`** with its seam (`WireInstance` in L7-1, `EndSession`/
  `FinalizeMarket` in L7-5) **as a broker prereq step built in that phase**. Never put an unbuilt
  method on the port in an earlier phase — the assertion would fail to compile.
- `MarketDataPort` and `ExchangeFactory` type against the shipped `marketdata`/`wire` surfaces above.

### Placeholder ports (stubs — L2 undesigned)
`AgentOrchestrator.BringUp(tag, mode, instruments) error` (SYNC) and
`LiquidityOperator.SeedLiquidity(marketID, specs)` (fire-and-trust), plus later
`AgentOrchestrator.WindDown(tag, mode)` — are **placeholders**; L2 is not designed. L7-0 defines the
interfaces + a **stub implementation** (no-op / logs). Consequence (flag, not a defect): a spun-up
market has **no real agents** until L2 lands, so it won't truly trade — testable in isolation only.
These port signatures **will change** with L2; update the ports then.

## Do NOT (L7 domain)
- hold the registry mutex across any call-out, the `BRING_UP` wait, or a WS delegate (THE RULE);
- read the money tables or serve prices/market data (money-blind + H.8);
- validate the identity packet (trusted);
- reference the unbuilt funnel seams (`WireInstance`/`EndSession`/`FinalizeMarket`) from L7-0;
- re-implement anything the broker/market-data ships (settlement, the WS handler body, the candle
  history handler, the price cache) — L7 only wires and dispatches;
- put L7 under `broker/` — it is a top-level sibling consuming the broker.

## Flags (surface in orientation)
- **Repo placement** is orientation's call (docs leave it unspecified) — but L7 is a top-level sibling
  of `broker/`/`exchange/`, not under `broker/`. Confirm the package path.
- **BrokerPort grows per phase** — three seams (`WireInstance`, `EndSession`, `FinalizeMarket`) are
  unbuilt broker additions, each built as a prereq in its consuming phase.
- **Agent/liquidity ports are stubs** (L2) — markets won't truly trade until L2; build + test against
  stubs.
- **Conductor construction / `ohlcv` pool / port injection** is L7-0's (the spin-up doc §8 parked it as
  "future bootstrap"; it is this scaffold). Decide whether the `ohlcv` read pool is its own handle or
  reuses `Store`'s pool — the spin-up doc specifies L7's **own** read pool; confirm.
- **Root `/CLAUDE.md`** names only exchange + broker as the two clusters; L7 is a third. A one-line
  mention is optional, not required (L7 inherits root's universal rules).

## Tests (scaffold — objective, no goldens/auditor)
- Config: `loadConfig`/`listConfiguredMarkets` over a temp `cfgDir` (valid, missing, malformed → error).
- Registry: reserve/finalize/rollback mutations under the mutex; state-enum transitions; `-race` on
  concurrent registry access (the mutex is the key race surface).
- Construction/DI: Conductor builds from a `bringup.Runtime` (use `BringUpForTest`); the compile-time
  `var _ BrokerPort = (*funnel.Broker)(nil)` / `var _ MarketDataPort = (*marketdata.MarketData)(nil)`
  assertions hold; stub agent/liq ports satisfy their interfaces.
- Routing: a route registered on `Runtime.Mux` by L7 is reachable (a trivial handler smoke test),
  proving the mux handoff without adding real endpoints.
- `go test ./... -race` standing gate.