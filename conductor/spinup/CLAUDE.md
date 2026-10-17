# layer7/spinup — Canonical Market Bring-Up (L7-1)

<!-- Provisional path; orientation's call. READ ORDER:
     1) /CLAUDE.md  2) layer7/CLAUDE.md (L7 domain + scaffold)  3) THIS file
     4) docs/LAYER7_CANONICAL_SPINUP_IMPL.md (the spec) -->

## Spec & authority
`LAYER7_CANONICAL_SPINUP_IMPL.md` is the spec (decisions C.1–C.13). Builds on the L7-0 scaffold
(Conductor, Registry, config, ports, THE RULE). **Code wins over doc** — exchange `wire`/`clock`/`gate`
and the shipped broker/marketdata surfaces are verified; bind to them exactly.

## What it IS (one line)
Bring a canonical market to life on its schedule, user-independent: **timer → reserve(Provisioning) →
build args → construct → seed → wire both axes → Start → flip Live**, leaving a trading, admittable
market with both consumers draining and the `CacheRegistry` populated. Stops at `Live`; join is L7-2.

## BROKER PREREQ (build first, this phase) — the one unbuilt seam
`funnel.Broker.WireInstance(exchangeID string, private <-chan contract.EngineResult)` does **not**
exist yet (verified — only comments). Build it in `broker/funnel` first: body = log + `go
b.runSettlementConsumer(private)` (that method exists, unexported, self-terminates on `Private` close;
`exchangeID` is logging only). Then **grow L7's `BrokerPort`** with `WireInstance` and keep the
`var _ BrokerPort = (*funnel.Broker)(nil)` assertion passing. This is C.4.

## Load-bearing rules (from the spec)
- **Trigger (C.1):** one timer goroutine **per configured market**, armed to `continuous-open −
  preopen_lead` in the market TZ; weekday re-arm; `spinUpCanonical` runs **inline** on the timer
  goroutine (naturally serializes same-market bring-up). `nextCanonicalFireTime`: weekdays only (no
  holiday calendar), process-started-late ⇒ next weekday (C.9).
- **Order is the design (C.2):** reserve → `buildExchangeArgs` → `NewExchange` → **`BRING_UP` (sync) →
  `SeedLiquidity` (fire-and-trust) → `broker.WireInstance(Private)` → `marketData.WireInstance(Public,
  ModeCanonical)` → `Start`** → flip Live. **Seed before wire** so every fallible step precedes
  goroutine spawn; abort = delete the Provisioning record (`abortProvisioning`), nothing to tear down.
- **Wire immediately before `Start` (C.3):** edges are unbuffered, the first emit must not block.
- **THE RULE:** lock taken exactly twice — the reserve (create Provisioning record; a second trigger
  for the same market finds it and returns `ErrAlreadyUp`) and the commit (set `exch`/`inbound`/`fanout`/
  `closeAt`, flip `Live`). **Never** across `BRING_UP`/construct/wire/`Start`.
- **`buildExchangeArgs`:** `numShards` default **1** (C.7); `SessionSchedule` = config boundaries
  anchored to today in the market TZ, **sorted ascending, terminal = `PhaseClosed`** — validate this in
  L7 and fail fast, because `clock.NewSessionClock` inside `NewExchange` **panics** on a bad schedule.
  Per-instrument `wire.InstrumentSpec{Instrument, Reference, BandBps, Rules:gate.Rulebook{TickSize},
  OHLCVInterval, OHLCVStart:openInstant}`.
- **`readReference` (C.10):** `SELECT close FROM candles WHERE market=$1 AND instrument=$2 ORDER BY
  timestamp DESC LIMIT 1` on L7's own `ohlcv` pool; **no row → `inst.SeedPrice`** (first-ever session);
  **genuine DB error → fail spin-up**. Keys on the market **tag**, not the instance id.
- **Wiring calls (shipped):** `broker.WireInstance(string(marketID), exch.Private)` then `fanout :=
  marketData.WireInstance(marketdata.ExchangeID(marketID), exch.Public, string(tag),
  marketdata.ModeCanonical)` — **retain the returned `*FanOut`** on the `Market` record (Model A, C.5).
- **`MarketID = ExchangeID = "<tag>:<sessionDate>"`** (C.6). `tickInterval = time.Second` (C.8).
- **Flip Live right after `Start` (C.12):** `Live` = constructed+populated+started = admittable;
  "tradeable" is the exchange clock's concern, never an L7 state.

## Deps & seam
L7-0 scaffold; the NEW `funnel.WireInstance` (above); shipped `marketData.WireInstance`;
`wire.NewExchange`/`Start`/`Stop` + edges (`simulator/exchange/wire`), `clock.SessionSchedule`/
`PhaseBoundary`/`Phase`, `gate.Rulebook`; the placeholder `AgentOrchestrator.BringUp` (SYNC, bounded
timeout) + `LiquidityOperator.SeedLiquidity` (fire-and-trust) **stubs** — a spun-up market has no real
agents until L2.

## Do NOT
- hold the registry lock across `BRING_UP`/construct/wire/`Start` (THE RULE);
- wire or `Start` before seeding (abort must have nothing spawned);
- let a bad schedule reach `NewExchange` (validate → fail fast, avoid the clock panic);
- retain `Private`/`Public` (retain only the returned `FanOut` + `inbound`);
- enter `PhaseClosingAuction` with order flow — the exchange auction is deferred and
  `deferredAuctionBatch.Add` **panics**; `closing_auction` config is parked, out of scope here.

## Flags (orientation)
- **Build `funnel.WireInstance` first**, grow `BrokerPort`, keep the compile assertion green.
- `BRING_UP`/`SeedLiquidity` are stubs (L2); signatures will change. Bounded `BRING_UP` timeout; on
  fail → `abortProvisioning` + next-day retry.
- `numShards` default-1 drift (Tier-1 assumed many) — a many-shard canonical market sets `num_shards`.
- No holiday calendar / no crash-startup-recovery (down till next day's timer) — parked.

## Tests (objective; no goldens/auditor)
- `nextCanonicalFireTime`: weekday roll, preopen_lead subtraction, late-start → next day (injected `now`).
- `buildExchangeArgs`: schedule sort + terminal-PhaseClosed assertion (bad → L7 error, not panic);
  numShards default; specs shape; `readReference` no-row→seed, row→close, DB-error→fail.
- `spinUpCanonical` with a **fake ExchangeFactory + stub ports + fake broker/marketData ports**: reserve
  gate (second trigger → ErrAlreadyUp); seed-before-wire ordering; abort deletes the Provisioning record
  (no goroutine/registry residue); commit flips `Live` with handles set; THE RULE (assert no lock across
  call-outs — structural review).
- `funnel.WireInstance`: spawns the settlement consumer; exits on `Private` close (in `broker/funnel`).
- `go test ./... -race`.