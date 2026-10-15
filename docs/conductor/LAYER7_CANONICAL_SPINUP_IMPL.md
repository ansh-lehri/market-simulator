Layer 7 — Canonical Market Bring-Up: Implementation Design

What this doc is. The implementation design for how L7 brings a canonical market instance to life on its schedule, before any human joins. It consolidates the flow we settled in discussion: the end-to-end narrative, the final pseudo-flows, the typed design against the real exchange/broker/market-data contracts, and the locked decisions.

What it is NOT. It does not cover admission/join, the /v1/ws dispatcher body, teardown/reap, or on-demand. Those are separate flows (see §11). This doc stops at a Live, trading, admittable canonical market.

Authority & contracts. Typed against the shipped code: simulator/exchange/wire, .../clock, .../gate, .../emit, .../contract, simulator/broker/funnel, simulator/broker/marketdata. Where code and the older L7 prose disagree, code wins. Builds on LAYER7_TIER0_REGISTRY_AND_IDENTITY.md, LAYER7_TIER1_MARKET_CONSTRUCTION_AND_WIRING.md, LAYER7_TIER2_SEEDING.md, and the locked live-data flow (Phases A–I).

Repo layout is intentionally not specified here (package names, file paths, module path). That is the implementer's call. This doc specifies behavior, types, signatures, and flow. L7's own types are shown as Go without an import path; only the external packages L7 binds to are named by their real import paths, because those are facts.

1. The two axes (frame)

Everything splits into two independent pipelines sharing one process, one HTTP mux, one DB pool, never leaking state:

Account / money — keyed by AccountID; lives in broker/funnel + shards.
Market data / public — keyed by ExchangeID + InstrumentID; lives in broker/marketdata.

The one controlled hand-off is the CacheRegistry: market-data writes the per-instrument snapshot, the account side reads it for price lookups. L7 wires both axes at bring-up and then stays out of their internals.

2. Precondition — broker process already bootstrapped

Before any market exists, broker/bringup.BringUp has run once: N per-shard capstone bundles, the broker-owned Store, the market-data holder (CandleStore, ReadEngine, an empty CacheRegistry), the Broker constructed with the CacheRegistry injected as its price cache and started, and the HTTP mux with the eight /v1/account/* reads + /v1/candles registered (not /v1/ws). At this point the broker is alive but hollow: no exchanges, empty CacheRegistry, no sessions. L7 holds its dependencies (below) and an empty registry.

Canonical bring-up is user-independent — triggered by a clock, not by anyone arriving.

3. End-to-end narrative (the flow)
Timer fires. One L7 timer per configured market, armed to that market's continuous-open − preopen_lead in the market's own timezone. No user is involved.
Reserve (locked, microseconds). L7 creates the Market record in state Provisioning and inserts it into the registry. Provisioning is the gate: a second trigger for the same market waits instead of building a duplicate. Lock released.
Assemble inputs (off-lock). Read the market config; compute numShards, the SessionSchedule (config boundaries anchored to today in the market TZ, sorted, terminal = PhaseClosed), and per-instrument specs — including Reference read directly from the OHLCV candles table (prior close; seed_price fallback for the first-ever session).
Construct. wire.NewExchange(...) builds the instance and its three unbuffered edge channels. Nothing runs yet.
Seed (off-lock, still PhaseClosed). BRING_UP (synchronous — agents into existence) then SeedLiquidity (fire-and-trust — operator liquidity). These run before wiring so a seeding failure aborts cleanly with no goroutines spawned.
Wire both axes (before Start). Hand exch.Private to the broker (broker.WireInstance → settlement consumer) and exch.Public to market-data (marketData.WireInstance → ingestor + snapshot registered in CacheRegistry, returns the FanOut). L7 retains the FanOut on the Market record; forgets Private/Public. Wiring precedes Start because the edges are unbuffered and the first emit must not block.
Start. exch.Start(tickInterval) launches monitor + shards + ingress; the consumers begin draining. The clock is still PhaseClosed — Start does not open the market.
Flip Live (locked, microseconds). Commit the exchange handle, Inbound, FanOut, and closeAt onto the record and set State = Live (= constructed + populated + started = admittable). The provisioning work returns.
Market opens by time. The monitor crosses continuous-open; agents and the operator place orders → the book forms; trades flow on Public → the ingestor updates the snapshot and persists 1-min candles → the account path can now resolve BestAsk/LastPrice. No humans are subscribed yet; FanOut publishes drop harmlessly. The later user-facing open is a UI gate, not modeled in L7.

End state: a Live, trading canonical market with agents + operator, candles persisting, both consumers draining, CacheRegistry populated, and the FanOut retained — ready for the separate admission flow to wire humans in.

4. External contracts bound (exact, from shipped code)

Exchange (simulator/exchange/...):

go
func wire.NewExchange(numShards int, schedule clock.SessionSchedule, instruments []wire.InstrumentSpec) *wire.Exchange
func (*wire.Exchange) Start(tickInterval time.Duration)   // idempotent; launches goroutines
func (*wire.Exchange) Stop()                              // idempotent; NO-OP if never Start()ed

// exported edge FIELDS on *wire.Exchange:
//   Inbound chan<- contract.InboundMessage
//   Public  <-chan emit.TapeEvent
//   Private <-chan contract.EngineResult

type wire.InstrumentSpec struct {
    Instrument    contract.InstrumentID
    Reference     contract.Ticks
    BandBps       int
    Rules         gate.Rulebook      // { TickSize contract.Ticks }
    OHLCVInterval time.Duration
    OHLCVStart    time.Time
}
type clock.SessionSchedule struct { Boundaries []clock.PhaseBoundary }
type clock.PhaseBoundary   struct { At time.Time; Phase clock.Phase }
const ( clock.PhaseClosed; clock.PhaseContinuous; clock.PhaseClosingAuction )

clock.NewSessionClock (inside NewExchange) panics unless boundaries are strictly ascending by At and the terminal phase is PhaseClosed. L7 validates the schedule itself and fails fast with an L7 error, so bad config never reaches the exchange panic.

Stop() is a no-op before Start() (if monitorStop == nil { return }). This is why seeding is ordered before wiring (§7 abort): there is no clean pre-Start teardown for already-spawned consumer goroutines.

Broker (simulator/broker/funnel) — one new exported seam; everything else exists:

go
// NEW broker-side addition (the only broker change canonical bring-up needs).
func (b *funnel.Broker) WireInstance(exchangeID string, private <-chan contract.EngineResult)
//   body: logs with exchangeID, then `go b.runSettlementConsumer(private)`
//   runSettlementConsumer already exists (unexported); self-terminates when the
//   exchange closes Private at Stop(). exchangeID is for logging/telemetry only.

Market-data (simulator/broker/marketdata) — all shipped:

go
func (m *marketdata.MarketData) WireInstance(exchangeID marketdata.ExchangeID,
        tape <-chan emit.TapeEvent, market string, mode marketdata.Mode) *marketdata.FanOut
const ( marketdata.ModeCanonical; marketdata.ModeOnDemand )   // marketdata.ExchangeID = string

(marketData.WSConfig() + marketdata.NewWSHandler(fanOut, cfg) exist too, but are used by the later /v1/ws dispatcher flow, not bring-up.)

OHLCV DB — the market-data candles table (market, instrument, timestamp, open, high, low, close, volume; PK (market,instrument,timestamp)). L7 reads it directly via its own read pool. Reference keys on the market tag, not the per-instance id:

sql
SELECT close FROM candles WHERE market=$1 AND instrument=$2 ORDER BY timestamp DESC LIMIT 1
5. L7 types touched by bring-up
go
// --- identifiers / enums (Tier 0) ---
type MarketID   string      // "IN:2026-10-12"  (= the ExchangeID used across the seams)
type MarketTag  string      // "IN" | "US"
type Mode        int ; const ( Canonical Mode = iota; OnDemand )
type MarketState int ; const ( Provisioning MarketState = iota; Live; TearingDown )

// --- registry (bring-up touches Market only; sessions/byUser are admission) ---
type Registry struct {
    mu       sync.Mutex
    markets  map[MarketID]*Market
    sessions map[SessionID]*Session   // admission
    byUser   map[UserID]*Session      // admission
}
type Market struct {
    ID      MarketID
    Mode    Mode                            // Canonical
    Tag     MarketTag
    State   MarketState
    exch    *wire.Exchange
    inbound chan<- contract.InboundMessage  // = exch.Inbound; retained for admission's StartSession binding
    fanout  *marketdata.FanOut              // = WireInstance return; retained for the /v1/ws dispatcher (Model A)
    closeAt time.Time                       // terminal PhaseClosed instant; arms teardown/reap
    members map[SessionID]struct{}
}

// --- config (one per market) ---
type MarketConfig struct {
    Market        MarketTag
    Timezone      string          // "Asia/Kolkata"
    OHLCVInterval time.Duration    // "1m"
    NumShards     int              // 0/absent → default (1)
    PreopenLead   time.Duration    // "25m"
    Session       []SessionBoundaryConfig
    OnDemand      OnDemandConfig
    Instruments   []InstrumentConfig
}
type SessionBoundaryConfig struct { At string; Phase string }   // "09:15","continuous"
type OnDemandConfig        struct { Duration time.Duration }
type InstrumentConfig struct {
    ID        contract.InstrumentID
    TickSize  contract.Ticks
    BandBps   int
    SeedPrice contract.Ticks
    Liquidity LiquidityConfig
}
type LiquidityConfig struct { Levels int; SpacingPct float64; NearQty int64; Taper float64 }

// --- the Conductor: L7's top object; holds the registry + injected deps ---
type Conductor struct {
    reg       *Registry
    cfgDir    string
    ohlcv     *pgxpool.Pool          // L7's own read pool (candles)
    broker    BrokerPort
    market    MarketDataPort
    exch      ExchangeFactory
    agents    AgentOrchestrator      // L2 — signature/behavior WILL change (§11)
    liq       LiquidityOperator      // operator — WILL change (§11)
    now       func() time.Time       // injectable clock (tests)
    log       *slog.Logger
    timerQuit chan struct{}
    wg        sync.WaitGroup
}

const tickInterval = time.Second

// --- consumer-side ports (concrete: *funnel.Broker, *marketdata.MarketData, wire.NewExchange) ---
type ExchangeFactory interface {
    NewExchange(numShards int, schedule clock.SessionSchedule, instruments []wire.InstrumentSpec) *wire.Exchange
}
type BrokerPort interface {
    WireInstance(exchangeID string, private <-chan contract.EngineResult)
}
type MarketDataPort interface {
    WireInstance(exchangeID marketdata.ExchangeID, tape <-chan emit.TapeEvent,
                 market string, mode marketdata.Mode) *marketdata.FanOut
}
// PLACEHOLDER ports — signature + behavior WILL change (§11); kept as-is for now.
type AgentOrchestrator interface { BringUp(market MarketTag, mode Mode, instruments []contract.InstrumentID) error } // SYNC
type LiquidityOperator  interface { SeedLiquidity(marketID MarketID, specs []LiquiditySpec) }                        // fire-and-trust
6. Final pseudo-flows (as settled before this design)
6.1 Timer subsystem — one goroutine per configured market
Conductor.Start():
    tags := listConfiguredMarkets()          // every market that has a config
    timerQuit = make(chan struct{})
    for tag in tags:
        wg.Add(1); go runCanonicalTimer(tag)

runCanonicalTimer(tag):
    defer wg.Done()
    loop:
        cfg, err := loadConfig(tag)
        if err: log; if sleepOrQuit(1h, timerQuit) return; continue   // don't spin on bad config
        fireAt := nextCanonicalFireTime(cfg, now())
        timer  := NewTimer(fireAt - now())
        select:
          case <-timerQuit:  timer.Stop(); return                     // shutdown
          case <-timer.C:    _, err := spinUpCanonical(tag)           // IT'S TIME → bring up
                             if err: log "stays down until next arm"
                             // loop recomputes fireAt → next weekday → re-arm

nextCanonicalFireTime(cfg, now):
    loc := LoadLocation(cfg.Timezone)
    openHHMM := cfg.Session[0].At                 // first boundary = continuous-open
    for dayOffset := 0 ..:
        day := now.In(loc) + dayOffset
        if isWeekend(day): continue               // no holiday calendar yet (§11)
        fireAt := atWallClock(day, openHHMM, loc) - cfg.PreopenLead
        if fireAt.After(now): return fireAt        // first future weekday fire
        // today's already passed → next day (process-started-late ⇒ stays down today)

Per-market by construction (own TZ, own open, own lead). spinUpCanonical runs inline on the timer goroutine — safe, because its slow middle is off-lock, and it naturally prevents an overlapping bring-up of the same market.

6.2 spinUpCanonical — construct → seed → wire → start → Live
spinUpCanonical(tag) -> (MarketID, error):
    cfg      := loadConfig(tag)
    marketID := tag + ":" + today(cfg.TZ)                      // "IN:2026-10-12"

    // ---- reserve (locked, µs) ----
    lock:
      if markets[marketID] exists: unlock; return marketID, ErrAlreadyUp   // Provisioning gate
      m := &Market{ID:marketID, Mode:Canonical, Tag:tag, State:Provisioning, members:{}}
      markets[marketID] = m
    unlock

    // ---- slow work, OFF-LOCK (THE RULE) ----
    numShards, schedule, specs, err := buildExchangeArgs(cfg)
    if err: abortProvisioning(marketID, err); return "", err

    exch := exch.NewExchange(numShards, schedule, specs)       // nothing running; no goroutines

    // SEED FIRST (before wiring) so a failure aborts with nothing spawned:
    if err := agents.BringUp(tag, Canonical, ids(specs)); err != nil {   // SYNC, bounded timeout
        abortProvisioning(marketID, err); return "", err
    }
    liq.SeedLiquidity(marketID, liquiditySpecs(cfg, specs))    // fire-and-trust, no wait

    // WIRE both axes (immediately before Start; unbuffered edges):
    broker.WireInstance(string(marketID), exch.Private)        // settlement consumer parks
    fanout := market.WireInstance(ExchangeID(marketID), exch.Public, string(tag), ModeCanonical)
                                                               // ingestor parks; snapshot in CacheRegistry

    exch.Start(tickInterval)                                   // goroutines live; clock still PhaseClosed

    // ---- commit (locked, µs) ----
    lock:
      m.exch, m.inbound, m.fanout = exch, exch.Inbound, fanout
      m.closeAt = lastBoundaryInstant(schedule)
      m.State   = Live
    unlock
    return marketID, nil

abortProvisioning(id, cause):
    lock: delete(markets, id)    // only Provisioning, no sessions, nothing wired/started → nothing to tear down
    unlock; log cause

Lock taken exactly twice. Every fallible step (buildExchangeArgs, NewExchange, BRING_UP) precedes wiring, so abort is always "delete the Provisioning record" — no goroutine leak, no CacheRegistry residue. After wiring, the only remaining step is the no-error, idempotent Start.

6.3 Builders
buildExchangeArgs(cfg) -> (numShards, schedule, specs, err):
    numShards = cfg.NumShards>0 ? cfg.NumShards : 1           // default 1
    loc = LoadLocation(cfg.Timezone)
    schedule.Boundaries = for each cfg.Session[i]:
        { At: atWallClock(today(loc), At_i, loc), Phase: mapPhase(Phase_i) }
    sort ascending by At
    assert Boundaries[0] is session start AND Boundaries[last].Phase == PhaseClosed  // else err (avoid exchange panic)
    openInstant = Boundaries[0].At
    specs = for each inst in cfg.Instruments:
        wire.InstrumentSpec{
            Instrument:    inst.ID,
            Reference:     readReference(cfg.Market, inst),
            BandBps:       inst.BandBps,
            Rules:         gate.Rulebook{TickSize: inst.TickSize},
            OHLCVInterval: cfg.OHLCVInterval,
            OHLCVStart:    openInstant,
        }

readReference(tag, inst) -> contract.Ticks:
    row := query "SELECT close FROM candles WHERE market=tag AND instrument=inst.ID ORDER BY timestamp DESC LIMIT 1"
    if no row:  return inst.SeedPrice            // first-ever session
    if db error: FAIL spin-up (bubble err)       // only a real DB error aborts
    return row.close

mapPhase: "continuous"→PhaseContinuous, "closing_auction"→PhaseClosingAuction, "closed"→PhaseClosed, else err
6.4 The new broker seam
// package funnel
func (b *Broker) WireInstance(exchangeID string, private <-chan contract.EngineResult):
    log.Info("settlement consumer starting", exchangeID)
    go b.runSettlementConsumer(private)          // existing, unchanged; self-terminates on Private close
7. Locked decisions
#	Decision
C.1	Trigger = one timer goroutine per configured market, armed to continuous-open − preopen_lead in the market TZ; weekday re-arm; inline spinUpCanonical.
C.2	Seed before wire (BRING_UP → SeedLiquidity → broker.WireInstance → marketData.WireInstance → Start) so every fallible pre-Start step precedes goroutine spawn; abort = delete the Provisioning record.
C.3	Wiring sits immediately before Start (unbuffered edges, first emit must not block).
C.4	broker.WireInstance(exchangeID, private) is the new exported broker seam; wraps go runSettlementConsumer.
C.5	Market-data: marketData.WireInstance(exchID, Public, tag, ModeCanonical); L7 retains the returned *FanOut on the Market record (Model A).
C.6	MarketID = ExchangeID = "<tag>:<sessionDate>".
C.7	numShards default = 1 when config is 0/absent (a canonical market wanting many sets num_shards explicitly — see §8 drift note).
C.8	tickInterval = time.Second (L7 constant, not config).
C.9	Process starts after today's fireAt → roll to next weekday; market stays down today.
C.10	Reference: prior-close from candles; no row → seed_price; genuine DB error → fail spin-up.
C.11	BRING_UP is SYNC with a bounded timeout; on failure abort + next-day retry. SeedLiquidity is fire-and-trust.
C.12	Provisioning → Live flips right after Start (Live = admittable; "tradeable" is the exchange clock's concern, not an L7 state).
C.13	Registry = one mutex; THE RULE — lock only for the two µs flips, never across a call-out or the BRING_UP wait.
8. Flagged / will-change / parked
BRING_UP & SeedLiquidity signatures + behavior WILL change. Kept here as the placeholder ports (AgentOrchestrator.BringUp, LiquidityOperator.SeedLiquidity) with their current shape. The real shapes land when L2 / the operator are designed; update the ports then.
closing_auction — parked (per direction). Not designed/guarded here. Note for whoever runs it: the exchange's auction package is deferred and deferredAuctionBatch.Add panics, so a schedule that enters PhaseClosingAuction with order flow will panic the exchange. Config policy for this is out of scope for this doc.
L2 agents / operator reaching the instance inbound — parked. How L2's agents and the operator bind broker sessions onto this instance's Inbound is their concern; L7 only fires the triggers. Left for the L2 work.
numShards default drift. Tier 1 §2.4 assumed "canonical = many shards"; with the default now 1, a many-shard canonical market must set num_shards in its config. Conscious choice, noted.
L7 bootstrap / DI. Who constructs the Conductor, opens the ohlcv read pool, and injects the ports is a separate (future) concern. Bring-up assumes they're present.
Holiday calendar — weekdays only for now (later config).
Crash startup-recovery — dropped for beta (a mid-day crash leaves the market down until the next day's timer; C.9).
9. Failure summary (bring-up)
Failure point	Handling
Config load (in timer)	log; retry in 1h; market not brought up this arm
buildExchangeArgs (bad schedule / DB error on reference)	abortProvisioning (delete Provisioning record)
NewExchange precondition panic	prevented by schedule validation in buildExchangeArgs
BRING_UP fail/timeout	abortProvisioning; next-day timer retries
after wiring (only Start remains)	no error path; Start is idempotent
10. End state (one canonical market, Live)

Exchange running (monitor + shards + ingress); settlement consumer + market-data ingestor draining; CacheRegistry holds {MarketID: snapshot}; 1-min candles persisting once open; registry Market record Live with exch + inbound + fanout + closeAt. No human session. Ready for admission.

11. Out of scope (separate flows)

Admission/join (broker.StartSession, EnsureAccount, ParticipantID, Session record, SessionID → FanOut bind); the /v1/ws dispatcher body (decided Model A — L7 resolves sessionID → fanout and delegates to marketdata.NewWSHandler; runs at connect-time, not bring-up); teardown / reap (exch.Stop, CacheRegistry.Remove, unwire); on-demand deltas.