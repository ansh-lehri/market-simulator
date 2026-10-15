Broker Market-Data Component — Wiring & Lifecycle (Implementation Design)

Altitude. Implementation design: signatures, the construction sequence, and the lifecycle flow — grounded against the L7 docs and fresh broker/exchange source read this session.

Where this sits. Continues the base pair (BROKER_MARKETDATA_DESIGN.md, BROKER_MARKETDATA_IMPLEMENTATION_DESIGN.md) and the bodies piece (BROKER_MARKETDATA_IMPL_COMPONENT_BODIES.md). It closes the item both base docs parked as "rides a still-moving seam":

DESIGN §10 Parked: "Ingestor ↔ exchange attachment (WireInstance / Phase 7)"; "MarketPriceCache wiring — how one cache injected at NewBroker resolves to the right instance's snapshot."

This is the component-internal wiring (what WireInstance builds, and the order of operations). The parts that are genuinely L7's (who calls WireInstance, the session → FanOut binding store) stay L7's and are only referenced here.

Source docs this reconciles against (named, with what each is)
BROKER_MARKET_DATA_L7_EXPECTATIONS.md — the component's contract toward L7 (session lifecycle / orchestration): the expectations E1–E7 and the WireInstance seam.
LAYER7_TIER1_MARKET_CONSTRUCTION_AND_WIRING.md — how L7 stands an exchange instance up, feeds it, wires it to services, starts/stops it; the per-session cycle (§3) and the canonical daily timer (§4).
broker/funnel/mkt_cache.go — the real MarketPriceCache interface (keyed (ExchangeID, InstrumentID)).
broker/funnel/binding.go — SessionBinding.ExchangeID exists (the reader key).
broker/bringup/bringup.go — current bootstrap; passes nil for mktCache today (nop).
BROKER_PHASE6_IMPLEMENTATION_DESIGN.md — the Phase-6 broker spec (position marks + market-buy pricing); defines a MarketDataCache interface (see §5 reconciliation — the code has since moved past it).
1. Cardinality — what exists once vs per instance

Per BROKER_MARKET_DATA_L7_EXPECTATIONS.md §1, extended with this session's additions (MarketSnapshot, the cache registry):

Part	Cardinality	Built by
CandleStore (the candles table layer)	one, process-lifetime	broker bootstrap
ReadEngine (rollup + range read)	one, process-lifetime	broker bootstrap
HTTP server (GET /candles) + the GET /ws accept handler	one, process-lifetime	broker bootstrap
cache registry (map[ExchangeID]*MarketSnapshot)	one, process-lifetime	broker bootstrap
Ingestor (tape consumer)	one per exchange instance	WireInstance (invoked by L7)
FanOut (subscriber registry)	one per exchange instance	WireInstance
MarketSnapshot (retained live state)	one per exchange instance	WireInstance

The shared parts are broker-bootstrap concerns — L7 is not involved. L7 drives the per-instance column + the session→feed binding.

2. The MarketData holder + the reconciled WireInstance

A process-lifetime holder owns the shared pieces and exposes the one entry point L7 calls:

type MarketData struct {
    store    *CandleStore
    engine   *ReadEngine
    registry *cacheRegistry        // map[ExchangeID]*MarketSnapshot — implements funnel.MarketPriceCache
}

func (md *MarketData) WireInstance(exch ExchangeID, tape <-chan emit.TapeEvent, market string, mode Mode) *FanOut

[CORRECTS the seam signature] BROKER_MARKET_DATA_L7_EXPECTATIONS.md §5 wrote WireInstance(tape, market, mode). It predates this session's MarketSnapshot split and the (ExchangeID, InstrumentID)-keyed cache, so it gains exch ExchangeID — the instance id. It's needed because ExchangeID is finer than market: many on-demand IN instances share market="IN" but each has its own exch. L7 already assigns this id and tags channels with it (LAYER7_TIER1… §3.1) and stamps it on SessionBinding.ExchangeID.

What WireInstance does, synchronously:

WireInstance(exch, tape, market, mode):
    snap := NewMarketSnapshot(exch)              // §bodies doc §2 — empty; instruments learned from tape (E3)
    md.registry.put(exch, snap)                  // shared-registry mutation (its own lock) → account side gains this instance
    roll := NewStreamRollup(market)              // forming-bar state (base IMPL §4)
    fan  := NewFanOut(snap)                       // closes over snap for snapshot join-seed
    go runMarketDataIngestor(tape, exch, market, mode, md.store, snap, roll, fan)   // sole Public consumer; parks on empty tape
    return fan
3. The existence sequence (end to end)

Two altitudes — the skeleton, then the narrated flow.

3.1 Skeleton
A. BROKER BOOTSTRAP  (once, process start — bringup, NOT L7)
   - CandleStore (one pgxpool), ReadEngine, cacheRegistry{} (empty), MarketData holder
   - NewBroker(..., cacheRegistry)                 // the registry IS the MarketPriceCache injected (replaces today's nil→nop)
   - register GET /candles and GET /ws on the one stdlib mux; start HTTP server

B. PER-INSTANCE WIRE  (L7, at instance creation — canonical daily timer OR on-demand join; LAYER7_TIER1 §3.3)
   1. build config (LAYER7_TIER1 §2) → NewExchange(numShards, schedule, instruments)   // 3 channels born, nothing running
   2. attach emit-side consumers BEFORE Start (LAYER7_TIER1 §3.3 ordering — so the first emit can't block):
        Private → broker.runSettlementConsumer(Private)         // existing Point 2
        Public  → md.WireInstance(exch, Public, market, mode)    // builds trio + registers snapshot; returns FanOut
   3. Start(tickInterval)                            // tape begins emitting → ingestor drains
   4. L7 records session→FanOut binding (E5) + ExchangeID on the SessionBinding

C. LIVE BIND  (L7, when a user/agent connects)
   - L7 resolves session → its FanOut → hands the WS connection that FanOut
   - client subscribes; snapshot subs seed from MarketSnapshot; candle subs no seed (R2)

D. TEARDOWN  (L7 drives; LAYER7_TIER1 §1.3 — Stop() owns edge-closing)
   - L7 calls exchange.Stop() → closes Inbound, drains shards, closes Public
   - ingestor for-range exits → cacheRegistry.remove(exch) → FanOut + MarketSnapshot GC'd
3.2 Narrated flow (the "how it comes into existence" walkthrough)
Stage 0 — broker boots (once). Bootstrap builds the shared pieces (CandleStore over one pgx pool, ReadEngine, the /candles + /ws routes on one mux) and the empty cache registry, then constructs the broker handing it that registry as its MarketPriceCache. The registry is empty, so price lookups miss — correct, no market is live yet.
Stage 1 — L7 decides a market is needed. Either a canonical market's daily timer fires (~20–25 min before open, preopen_lead, LAYER7_TIER1 §4) or a user starts an on-demand session. L7 reads the market config and assigns the instance its ExchangeID.
Stage 2 — L7 constructs the exchange. NewExchange(...) builds it and gives birth to the three channels (Inbound / Private / Public) — nothing running yet. This "built but not started" gap is where consumers must attach.
Stage 3 — L7 attaches consumers before Start (load-bearing order). Private → the broker's settlement consumer (exists). Public → WireInstance, which builds the MarketSnapshot, registers it in the shared registry (the moment the broker's order side gains a real price source for this market), builds the FanOut, and spawns the ingestor (now the sole Public consumer, parked). Returns the FanOut.
Stage 4 — L7 starts the exchange. Start(tickInterval) → the clock ticks, events flow, the parked ingestor drains and routes (persist candles, update snapshot, publish to FanOut). Market is live.
Stage 5 — L7 records the binding. session → FanOut (E5) + ExchangeID on the SessionBinding. Canonical users of one market resolve to the same FanOut (shared feed); an on-demand session gets its own FanOut.
Stage 6 — a user/agent connects. Client opens /ws; L7 hands it the bound FanOut; client subscribes; snapshot subs get an immediate seed, candle subs rehydrate via GET /candles.
Stage 7 — the order side reads prices (continuous, no extra wiring). Whenever the order path needs a price it reads the registry with the session's ExchangeID + instrument → lands on this instance's MarketSnapshot. Account side (pull) and WS side (push) read the same live state.
Stage 8 — teardown. L7 calls Stop(); closing Public is the only teardown signal; the ingestor exits, deregisters from the registry, and the FanOut + MarketSnapshot are GC'd.

History needs none of this. GET /candles runs off the shared ReadEngine + CandleStore, independent of any instance or binding — it works before anyone connects and, inside an on-demand session, returns canonical history (BROKER_MARKET_DATA_L7_EXPECTATIONS.md §3). L7 wiring is entirely a live-path concern.

4. The three corrections the L7 docs forced
Public is sole-consumed by market-data; the broker does NOT drain it a second time. [CORRECTS LAYER7_TIER1… §3.1] That doc says "Public → broker too (broker feeds its own best-ask cache off the tape)." But BROKER_MARKET_DATA_L7_EXPECTATIONS.md E2 requires the ingestor to be the sole consumer — two consumers of one Go channel split events, they don't duplicate. Resolution: the market-data ingestor is the only Public consumer; it populates MarketSnapshot; the broker's order path reads that snapshot via the injected MarketPriceCache (the registry). Same process, so "broker feeds its cache off the tape" is true indirectly — via market-data, not a second drain.
The cache registry needs a deregister-on-teardown step. The component is "self-cleaning" on tape close, but the MarketSnapshot was placed in a shared structure. Self-GC does not remove that entry, so the ingestor's exit must cacheRegistry.remove(exch), else the registry leaks and the account side reads a dead snapshot. New required step, beyond the docs' "self-clean."
MarketSnapshot instruments are tape-learned, not prebuilt. Covered in the bodies doc §2.1 — forced by E3 (L7 provides no instrument identities). Noted here because it's the third L7-driven correction.
5. ExchangeID + the MarketPriceCache reconciliation
ExchangeID source: L7 assigns one per instance (= the instance id), stamps it on SessionBinding.ExchangeID at session install, and passes it to WireInstance. The ingestor stamps it on outbound packets and on the registry entry. It is not on the exchange wire and not sent by WS clients (broker/funnel/mkt_cache.go says so explicitly: "the exchange wire does NOT carry ExchangeID; the ingestor stamps it on receipt because it knows which Public channel it was wired to").
The real interface (code wins): broker/funnel/mkt_cache.go defines MarketPriceCache with BestAsk(exchange ExchangeID, instrument) (Ticks, bool) and LastPrice(exchange, instrument) — keyed (ExchangeID, InstrumentID). The cacheRegistry implements exactly this: BestAsk(exch, i) → registry[exch].BestAsk(i). It is the single MarketPriceCache injected at NewBroker, replacing today's nopMarketCache (every lookup a deliberate miss; bringup.go passes nil today).
[CORRECTS stale doc] BROKER_PHASE6_IMPLEMENTATION_DESIGN.md still describes this seam as MarketDataCache keyed by instrument only. The code has since renamed it MarketPriceCache and re-keyed it (ExchangeID, InstrumentID). Per broker/exchangeseam/CLAUDE.md's rule "code wins over doc," the code shape above is authoritative.
6. What is L7's vs ours
L7's (not designed here): who calls WireInstance and when (canonical timer / on-demand join), the session → FanOut binding store and lookup (FanOutForSession), closing the tape at session end (E6), and enforcing one-live-session-per-user / one-market-per-session (E7).
Ours (this doc): the MarketData holder, WireInstance's body (build trio + register snapshot), the bootstrap of the shared pieces, and the deregister-on-teardown step.
Status after this doc

Closed: the WireInstance seam (reconciled signature + body), the full bring-up→bind→teardown sequence, the MarketPriceCache registry resolution, and the three L7-doc corrections.

Still L7's / parked (correctly): the session→FanOut binding representation, canonical scheduling, teardown choreography — all owned by Layer 7 (LAYER7_TIER1… §5), not this component.