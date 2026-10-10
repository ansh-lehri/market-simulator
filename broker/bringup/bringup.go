package bringup

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"simulator/broker/funnel"
	"simulator/broker/marketdata"
	"simulator/broker/reads"
	"simulator/broker/store"
	"simulator/connectivity/config"
	"simulator/connectivity/db"
	"simulator/economy/capstone"
	"simulator/economy/lifecycle"
	"simulator/economy/persistence"
)

// Runtime is the handle BringUp returns and Shutdown consumes. It is
// NOT a service — just a container for the three things that outlive
// one call: the running broker, the per-shard bundles (held for
// LIFO teardown), and the broker-owned Store with its own pool
// (closed last on shutdown).
//
// bundles and storePool are unexported — the broker's lifecycle is
// mediated entirely through Shutdown. Broker and store are exported
// because callers need them to route traffic (Broker) and do off-shard
// durable work (Store).
type Runtime struct {
	Broker *funnel.Broker
	Store  *store.Store

	// Market is the Phase-7 (B10) market-data holder — process-lifetime
	// pieces (CandleStore, ReadEngine, CacheRegistry). L7 (when it
	// exists) calls Market.WireInstance per exchange instance; the
	// CacheRegistry inside Market is also the funnel.MarketPriceCache
	// injected into the broker (replaces the Phase-6 nil). Exposed so
	// L7 can reach it.
	Market *marketdata.MarketData

	// Mux carries the account read routes (eight from broker/reads)
	// PLUS the Phase-7 GET /v1/candles history route. Exposed as
	// *http.ServeMux (not http.Handler) so callers can register more
	// routes on it — health probes, admin surfaces, metrics scraping,
	// the bank-transfer handler, and L7's own /v1/ws dispatcher. The
	// bank-transfer handler is deliberately not registered here (see
	// buildMux below for the cycle rationale); /v1/ws likewise waits
	// on L7 because the per-session FanOut dispatch is L7's seam, not
	// marketdata's.
	Mux *http.ServeMux

	bundles []*capstone.Bundle

	// shutdownOnce makes Shutdown idempotent — callers typically defer
	// Shutdown AND call it on an error path, so the sync.Once absorbs
	// the duplicate without double-closing pool handles.
	shutdownOnce sync.Once
}

// BringUp loads connectivity config from env once, then stands up the
// full broker:
//
//  1. BrokerConfig.validate() — fail fast on bad dials.
//  2. connectivity/config.Load() — reads DATABASE_URL / REDIS_URL ONCE.
//  3. buildCountryStack(cfg.Country) — pick cost module + adapter.
//  4. For k in [0, ShardCount): capstone.Wire → one bundle. On any
//     Wire failure, every bundle already built is Close'd (LIFO) and
//     the error returned — a partial Runtime never escapes.
//  5. Open the Store's OWN Postgres pool (same cluster, independently
//     sized via cfg.StoreMaxConns / defaultStoreMaxConns). On failure,
//     every bundle is Close'd and the error returned.
//  6. marketdata.NewMarketData — build the Phase-7 (B10) holder:
//     CandleStore borrowed from brokerStore.Pool(), ReadEngine, and
//     the CacheRegistry (empty, no instances yet). Threaded tunables
//     for the HTTP /candles read + WS transport come from BrokerConfig.
//  7. funnel.NewBroker(cfg.BrokerID, cfg.InboxDepth, bundles, store,
//     money, marketData.MarketPriceCache()). The registry structurally
//     satisfies funnel.MarketPriceCache; readers that pass
//     sess.binding.ExchangeID land on the right instance's snapshot.
//     Account-side marks stop degrading to MarkAvailable:false once
//     L7 begins calling marketData.WireInstance per exchange instance.
//  8. broker.Start() — spawn N owner goroutines; each parks on its
//     inbox.
//
// End state: N shard owners (empty, parked) + N closed-trade drainers
// + a ready Store. No accounts, no markets — Tier 0's terminal state.
func BringUp(ctx context.Context, brokerCfg BrokerConfig) (*Runtime, error) {
	infra, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("bringup: config.Load: %w", err)
	}
	return bringUp(ctx, brokerCfg, infra)
}

// BringUpForTest is the public shim external *_test packages use to build
// a Runtime against a caller-supplied connectivity.Config (typically
// composed from TEST_DATABASE_URL / TEST_REDIS_URL). Delegates straight
// to bringUp; exists only so the broker/funnel integration tests can wire
// a real Runtime through the one correct path without duplicating it.
//
// Production code must NOT call this — use BringUp, which loads config
// from env once per process (per connectivity/CLAUDE.md's "Load() is the
// only env read" invariant).
func BringUpForTest(ctx context.Context, brokerCfg BrokerConfig, infraCfg config.Config) (*Runtime, error) {
	return bringUp(ctx, brokerCfg, infraCfg)
}

// bringUp is the testable core. Public BringUp delegates here with an
// env-loaded config; tests build a connectivity.Config from TEST_* env
// values and call this directly so they never touch DATABASE_URL /
// REDIS_URL (which would be dev/prod data in a developer environment).
func bringUp(ctx context.Context, brokerCfg BrokerConfig, infraCfg config.Config) (*Runtime, error) {
	if err := brokerCfg.validate(); err != nil {
		return nil, fmt.Errorf("bringup: %w", err)
	}

	stack, err := buildCountryStack(brokerCfg.Country)
	if err != nil {
		return nil, fmt.Errorf("bringup: buildCountryStack: %w", err)
	}

	bundles := make([]*capstone.Bundle, 0, brokerCfg.ShardCount)
	closeAlreadyBuilt := func() {
		// LIFO close on partial-failure rollback.
		for bundleIndex := len(bundles) - 1; bundleIndex >= 0; bundleIndex-- {
			bundles[bundleIndex].Close()
		}
	}
	for shardIndex := 0; shardIndex < brokerCfg.ShardCount; shardIndex++ {
		wiredBundle, wireErr := capstone.Wire(ctx, capstone.Options{
			ConnConfig:     infraCfg,
			RedisNamespace: "", // prod — L5 §7 verbatim keys
			Cost:           stack.Cost,
			CostAdapter:    stack.Adapter,
		})
		if wireErr != nil {
			closeAlreadyBuilt()
			return nil, fmt.Errorf("bringup: capstone.Wire shard %d: %w", shardIndex, wireErr)
		}
		bundles = append(bundles, wiredBundle)
	}

	// Store pool is dialed from a COPY of the infra config with its own
	// MaxConns; the per-shard bundles' pools are unaffected.
	storeInfra := infraCfg.Postgres
	storeInfra.MaxConns = brokerCfg.storeMaxConnsOrDefault()
	storePool, poolErr := db.NewPostgres(ctx, storeInfra)
	if poolErr != nil {
		closeAlreadyBuilt()
		return nil, fmt.Errorf("bringup: Store pool: %w", poolErr)
	}

	// Store's OWN Redis client. Same cluster + same empty KeyNamespace as
	// each per-shard bundle so a snapshot the Store writes at session load
	// lands at the exact key the owning shard later mutates and the save
	// path deletes. Phase-2 supersession P2-1·D3.
	storeRedis, redisErr := db.NewRedis(ctx, infraCfg.Redis)
	if redisErr != nil {
		storePool.Close()
		closeAlreadyBuilt()
		return nil, fmt.Errorf("bringup: Store redis: %w", redisErr)
	}

	// Build the Store's lifecycle.Service over an UNSTARTED
	// persistence.Component. The Store never calls EnqueueClosedTrade (the
	// closed-trade drainer belongs on the live per-shard bundles), so the
	// Component's drainer is never Started and no goroutine spawns here.
	// Match capstone.Wire's prod config: empty KeyNamespace so keys match
	// LAYER_5_PERSISTENCE §7 verbatim.
	storePersistCfg := persistence.Config{KeyNamespace: ""}
	storePersist := persistence.NewComponent(storePool, storeRedis, stack.Adapter, storePersistCfg)
	// NOTE: no storePersist.Start — unstarted by design.
	storeLifecycle := lifecycle.NewService(storePersist)

	brokerStore := store.NewStore(storePool, storeRedis, storeLifecycle)

	// Phase-7 (B10): build the market-data holder BEFORE NewBroker so
	// the CacheRegistry inside can be injected as mktCache (replaces
	// the Phase-6 nil). CandleStore shares brokerStore's pool via
	// Pool() — borrowed, not owned (store.Close handles pool teardown).
	marketDataCandleCfg := marketdata.Config{
		ReadTimeout: brokerCfg.candleReadTimeoutOrDefault(),
	}
	marketDataWSCfg := marketdata.WSConfig{
		ConnBuffer:           brokerCfg.wsConnBufferOrDefault(),
		LagEvictionThreshold: brokerCfg.wsLagEvictionThresholdOrDefault(),
		PingInterval:         brokerCfg.wsPingIntervalOrDefault(),
		ReadTimeout:          brokerCfg.wsReadTimeoutOrDefault(),
		WriteTimeout:         brokerCfg.wsWriteTimeoutOrDefault(),
	}
	marketData := marketdata.NewMarketData(brokerStore.Pool(), marketDataCandleCfg, marketDataWSCfg)

	brokerMoney := funnel.MoneyConfig{
		StartingCapital: brokerCfg.StartingCapital,
		StartingBank:    brokerCfg.StartingBank,
		OnDemandMinimum: brokerCfg.OnDemandMinimum,
	}
	// Phase-7 wire-up: pass marketData.MarketPriceCache() as mktCache.
	// Structural interface match — the registry's BestAsk/LastPrice
	// signatures match funnel.MarketPriceCache (both ExchangeID aliases
	// resolve to string). Readers that pass sess.binding.ExchangeID
	// land on the right instance's MarketSnapshot. Account-side marks
	// stop degrading to MarkAvailable:false once L7 starts calling
	// marketData.WireInstance per exchange instance.
	brokerInstance := funnel.NewBroker(
		brokerCfg.BrokerID, brokerCfg.InboxDepth, bundles, brokerStore, brokerMoney,
		marketData.MarketPriceCache(),
	)
	brokerInstance.Start()

	httpMux := buildMux(brokerInstance, brokerStore, marketData, brokerCfg)

	slog.Info("bringup: broker up",
		"broker_id", brokerCfg.BrokerID,
		"shard_count", brokerCfg.ShardCount,
		"inbox_depth", brokerCfg.InboxDepth,
		"country", brokerCfg.Country,
		"store_max_conns", brokerCfg.storeMaxConnsOrDefault(),
		"read_timeout", brokerCfg.readTimeoutOrDefault(),
		"store_lifecycle", "unstarted-component",
	)
	return &Runtime{
		Broker:  brokerInstance,
		Store:   brokerStore,
		Market:  marketData,
		Mux:     httpMux,
		bundles: bundles,
	}, nil
}

// buildMux constructs the HTTP mux and registers:
//   - the eight account read routes from broker/reads
//   - the Phase-7 GET /v1/candles history route from broker/marketdata
//
// The bank-transfer handler is deliberately NOT registered here. Its
// integration tests already import this package, so pulling
// broker/transfer in from bringup would introduce a build-time import
// cycle. The caller wires it with:
//
//     runtime.Mux.Handle(
//         transfer.Path,
//         transfer.NewHandler(runtime.Broker, runtime.Store,
//             transfer.Config{TransferTimeout: cfg.TransferTimeout}),
//     )
//
// after BringUp returns. GET /v1/ws is likewise NOT registered here —
// the per-session FanOut dispatch is L7's seam (marketdata/CLAUDE.md
// explicitly scopes "session→FanOut binding" out of this component).
// L7 will register /v1/ws against its own dispatcher once it exists;
// the WSConfig for that handler is already held on marketData.WSConfig().
//
// Keeping transfer/ws on the caller side is one extra line each at
// the wiring root in exchange for a clean dependency graph. The reads
// + marketdata packages have no back-import into bringup, so their
// handlers can be registered here without any cycle.
func buildMux(
	brokerInstance *funnel.Broker,
	brokerStore *store.Store,
	marketData *marketdata.MarketData,
	brokerCfg BrokerConfig,
) *http.ServeMux {
	readsCfg := reads.Config{ReadTimeout: brokerCfg.readTimeoutOrDefault()}

	mux := http.NewServeMux()
	mux.Handle(reads.PathBalance, reads.NewBalanceHandler(brokerInstance, readsCfg))
	mux.Handle(reads.PathPositions, reads.NewPositionsHandler(brokerInstance, readsCfg))
	mux.Handle(reads.PathOrders, reads.NewOrdersHandler(brokerInstance, readsCfg))
	mux.Handle(reads.PathObservation, reads.NewObservationHandler(brokerInstance, brokerStore, readsCfg))
	mux.Handle(reads.PathTrades, reads.NewTradesHandler(brokerStore, readsCfg))
	mux.Handle(reads.PathPnL, reads.NewPnLHandler(brokerStore, readsCfg))
	mux.Handle(reads.PathCosts, reads.NewCostsHandler(brokerStore, readsCfg))
	mux.Handle(reads.PathCostsSummary, reads.NewCostsSummaryHandler(brokerStore, readsCfg))

	marketData.RegisterCandlesRoute(mux)

	slog.Info("bringup: http mux built", "read_routes", 8, "marketdata_routes", 1)
	return mux
}
