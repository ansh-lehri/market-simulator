package conductor

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/broker/marketdata"
)

// wsHandlerFactoryFn builds the per-connection WebSocket http.Handler
// the dispatcher delegates to once sessionID + market have resolved.
// Production always uses marketdata.NewWSHandler; tests inject fakes to
// intercept delegation without going through gorilla's real upgrade.
type wsHandlerFactoryFn func(fanOut *marketdata.FanOut, cfg marketdata.WSConfig) http.Handler

// LivenessPath is the scaffold's only mounted route — a smoke probe
// that proves the mux handoff from bringup.Runtime.Mux to the conductor
// works. The /v1/_conductor/ prefix is deliberately underscored so no
// real conductor endpoint can accidentally shadow it in a later phase.
const LivenessPath = "/v1/_conductor/live"

// DefaultBringUpTimeout bounds the synchronous AgentOrchestrator.BringUp
// call made during canonical market bring-up. Picked as a round-number
// upper bound well above a healthy orchestrator's response time; the
// value is a dial on Config.BringUpTimeout.
const DefaultBringUpTimeout = 30 * time.Second

// DefaultJoinTimeout bounds the per-request context the canonical-join
// handler installs around JoinCanonical. One EnsureAccount round-trip
// (durable upsert) plus one StartSession round-trip (shard reply over a
// Postgres load + Redis snapshot seed) sits well inside five seconds on
// a healthy stack; a slower request surfaces as 504 to the client (which
// retries). Config.JoinTimeout overrides.
const DefaultJoinTimeout = 5 * time.Second

// DefaultAccountOpenTimeout bounds the per-request context the
// account-open handler installs around EnsureAccount. One durable
// upsert round-trip (open_account INSERT on uq_accounts_owner, falling
// back to AccountHandle on the UNIQUE conflict) is sub-second on a
// healthy Postgres; three seconds is tight enough that a stuck request
// surfaces to the client fast but loose enough to tolerate a spiky
// cluster. Config.AccountOpenTimeout overrides.
const DefaultAccountOpenTimeout = 3 * time.Second

// DefaultSetupTimeout bounds the per-request context the on-demand
// trigger handler installs around StartOnDemand. The held setup walks
// EnsureAccount → QualifyOnDemand → agents.BringUp → NewExchange →
// WireInstance (both axes) → Start → StartSession; the dominant cost
// is agents.BringUp (synchronous orchestrator round-trip) plus the
// StartSession shard reply over the account's durable load. Ninety
// seconds is loose enough that a healthy stack never trips it even
// with a slow orchestrator, tight enough that a wedged request
// surfaces to the client as a 503 within a browser's attention span.
// Config.SetupTimeout overrides.
const DefaultSetupTimeout = 90 * time.Second

// Config is the construction input for NewConductor. Required fields
// (CfgDir, OHLCVPool, Broker, Market) have no sensible default and
// error at construction; optional fields default to the logging stubs
// / time.Now / slog.Default.
//
// Broker and Market are injected directly (not via a *bringup.Runtime
// handle) so this package has no production coupling to the full
// Runtime shape — the production call site simply passes rt.Broker +
// rt.Market from the Runtime it already has. Tests inject fakes.
type Config struct {
	// CfgDir is the directory containing one <tag>.json per configured
	// market. Required.
	CfgDir string

	// OHLCVPool is the conductor's own read pool for the OHLCV candles
	// table — separate from the broker Store's pool. The Conductor
	// stores it and closes it on Stop. Opened by the caller against
	// the same database cluster the broker uses, with its own MaxConns
	// so chart-history reads at market-open don't compete with
	// session-load traffic on the broker's pool. Required.
	OHLCVPool *pgxpool.Pool

	// Broker and Market are the two shipped bindings — pass rt.Broker
	// and rt.Market from a bringup.Runtime in production. Required.
	Broker BrokerPort
	Market MarketDataPort

	// Optional dependencies — nil values fall back to safe defaults.
	Exchange  ExchangeFactory   // nil → defaultExchangeFactory wrapping wire.NewExchange
	Agents    AgentOrchestrator // nil → randomized-outcome stub (agent-orchestrator layer undesigned)
	Liquidity LiquidityOperator // nil → randomized-outcome stub (operator undesigned)
	Now       func() time.Time  // nil → time.Now
	Logger    *slog.Logger      // nil → slog.Default

	// WSHandlerFactory is a test seam that lets a fake http.Handler
	// intercept WebSocket delegation without routing through gorilla's
	// real upgrade. Nil → marketdata.NewWSHandler (the shipped WS layer).
	// Production never overrides.
	WSHandlerFactory wsHandlerFactoryFn

	// BringUpTimeout bounds the synchronous AgentOrchestrator.BringUp
	// call made during canonical market bring-up. Zero →
	// DefaultBringUpTimeout.
	BringUpTimeout time.Duration

	// JoinTimeout bounds the context the canonical-join HTTP handler
	// passes into JoinCanonical. Zero → DefaultJoinTimeout. A slow
	// StartSession round-trip surfaces to the client as HTTP 504.
	JoinTimeout time.Duration

	// AccountOpenTimeout bounds the context the account-open HTTP
	// handler passes into broker.EnsureAccount. Zero →
	// DefaultAccountOpenTimeout. A slow durable upsert surfaces to the
	// client as HTTP 504.
	AccountOpenTimeout time.Duration

	// SetupTimeout bounds the context the on-demand trigger HTTP
	// handler passes into StartOnDemand. The whole held-setup path —
	// EnsureAccount, QualifyOnDemand gate, agents.BringUp, exchange
	// construct + wire + Start, StartSession seal — runs under this
	// deadline. Zero → DefaultSetupTimeout. Expiry surfaces to the
	// client as HTTP 503 {status:"failed", reason:"..."}.
	SetupTimeout time.Duration

	// BadConfigRetryDelay is how long each per-market timer sleeps
	// between retries when its config fails to load or compute a next
	// fire time. Zero → DefaultBadConfigRetryDelay (one hour) — long
	// so a persistent config error never spins the loop. Tests set a
	// small value.
	BadConfigRetryDelay time.Duration
}

// Conductor is this package's top-level object — holds the registry,
// the injected ports, config state, and the timer-lifecycle fields
// reserved for the canonical bring-up phase's per-market timers. Zero
// value is NOT usable; construct via NewConductor.
type Conductor struct {
	reg                 *Registry
	cfgDir              string
	ohlcv               *pgxpool.Pool
	referenceReader     candleReader // defaults to ohlcv; test seam for in-package tests only
	wsCfg               marketdata.WSConfig
	broker              BrokerPort
	market              MarketDataPort
	exch                ExchangeFactory
	agents              AgentOrchestrator
	liq                 LiquidityOperator
	now                 func() time.Time
	log                 *slog.Logger
	bringUpTimeout      time.Duration
	joinTimeout         time.Duration
	accountOpenTimeout  time.Duration
	setupTimeout        time.Duration
	badConfigRetryDelay time.Duration
	wsHandlerFactory    wsHandlerFactoryFn
	stopOnce            sync.Once

	// Timer-lifecycle fields reserved for the canonical bring-up phase
	// — one per-market timer goroutine fires market-open minus the
	// pre-open lead. Fields exist now; the goroutines land in that
	// next phase.
	timerQuit chan struct{}
	wg        sync.WaitGroup
}

// NewConductor validates Config, resolves defaults, and returns a ready
// Conductor. Does not spawn any goroutines (that is Start's job).
func NewConductor(cfg Config) (*Conductor, error) {
	if cfg.CfgDir == "" {
		return nil, fmt.Errorf("conductor: NewConductor requires CfgDir")
	}
	if cfg.OHLCVPool == nil {
		return nil, fmt.Errorf("conductor: NewConductor requires OHLCVPool (the conductor's own candles read pool)")
	}
	if cfg.Broker == nil {
		return nil, fmt.Errorf("conductor: NewConductor requires Broker (pass rt.Broker)")
	}
	if cfg.Market == nil {
		return nil, fmt.Errorf("conductor: NewConductor requires Market (pass rt.Market)")
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	exch := cfg.Exchange
	if exch == nil {
		exch = defaultExchangeFactory{}
	}
	agents := cfg.Agents
	if agents == nil {
		// Seed 0 → the constructor substitutes time.Now().UnixNano().
		agents = newStubAgentOrchestrator(log, 0)
	}
	liq := cfg.Liquidity
	if liq == nil {
		liq = newStubLiquidityOperator(log, 0)
	}
	bringUpTimeout := cfg.BringUpTimeout
	if bringUpTimeout == 0 {
		bringUpTimeout = DefaultBringUpTimeout
	}
	joinTimeout := cfg.JoinTimeout
	if joinTimeout == 0 {
		joinTimeout = DefaultJoinTimeout
	}
	accountOpenTimeout := cfg.AccountOpenTimeout
	if accountOpenTimeout == 0 {
		accountOpenTimeout = DefaultAccountOpenTimeout
	}
	setupTimeout := cfg.SetupTimeout
	if setupTimeout == 0 {
		setupTimeout = DefaultSetupTimeout
	}
	wsHandlerFactory := cfg.WSHandlerFactory
	if wsHandlerFactory == nil {
		wsHandlerFactory = marketdata.NewWSHandler
	}
	badConfigRetryDelay := cfg.BadConfigRetryDelay
	if badConfigRetryDelay == 0 {
		badConfigRetryDelay = DefaultBadConfigRetryDelay
	}

	conductor := &Conductor{
		reg:                 newRegistry(),
		cfgDir:              cfg.CfgDir,
		ohlcv:               cfg.OHLCVPool,
		referenceReader:     cfg.OHLCVPool, // *pgxpool.Pool satisfies candleReader
		wsCfg:               cfg.Market.WSConfig(),
		broker:              cfg.Broker,
		market:              cfg.Market,
		exch:                exch,
		agents:              agents,
		liq:                 liq,
		now:                 now,
		log:                 log,
		bringUpTimeout:      bringUpTimeout,
		joinTimeout:         joinTimeout,
		accountOpenTimeout:  accountOpenTimeout,
		setupTimeout:        setupTimeout,
		badConfigRetryDelay: badConfigRetryDelay,
		wsHandlerFactory:    wsHandlerFactory,
	}
	log.Info("conductor: constructed",
		"cfg_dir", cfg.CfgDir,
		"ws_conn_buffer", conductor.wsCfg.ConnBuffer,
		"bringup_timeout", conductor.bringUpTimeout,
		"join_timeout", conductor.joinTimeout,
		"account_open_timeout", conductor.accountOpenTimeout,
		"setup_timeout", conductor.setupTimeout,
		"bad_config_retry_delay", conductor.badConfigRetryDelay,
	)
	return conductor, nil
}

// Start is the lifecycle entry. It allocates timerQuit, lists every
// configured market tag under cfgDir, and spawns one per-market timer
// goroutine for each. Returns an error only if the cfgDir listing
// itself fails — a wholly empty cfgDir is legal (zero markets, zero
// timers; a valid configuration for a boot-and-wait process).
//
// Each timer goroutine arms itself to the market's next weekday
// pre-open and fires spinUpCanonical inline. Timers never share state;
// the Registry mutex serializes any registry mutation.
func (conductor *Conductor) Start() error {
	if conductor.timerQuit == nil {
		conductor.timerQuit = make(chan struct{})
	}
	tags, listErr := listConfiguredMarkets(conductor.cfgDir)
	if listErr != nil {
		return fmt.Errorf("conductor: Start: list configured markets: %w", listErr)
	}
	conductor.log.Info("conductor: Start spawning per-market timers",
		"cfg_dir", conductor.cfgDir,
		"tag_count", len(tags),
	)
	for _, marketTag := range tags {
		spawnTag := marketTag
		conductor.wg.Add(1)
		go conductor.runCanonicalTimer(spawnTag)
	}
	return nil
}

// Stop closes the timer-quit signal, waits for every spawned timer to
// exit (and for any spin-up already in flight inside a timer to
// complete), then closes the ohlcv pool. Idempotent via stopOnce — a
// second call is a no-op.
//
// The quit channel is closed, NOT nilled, so a timer that re-selects
// on it after returning from spin-up still receives the shutdown
// signal rather than blocking on a nil channel.
func (conductor *Conductor) Stop() {
	conductor.stopOnce.Do(func() {
		if conductor.timerQuit != nil {
			// Guard against a caller (typically a test) that already
			// closed timerQuit directly — close-of-closed panics.
			// The select/default distinguishes "ready" (closed) from
			// "blocking" (still open) on a signal-only channel.
			select {
			case <-conductor.timerQuit:
				// Already closed; nothing to do.
			default:
				close(conductor.timerQuit)
			}
		}
		conductor.wg.Wait()
		if conductor.ohlcv != nil {
			conductor.ohlcv.Close()
		}
		conductor.log.Info("conductor: Stop complete")
	})
}

// MountRoutes registers the conductor's HTTP routes on the shared mux
// (production callers pass bringup.Runtime.Mux). The scaffold mounts
// only LivenessPath — a 200/ok probe that proves the handoff. Real
// endpoints (session create / join / on-demand / WebSocket / markets /
// instruments / account open) land in later phases.
//
// Panics on a nil mux — a caller-side programming error per root
// CLAUDE.md invariant discipline.
func (conductor *Conductor) MountRoutes(mux *http.ServeMux) {
	if mux == nil {
		panic("conductor: MountRoutes requires non-nil *http.ServeMux")
	}
	mux.HandleFunc(LivenessPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc(PathSessionCanonical, conductor.handleJoinCanonical)
	mux.HandleFunc(PathSessionOnDemand, conductor.handleStartOnDemand)
	mux.HandleFunc(marketdata.PathWS, conductor.handleWS)
	mux.HandleFunc(PathMarkets, conductor.handleMarkets)
	mux.HandleFunc(PathInstruments, conductor.handleInstruments)
	mux.HandleFunc(PathAccountOpen, conductor.handleAccountOpen)
	conductor.log.Info("conductor: routes mounted",
		"liveness", LivenessPath,
		"join_canonical", PathSessionCanonical,
		"start_ondemand", PathSessionOnDemand,
		"ws", marketdata.PathWS,
		"markets", PathMarkets,
		"instruments", PathInstruments,
		"account_open", PathAccountOpen,
	)
}
