package conductor

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/broker/funnel"
	"simulator/broker/marketdata"
	"simulator/contract"
	"simulator/economy/lifecycle"
	"simulator/exchange/emit"
)

// --- Test port implementations (interface fakes, no I/O) --------------

type stubBrokerPort struct{}

func (stubBrokerPort) EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error) {
	return funnel.AccountID("test-account"), nil
}

func (stubBrokerPort) StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error {
	return nil
}

func (stubBrokerPort) QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error) {
	return true, nil
}

func (stubBrokerPort) WireInstance(exchangeID string, privateStream <-chan contract.EngineResult) {
	// No-op: scaffold tests do not spawn settlement consumers.
}

type stubMarketDataPort struct{}

func (stubMarketDataPort) WireInstance(exchangeID marketdata.ExchangeID, tape <-chan emit.TapeEvent, market string, mode marketdata.Mode) *marketdata.FanOut {
	return nil
}

func (stubMarketDataPort) WSConfig() marketdata.WSConfig {
	return marketdata.WSConfig{
		ConnBuffer:           256,
		LagEvictionThreshold: 5,
		PingInterval:         30 * time.Second,
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         10 * time.Second,
	}
}

// Compile-time: the test fakes satisfy the port interfaces. Catches
// signature drift inside the test file too, not just at the shipped-
// broker assertion in ports.go.
var (
	_ BrokerPort     = stubBrokerPort{}
	_ MarketDataPort = stubMarketDataPort{}
)

// newUnconnectedPool returns a *pgxpool.Pool with a valid-shape DSN
// that will NEVER connect (port 1 is unreachable). pgxpool.New in v5
// is lazy — it parses the DSN and returns a pool handle without
// opening a connection. The scaffold code never acquires a connection
// from the pool, so this is safe.
func newUnconnectedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://none:none@127.0.0.1:1/none?pool_max_conns=1")
	if err != nil {
		t.Skipf("pgxpool.New rejected the stub DSN (platform-dependent): %v", err)
	}
	return pool
}

func newTestConfig(t *testing.T, pool *pgxpool.Pool) Config {
	t.Helper()
	return Config{
		CfgDir:    t.TempDir(),
		OHLCVPool: pool,
		Broker:    stubBrokerPort{},
		Market:    stubMarketDataPort{},
	}
}

func TestNewConductor_BuildsWithDefaults(t *testing.T) {
	pool := newUnconnectedPool(t)
	defer pool.Close()

	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	if cd == nil {
		t.Fatal("NewConductor returned nil")
	}
	if cd.reg == nil {
		t.Error("reg not initialized")
	}
	if cd.exch == nil {
		t.Error("ExchangeFactory not defaulted")
	}
	if cd.agents == nil {
		t.Error("AgentOrchestrator stub not defaulted")
	}
	if cd.liq == nil {
		t.Error("LiquidityOperator stub not defaulted")
	}
	if cd.now == nil {
		t.Error("now not defaulted")
	}
	if cd.log == nil {
		t.Error("log not defaulted")
	}
	if cd.wsCfg.ConnBuffer != 256 {
		t.Errorf("wsCfg not pulled from MarketDataPort: ConnBuffer=%d", cd.wsCfg.ConnBuffer)
	}
}

func TestNewConductor_UsesInjectedOverrides(t *testing.T) {
	pool := newUnconnectedPool(t)
	defer pool.Close()

	cfg := newTestConfig(t, pool)
	fixedTime := time.Date(2026, 10, 12, 9, 15, 0, 0, time.UTC)
	cfg.Now = func() time.Time { return fixedTime }
	cfg.Exchange = defaultExchangeFactory{} // explicit; the type-check matters more than the value
	customAgents := stubAgentOrchestrator{log: nil}
	cfg.Agents = customAgents
	customLiq := stubLiquidityOperator{log: nil}
	cfg.Liquidity = customLiq

	cd, err := NewConductor(cfg)
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	if cd.now() != fixedTime {
		t.Error("injected Now not used")
	}
	if _, ok := cd.agents.(stubAgentOrchestrator); !ok {
		t.Error("injected Agents not retained")
	}
	if _, ok := cd.liq.(stubLiquidityOperator); !ok {
		t.Error("injected Liquidity not retained")
	}
}

func TestNewConductor_RejectsMissingRequiredFields(t *testing.T) {
	pool := newUnconnectedPool(t)
	defer pool.Close()
	dir := t.TempDir()

	cases := []struct {
		name string
		cfg  Config
	}{
		{"no CfgDir", Config{OHLCVPool: pool, Broker: stubBrokerPort{}, Market: stubMarketDataPort{}}},
		{"no OHLCVPool", Config{CfgDir: dir, Broker: stubBrokerPort{}, Market: stubMarketDataPort{}}},
		{"no Broker", Config{CfgDir: dir, OHLCVPool: pool, Market: stubMarketDataPort{}}},
		{"no Market", Config{CfgDir: dir, OHLCVPool: pool, Broker: stubBrokerPort{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConductor(tc.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestConductor_StartStop(t *testing.T) {
	pool := newUnconnectedPool(t)
	// Not deferring pool.Close — Conductor.Stop closes it.

	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	cd.Start()
	if cd.timerQuit == nil {
		t.Error("Start did not allocate timerQuit")
	}
	cd.Stop()
	if cd.timerQuit != nil {
		t.Error("Stop did not clear timerQuit")
	}
	// Second Stop is a no-op.
	cd.Stop()
}
