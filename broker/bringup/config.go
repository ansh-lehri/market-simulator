package bringup

import (
	"errors"
	"fmt"
	"time"

	"simulator/broker/funnel"
)

// defaultShardMaxConns is the per-shard bundle pool size when the
// caller leaves BrokerConfig.ShardMaxConns at zero. 2 is the
// minimum per P1-D2 for normal deployments; Supabase session-mode
// deployments set ShardMaxConns explicitly to 1.
const defaultShardMaxConns int32 = 2

// defaultStoreMaxConns is the Store pool's conservative size when the
// caller leaves BrokerConfig.StoreMaxConns at zero. The Store serves
// the five durable reads + the durable transfer — a few concurrent
// reads at a time; 4 lets the pool breathe without eating Postgres
// connection budget that the per-shard bundles need.
const defaultStoreMaxConns int32 = 4

// defaultReadTimeout is the per-request deadline every read HTTP
// handler uses when the caller leaves BrokerConfig.ReadTimeout at
// zero. The value is a reasonable dial for tests and demos, not a
// production number — a real deploy sets ReadTimeout explicitly.
const defaultReadTimeout = 2 * time.Second

// defaultOrderTimeout is the per-request deadline every order HTTP
// handler (place / cancel / modify) uses when the caller leaves
// BrokerConfig.OrderTimeout at zero. Same disposition as
// defaultReadTimeout: tests + demos dial, real deploys set it.
const defaultOrderTimeout = 2 * time.Second

// defaultTransferTimeout is the per-request deadline the bank-transfer
// HTTP handler uses when the caller leaves BrokerConfig.TransferTimeout
// at zero. The transfer handler owns the deadline (BROKER_PHASE3 P3-D4);
// one ctx bounds both the live shard round-trip AND, on a miss, the
// durable UPDATE round-trip. Tests + demos dial; real deploys set it.
const defaultTransferTimeout = 2 * time.Second

// defaultSeedTimeout is the per-request deadline the seed-inventory
// HTTP handler uses when the caller leaves BrokerConfig.SeedTimeout at
// zero (BROKER_SEED_INVENTORY §5). Tests + demos dial; real deploys set
// it.
const defaultSeedTimeout = 2 * time.Second

// Phase-7 (B10 tape relay + market-data component) tunables. Dials
// live here so bringup validates once; marketdata.NewMarketData reads
// marketdata.Config{ReadTimeout} + marketdata.WSConfig{...} composed
// from these. Stage-5 default values; a real deploy sets them.
const (
	defaultCandleReadTimeout     = 2 * time.Second
	defaultWSConnBuffer          = 256
	defaultWSLagEvictionThresh   = 32
	defaultWSPingInterval        = 30 * time.Second
	defaultWSReadTimeout         = 60 * time.Second
	defaultWSWriteTimeout        = 10 * time.Second
)

// BrokerConfig is the broker's own dials — distinct from
// connectivity.Config (which carries infra DSNs + pool sizing for the
// shared Postgres / Redis). BrokerConfig is composed by the caller; the
// infra config is loaded from env once by BringUp itself.
type BrokerConfig struct {
	// BrokerID identifies this broker instance; stamped on outbound
	// punches at Tier 1. Required.
	BrokerID funnel.BrokerID

	// ShardCount is N — the funnel width (hash(AccountID) % N).
	// Required; must be >= 1.
	ShardCount int

	// InboxDepth is the buffered depth of each shard's inbox.
	// Required; must be >= 1 (zero would make routing synchronous).
	InboxDepth int

	// Country selects the per-shard cost module and persistence adapter
	// via buildCountryStack (country.go). Required; must be in the
	// allowlist. "IN" is wired for Phase 1.
	Country string

	// ShardMaxConns caps each per-shard bundle pool's connection count.
	// Zero → defaultShardMaxConns (2). There are ShardCount pools, so
	// total shard connections = ShardCount × ShardMaxConns. On
	// Supabase session-mode (pool_size=15) set this to 1 to share the
	// budget across all pools; normal deployments use the default (2).
	ShardMaxConns int32

	// StoreMaxConns caps the Store pool's connection count. Zero →
	// defaultStoreMaxConns (4). The Store pool is separate from each
	// shard's bundle pool so a flood of read traffic can't starve the
	// shard owners (P1-D2 locked a shard pool at MaxConns >= 2; the
	// Store pool is independently sized here).
	StoreMaxConns int32

	// StartingCapital is stamped as every account's initial trading_cash
	// at EnsureAccount's lifecycle.OpenAccount call (BROKER_PHASE2 §1 /
	// P2-1·D4). Must be >= 0 — trading_cash is signed for later debt, but
	// at birth it's always non-negative (lifecycle enforces the same
	// floor; validate catches it here first so the error carries bringup's
	// context). The value is an open dial per the Phase-2 doc; zero is
	// legal and means "accounts start with no trading cash."
	StartingCapital int64

	// StartingBank is stamped as every account's initial bank_cash at
	// EnsureAccount's lifecycle.OpenAccount call (BROKER_PHASE2 §1 /
	// P2-1·D4). Must be >= 0 — bank_cash carries a DB CHECK (ck_bank_nonneg,
	// migration 0001) that would reject a negative value at insert time;
	// validate catches it earlier so the error is a config error, not a
	// driver violation. Zero is legal.
	StartingBank int64

	// OnDemandMinimum is the threshold QualifyOnDemand compares the
	// account's signed trading_cash against (BROKER_PHASE2 §2 / P2-2·D3).
	// L7 never sees this number — the gate returns only a boolean. Must
	// be >= 0; zero means "no bar" (every account qualifies). The actual
	// value is an open dial per the Phase-2 doc.
	OnDemandMinimum int64

	// ReadTimeout bounds every read HTTP handler's broker + store
	// round-trips. Zero → defaultReadTimeout (bringup is caller-
	// friendly — a test that doesn't care about timeouts gets a
	// workable default). Negative → ErrBadConfig.
	ReadTimeout time.Duration

	// OrderTimeout bounds every order HTTP handler's broker round-trip
	// (place / cancel) and the two-leg compose (modify). Zero →
	// defaultOrderTimeout, same caller-friendly convention as
	// ReadTimeout. Negative → ErrBadConfig.
	OrderTimeout time.Duration

	// TransferTimeout bounds the bank-transfer HTTP handler's broker
	// round-trip AND the durable fallback (one ctx threads both). Zero
	// → defaultTransferTimeout, same caller-friendly convention as
	// ReadTimeout / OrderTimeout. Negative → ErrBadConfig.
	TransferTimeout time.Duration

	// SeedTimeout bounds the seed-inventory HTTP handler's broker round-
	// trip (BROKER_SEED_INVENTORY §5). Zero → defaultSeedTimeout, same
	// caller-friendly convention as ReadTimeout / OrderTimeout /
	// TransferTimeout. Negative → ErrBadConfig.
	SeedTimeout time.Duration

	// --- Phase-7 (B10 market-data) tunables ---------------------------
	// All six fall back to default* consts on zero; validate rejects
	// negatives. Threaded into marketdata.NewMarketData at bringUp as
	// marketdata.Config{ReadTimeout: CandleReadTimeout} and
	// marketdata.WSConfig{the other five}.

	// CandleReadTimeout bounds the GET /v1/candles handler per-request
	// deadline. Zero → defaultCandleReadTimeout. Negative → ErrBadConfig.
	CandleReadTimeout time.Duration

	// WSConnBuffer is the per-WS-connection outbound-frame buffer size.
	// Zero → defaultWSConnBuffer. Negative → ErrBadConfig.
	WSConnBuffer int

	// WSLagEvictionThreshold is the consecutive-drop count that triggers
	// slow-client eviction on a WS conn. Zero → defaultWSLagEvictionThresh.
	// Negative → ErrBadConfig.
	WSLagEvictionThreshold int

	// WSPingInterval is how often the WS writeLoop sends a gorilla
	// control-frame ping. Zero → defaultWSPingInterval. Negative → ErrBadConfig.
	WSPingInterval time.Duration

	// WSReadTimeout is the WS socket read deadline (reset on each pong).
	// Zero → defaultWSReadTimeout. Negative → ErrBadConfig.
	WSReadTimeout time.Duration

	// WSWriteTimeout is the WS socket write deadline (per push + ping).
	// Zero → defaultWSWriteTimeout. Negative → ErrBadConfig.
	WSWriteTimeout time.Duration
}

// ErrBadConfig wraps BrokerConfig validation failures. Category sentinel
// — callers dispatch via errors.Is, not errors.As on a struct.
var ErrBadConfig = errors.New("bringup: bad BrokerConfig")

// validate checks the required fields. A zero-value or clearly-wrong
// BrokerConfig is a programming error at the caller — surface it
// immediately, before any pool is opened.
func (brokerCfg BrokerConfig) validate() error {
	if brokerCfg.BrokerID == "" {
		return fmt.Errorf("BrokerID empty: %w", ErrBadConfig)
	}
	if brokerCfg.ShardCount < 1 {
		return fmt.Errorf("ShardCount=%d (must be >= 1): %w", brokerCfg.ShardCount, ErrBadConfig)
	}
	if brokerCfg.InboxDepth < 1 {
		return fmt.Errorf("InboxDepth=%d (must be >= 1): %w", brokerCfg.InboxDepth, ErrBadConfig)
	}
	if brokerCfg.Country == "" {
		return fmt.Errorf("Country empty: %w", ErrBadConfig)
	}
	if brokerCfg.StartingCapital < 0 {
		return fmt.Errorf("StartingCapital=%d (must be >= 0): %w", brokerCfg.StartingCapital, ErrBadConfig)
	}
	if brokerCfg.StartingBank < 0 {
		return fmt.Errorf("StartingBank=%d (must be >= 0): %w", brokerCfg.StartingBank, ErrBadConfig)
	}
	if brokerCfg.OnDemandMinimum < 0 {
		return fmt.Errorf("OnDemandMinimum=%d (must be >= 0): %w", brokerCfg.OnDemandMinimum, ErrBadConfig)
	}
	if brokerCfg.ReadTimeout < 0 {
		return fmt.Errorf("ReadTimeout=%v (must be >= 0; zero means default): %w", brokerCfg.ReadTimeout, ErrBadConfig)
	}
	if brokerCfg.OrderTimeout < 0 {
		return fmt.Errorf("OrderTimeout=%v (must be >= 0; zero means default): %w", brokerCfg.OrderTimeout, ErrBadConfig)
	}
	if brokerCfg.TransferTimeout < 0 {
		return fmt.Errorf("TransferTimeout=%v (must be >= 0; zero means default): %w", brokerCfg.TransferTimeout, ErrBadConfig)
	}
	if brokerCfg.SeedTimeout < 0 {
		return fmt.Errorf("SeedTimeout=%v (must be >= 0; zero means default): %w", brokerCfg.SeedTimeout, ErrBadConfig)
	}
	if brokerCfg.CandleReadTimeout < 0 {
		return fmt.Errorf("CandleReadTimeout=%v (must be >= 0): %w", brokerCfg.CandleReadTimeout, ErrBadConfig)
	}
	if brokerCfg.WSConnBuffer < 0 {
		return fmt.Errorf("WSConnBuffer=%d (must be >= 0): %w", brokerCfg.WSConnBuffer, ErrBadConfig)
	}
	if brokerCfg.WSLagEvictionThreshold < 0 {
		return fmt.Errorf("WSLagEvictionThreshold=%d (must be >= 0): %w", brokerCfg.WSLagEvictionThreshold, ErrBadConfig)
	}
	if brokerCfg.WSPingInterval < 0 {
		return fmt.Errorf("WSPingInterval=%v (must be >= 0): %w", brokerCfg.WSPingInterval, ErrBadConfig)
	}
	if brokerCfg.WSReadTimeout < 0 {
		return fmt.Errorf("WSReadTimeout=%v (must be >= 0): %w", brokerCfg.WSReadTimeout, ErrBadConfig)
	}
	if brokerCfg.WSWriteTimeout < 0 {
		return fmt.Errorf("WSWriteTimeout=%v (must be >= 0): %w", brokerCfg.WSWriteTimeout, ErrBadConfig)
	}
	return nil
}

// shardMaxConnsOrDefault returns the configured ShardMaxConns, falling
// back to defaultShardMaxConns on zero. Negative is coerced to the
// default; a signed int32 slipping to negative would otherwise blow up
// pgxpool.
func (brokerCfg BrokerConfig) shardMaxConnsOrDefault() int32 {
	if brokerCfg.ShardMaxConns <= 0 {
		return defaultShardMaxConns
	}
	return brokerCfg.ShardMaxConns
}

// storeMaxConnsOrDefault returns the configured StoreMaxConns, falling
// back to defaultStoreMaxConns on zero. Negative is coerced to the
// default; a signed int32 slipping to negative would otherwise blow up
// pgxpool.
func (brokerCfg BrokerConfig) storeMaxConnsOrDefault() int32 {
	if brokerCfg.StoreMaxConns <= 0 {
		return defaultStoreMaxConns
	}
	return brokerCfg.StoreMaxConns
}

// readTimeoutOrDefault returns the configured ReadTimeout, falling
// back to defaultReadTimeout on zero. validate() already rejects
// negatives, so this helper only handles the "zero → default" branch
// — the Config.Validate inside each handler constructor requires a
// strictly-positive value, so we hand it a non-zero number here.
func (brokerCfg BrokerConfig) readTimeoutOrDefault() time.Duration {
	if brokerCfg.ReadTimeout == 0 {
		return defaultReadTimeout
	}
	return brokerCfg.ReadTimeout
}

// orderTimeoutOrDefault returns the configured OrderTimeout, falling
// back to defaultOrderTimeout on zero. validate() already rejects
// negatives, so this helper only handles the "zero → default" branch —
// order.Config.Validate inside each handler constructor requires a
// strictly-positive value, so we hand it a non-zero number here.
func (brokerCfg BrokerConfig) orderTimeoutOrDefault() time.Duration {
	if brokerCfg.OrderTimeout == 0 {
		return defaultOrderTimeout
	}
	return brokerCfg.OrderTimeout
}

// transferTimeoutOrDefault returns the configured TransferTimeout,
// falling back to defaultTransferTimeout on zero. validate() rejects
// negatives; transfer.Config.Validate inside NewHandler requires
// strictly-positive, so we hand it a non-zero number here.
func (brokerCfg BrokerConfig) transferTimeoutOrDefault() time.Duration {
	if brokerCfg.TransferTimeout == 0 {
		return defaultTransferTimeout
	}
	return brokerCfg.TransferTimeout
}

// seedTimeoutOrDefault returns the configured SeedTimeout, falling back
// to defaultSeedTimeout on zero. validate() rejects negatives;
// seedinventory.Config.Validate inside NewHandler requires strictly-
// positive, so we hand it a non-zero number here.
func (brokerCfg BrokerConfig) seedTimeoutOrDefault() time.Duration {
	if brokerCfg.SeedTimeout == 0 {
		return defaultSeedTimeout
	}
	return brokerCfg.SeedTimeout
}

// --- Phase-7 marketdata zero-to-default accessors ---------------------
// Each handles the "zero means default" branch (negatives were already
// rejected by validate). Downstream Config/WSConfig validation requires
// strictly-positive, so these ensure a non-zero handoff.

func (brokerCfg BrokerConfig) candleReadTimeoutOrDefault() time.Duration {
	if brokerCfg.CandleReadTimeout == 0 {
		return defaultCandleReadTimeout
	}
	return brokerCfg.CandleReadTimeout
}

func (brokerCfg BrokerConfig) wsConnBufferOrDefault() int {
	if brokerCfg.WSConnBuffer == 0 {
		return defaultWSConnBuffer
	}
	return brokerCfg.WSConnBuffer
}

func (brokerCfg BrokerConfig) wsLagEvictionThresholdOrDefault() int {
	if brokerCfg.WSLagEvictionThreshold == 0 {
		return defaultWSLagEvictionThresh
	}
	return brokerCfg.WSLagEvictionThreshold
}

func (brokerCfg BrokerConfig) wsPingIntervalOrDefault() time.Duration {
	if brokerCfg.WSPingInterval == 0 {
		return defaultWSPingInterval
	}
	return brokerCfg.WSPingInterval
}

func (brokerCfg BrokerConfig) wsReadTimeoutOrDefault() time.Duration {
	if brokerCfg.WSReadTimeout == 0 {
		return defaultWSReadTimeout
	}
	return brokerCfg.WSReadTimeout
}

func (brokerCfg BrokerConfig) wsWriteTimeoutOrDefault() time.Duration {
	if brokerCfg.WSWriteTimeout == 0 {
		return defaultWSWriteTimeout
	}
	return brokerCfg.WSWriteTimeout
}
