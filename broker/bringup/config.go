package bringup

import (
	"errors"
	"fmt"

	"simulator/broker/funnel"
)

// defaultStoreMaxConns is the Store pool's conservative size when the
// caller leaves BrokerConfig.StoreMaxConns at zero. The Store serves
// the five Phase-3+ durable reads + the durable transfer — a few
// concurrent reads at a time; 4 lets the pool breathe without eating
// Postgres connection budget that the per-shard bundles need.
const defaultStoreMaxConns int32 = 4

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

	// StoreMaxConns caps the Store pool's connection count. Zero →
	// defaultStoreMaxConns (4). The Store pool is separate from each
	// shard's bundle pool so a flood of read traffic can't starve the
	// shard owners (P1-D2 locked a shard pool at MaxConns >= 2; the
	// Store pool is independently sized here).
	StoreMaxConns int32
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
	return nil
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
