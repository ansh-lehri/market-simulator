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
