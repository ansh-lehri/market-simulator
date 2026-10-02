package funnel_test

// Integration-test harness for the funnel's Phase-2 off-shard surface
// (EnsureAccount, QualifyOnDemand, StartSession). These tests sit in the
// _test package so they can import bringup — bringup imports funnel for
// NewBroker, so an in-package funnel test cannot import bringup without a
// cycle. The _test package pattern is Go's standard escape hatch for
// exactly this: tests exercise the public API, with no reach into
// funnel's unexported internals.
//
// Why bringup and not a local infra wiring helper: bringup already owns
// the one correct wiring (pool + redis + unstarted persistence.Component
// + lifecycle.Service + MoneyConfig threaded into NewBroker). Duplicating
// that into a funnel-local helper would mean two wirings to keep in sync,
// and a Phase-2 regression against our chosen Store-close ordering would
// go undetected on the duplicated path. One wiring, one call site, one
// place for a shutdown-ordering bug to show up.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"simulator/broker/bringup"
	"simulator/connectivity/config"
)

const (
	envTestDBURL    = "TEST_DATABASE_URL"
	envTestRedisURL = "TEST_REDIS_URL"
)

// requireRuntime brings up a one-shard broker against TEST_DATABASE_URL /
// TEST_REDIS_URL and schedules its Shutdown on test end. Skips cleanly
// when either env is unset — individual tests do not need to re-check.
//
// money is passed in by the test so each case can tune its own dials
// (EnsureAccount tests leave them zero; QualifyOnDemand tests set the
// floor; StartSession tests inherit either shape). Everything else is
// a fixed one-shard, inbox-4, country-IN setup — minimal surface for
// funnel-level assertions.
func requireRuntime(t *testing.T, cfg bringup.BrokerConfig) *bringup.Runtime {
	t.Helper()
	dbDSN := os.Getenv(envTestDBURL)
	redisURL := os.Getenv(envTestRedisURL)
	if dbDSN == "" || redisURL == "" {
		t.Skipf("%s / %s not set — integration test skipped", envTestDBURL, envTestRedisURL)
	}
	infra := config.Config{
		Postgres: config.PostgresConfig{
			DSN:         dbDSN,
			PingTimeout: 5 * time.Second,
			MaxConns:    4,
		},
		Redis: config.RedisConfig{
			URL:         redisURL,
			PingTimeout: 5 * time.Second,
		},
	}
	runtime, err := bringup.BringUpForTest(context.Background(), cfg, infra)
	if err != nil {
		t.Fatalf("bringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)
	return runtime
}

// defaultTestBrokerConfig is the base BrokerConfig the Phase-2 integration
// tests tune. Country is IN (the only wired stack); shard count 1 keeps
// routing reasoning trivial; money dials default to zero and are
// overridden per test.
func defaultTestBrokerConfig() bringup.BrokerConfig {
	return bringup.BrokerConfig{
		BrokerID:   "B-funnel-integ",
		ShardCount: 1,
		InboxDepth: 4,
		Country:    "IN",
	}
}

// cleanupOwnerRows deletes any accounts rows (and their FK-children) whose
// owner_id matches the given prefix. Scheduled on test end. Mirrors the
// pattern in broker/store integration tests, inline here so this file
// stays self-contained.
func cleanupOwnerRows(t *testing.T, runtime *bringup.Runtime, ownerPrefix string) {
	t.Helper()
	t.Cleanup(func() {
		pool := runtime.Store.Pool()
		if pool == nil {
			return // Shutdown ran first; pool is already closed/nil'd.
		}
		ctx := context.Background()
		likePattern := ownerPrefix + "%"
		if _, err := pool.Exec(ctx,
			`DELETE FROM trade_costs
			 WHERE trade_id IN (
			     SELECT pt.trade_id FROM participant_trades pt
			     JOIN accounts acc ON acc.account_id = pt.account_id
			     WHERE acc.owner_id LIKE $1
			 )`, likePattern); err != nil {
			t.Logf("cleanup trade_costs: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`DELETE FROM participant_trades
			 WHERE account_id IN (
			     SELECT account_id FROM accounts WHERE owner_id LIKE $1
			 )`, likePattern); err != nil {
			t.Logf("cleanup participant_trades: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`DELETE FROM accounts WHERE owner_id LIKE $1`, likePattern); err != nil {
			t.Logf("cleanup accounts: %v", err)
		}
	})
}

// uniqueOwnerPrefix returns a per-test prefix with the test name and a
// nanosecond stamp, so parallel runs do not collide.
func uniqueOwnerPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("fnltest-%s-%d", t.Name(), time.Now().UnixNano())
}
