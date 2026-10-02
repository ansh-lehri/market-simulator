// Package bringup is the broker-owned wiring root (BROKER_PHASE1 §1).
//
// It loads connectivity config once (DATABASE_URL / REDIS_URL via
// connectivity/config.Load — the only env read in the system), wires N
// per-shard capstone bundles (each bundle = one pool, one redis client,
// one persistence Component with its own drainer, one country cost
// module + adapter), opens the broker-owned Store pool (its OWN pool,
// sized small), builds the funnel broker over the bundles + the Store,
// and spawns the N owner goroutines.
//
// Shutdown is strict LIFO (BROKER_PHASE1 §1.5):
//
//  1. Broker.Stop  — closes each shard inbox; shardWG.Wait blocks until
//     every owner goroutine has exited. This is what makes it safe to
//     tear down the bundles next — a shard's final MarkSnapshotDirty is
//     guaranteed landed before its bundle's handles close.
//  2. bundle.Close (LIFO over the slice) — each bundle stops its drainer
//     then closes Redis then closes Postgres.
//  3. Store pool close — the Store's own pool, closed LAST.
//
// Country binding (P1-D7) lives in country.go — the one place a
// BrokerConfig.Country selects a cost.CostModule + persistence
// adapter. "IN" is the only wired country for Phase 1; "US" is in the
// allowlist and returns ErrCountryUnavailable (same "slot defined, body
// empty" posture as BROKER_READ_SURFACES §8.6's A16 handler for US).
package bringup
