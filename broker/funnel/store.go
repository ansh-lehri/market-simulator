package funnel

import "simulator/broker/store"

// Store is the broker-owned durable layer — a single, shard-independent
// component with its own Postgres pool, hosting every off-shard durable
// operation (BROKER_PHASE1 §2 / §5). It is defined in broker/store; this
// alias exists so funnel.NewBroker keeps a stable signature
// (NewBroker(..., store *Store)) regardless of where the concrete type
// lives, and so the funnel package never needs to import broker/store
// directly outside this one file.
//
// Phase 0 kept this as a stub struct{}; Phase 1 Stage A promotes it to
// the real type via this alias. Callers continue to pass either a nil
// *Store (funnel-internal tests that never exercise durable ops) or a
// real *Store built by the wiring root (Stage D).
type Store = store.Store
