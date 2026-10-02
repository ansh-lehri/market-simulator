package bringup

import "log/slog"

// Shutdown tears the broker down in strict LIFO order (BROKER_PHASE1 §1.5
// + Phase-2 Store-close unification):
//
//  1. Broker.Stop — closes each shard inbox; shardWG.Wait blocks until
//     every owner goroutine has exited. **Owners must exit before any
//     bundle closes**: a shard's final MarkSnapshotDirty / WriteSnapshot
//     call must not land on a closed Redis / Postgres handle. The
//     WaitGroup inside funnel.Broker.Stop is the ordering witness —
//     Stop returning IS the "owners exited" signal.
//  2. For bundle in reverse(bundles): bundle.Close — persistence.Stop →
//     Redis close → Postgres close (capstone's own LIFO).
//  3. Store.Close — closes every handle the Store owns (redis then pool)
//     in the correct internal order. Phase 2 unified this onto one call
//     so shutdown ordering lives in one place; earlier the pool was
//     closed from here directly, which was fine for a one-handle Store
//     but fragmented once Phase 2 added redis + lifecycle.
//
// Idempotent via Runtime.shutdownOnce. Nil-safe — a nil Runtime is a
// no-op (defensive; a caller shouldn't nil us, but double-free panics
// are worse than a tiny guard).
func (runtime *Runtime) Shutdown() {
	if runtime == nil {
		return
	}
	runtime.shutdownOnce.Do(func() {
		slog.Info("bringup: Shutdown begin",
			"shard_count", len(runtime.bundles),
		)

		// 1. owners exit first — Broker.Stop closes each inbox and
		//    shardWG.Wait blocks until every run() returns.
		if runtime.Broker != nil {
			runtime.Broker.Stop()
		}

		// 2. LIFO close of per-shard bundles. Each bundle.Close is
		//    itself idempotent (capstone/bundle.go), so partial state
		//    does not panic.
		for bundleIndex := len(runtime.bundles) - 1; bundleIndex >= 0; bundleIndex-- {
			runtime.bundles[bundleIndex].Close()
		}

		// 3. Store teardown — redis then pool, both inside Store.Close.
		if runtime.Store != nil {
			runtime.Store.Close()
		}

		slog.Info("bringup: Shutdown done")
	})
}
