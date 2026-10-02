package bringup

import "log/slog"

// Shutdown tears the broker down in strict LIFO order
// (BROKER_PHASE1 §1.5):
//
//  1. Broker.Stop — closes each shard inbox; shardWG.Wait blocks until
//     every owner goroutine has exited. **Owners must exit before any
//     bundle closes**: a shard's final MarkSnapshotDirty / WriteSnapshot
//     call must not land on a closed Redis / Postgres handle. The
//     WaitGroup inside funnel.Broker.Stop is the ordering witness —
//     Stop returning IS the "owners exited" signal.
//  2. For bundle in reverse(bundles): bundle.Close — persistence.Stop →
//     Redis close → Postgres close (capstone's own LIFO).
//  3. Store pool close — closed LAST, independently of any bundle.
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

		// 3. Store pool closed LAST, via the Store's Pool() accessor so
		//    the handle lives in one place. The Store itself does not
		//    close its pool — this call site is the sole closer.
		if runtime.Store != nil {
			if pool := runtime.Store.Pool(); pool != nil {
				pool.Close()
			}
		}

		slog.Info("bringup: Shutdown done")
	})
}
