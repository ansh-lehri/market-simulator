package store

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"simulator/economy/lifecycle"
)

// Store is the broker-owned, shard-independent durable layer
// (BROKER_PHASE1 §2 / §5 + Phase-2 supersession P2-1·D3).
//
// Three handles — each a different kind of pool — all owned together so
// one shutdown call tears them down in LIFO order:
//
//   - pool       its own Postgres pool (same cluster as the per-shard
//                bundles, independently sized via BrokerConfig.StoreMaxConns).
//                Backs every Phase-1 durable read, the off-session
//                DurableTransfer, and the Phase-2 reads/writes the
//                lifecycle service issues.
//   - redis      its own Redis client (same cluster + same empty KeyNamespace
//                as the per-shard bundles so a session snapshot the Store
//                writes at load lands at the key the owning shard later
//                mutates and the save path deletes). Idle during
//                EnsureAccount birth (OpenAccount is Postgres-only); active
//                during StartSession's SessionLoad / OndemandSessionLoad
//                where WriteSnapshot seeds the session's initial payload.
//   - lifecycle  an economy/lifecycle.Service backed by an UNSTARTED
//                persistence.Component. The Store never calls
//                EnqueueClosedTrade (that lives on the live per-shard
//                bundles), so the drainer is never Started and no goroutine
//                exists to tear down. The Component only mediates
//                OpenAccount / LoadAccount / WriteSnapshot — all synchronous
//                owner-driven methods.
//
// Phase-2 doc P2-1·D3 LOCKED the three-field shape; this file's older
// "pool only, no Redis, no lifecycle" rule is superseded.
//
// Ownership on shutdown: pool is closed LAST by the wiring root (after
// every per-shard bundle is torn down and after Store.Close); redis is
// closed by Store.Close (called by bringup.Shutdown just before the pool
// close). The persistence.Component the lifecycle service wraps holds no
// goroutine of its own in Store because Start was never called — nothing
// to Stop, nothing to drain, GC reclaims the struct when Runtime drops.
type Store struct {
	pool      *pgxpool.Pool
	redis     *redis.Client
	lifecycle *lifecycle.Service
}

// NewStore wraps the three handles the wiring root composed for the Store
// surface. All three MUST be non-nil in production; a nil here is a
// programming error at the one caller (bringup) and surfaces at first use
// via nil deref — consistent with the root constitution's
// "invariant violation → panic" rule for programming errors.
func NewStore(pool *pgxpool.Pool, redisClient *redis.Client, lifecycleSvc *lifecycle.Service) *Store {
	return &Store{
		pool:      pool,
		redis:     redisClient,
		lifecycle: lifecycleSvc,
	}
}

// Pool returns the underlying Postgres pool so callers (bringup smoke
// tests, future diagnostics) can run SQL against it directly. Shutdown
// is NOT the caller's responsibility — Store.Close owns teardown of
// every handle the Store holds, including the pool.
func (store *Store) Pool() *pgxpool.Pool {
	return store.pool
}

// Lifecycle returns the Store-owned economy/lifecycle.Service that Phase-2's
// EnsureAccount (OpenAccount) and StartSession (SessionLoad /
// OndemandSessionLoad) drive. Off-shard orchestration only — the funnel's
// shards never touch this service (their L5 compute stack lives on each
// shard's own per-bundle persistence.Component).
func (store *Store) Lifecycle() *lifecycle.Service {
	return store.lifecycle
}

// Close releases every handle the Store owns, in the correct internal
// order (redis before pool: the Component is wrapped by the lifecycle
// service but holds no goroutine of its own here, so closing redis first
// just frees connections; pool goes last because the Component reads
// through it and we want its handle live until nothing can call in).
// Phase 2 unified teardown onto this one method so the wiring root makes
// one call rather than threading an ordering convention through
// bringup.Shutdown.
//
// Call site: bringup.Shutdown, AFTER Broker.Stop (owners exited) AND
// AFTER every per-shard bundle has been torn down (so a late shard
// MarkSnapshotDirty can't race the Store's redis close).
//
// Nil-safe. Idempotent — a second call is a cheap no-op after the handles
// have been nil'd. The persistence.Component the lifecycle service wraps
// was never Started, so no drainer goroutine exists to tear down; GC
// reclaims the Component when Runtime drops.
func (store *Store) Close() {
	if store == nil {
		return
	}
	if store.redis != nil {
		if err := store.redis.Close(); err != nil {
			slog.Warn("broker/store: redis close error (non-fatal)", "cause", err)
		}
		store.redis = nil
	}
	if store.pool != nil {
		store.pool.Close()
		store.pool = nil
	}
}
