package persistence

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Component is the public persistence seam — the value the wiring root
// injects into reserve.NewReservationService and realize.NewRealizer in
// place of the Phase-1 no-op/spy. It satisfies both reserve.Persistence
// (MarkSnapshotDirty only) and realize.Persistence (adds EnqueueClosedTrade)
// STRUCTURALLY: this package does not import reserve — see seam_guard_test.go
// for the reserve compile-time guard.
//
// Component holds:
//   - the SQL layer (store) that owns Postgres + Redis primitives
//   - the dirty-account set (D6, plain map, no lock — Phase 2 is owner-driven)
//   - the injected Config (with defaults applied)
//
// The drainer goroutine (Stage 4) attaches to Component and reads only the
// buffer / pool; it does not touch the dirty map, so Phase 2 keeps its
// lock-free single-writer discipline.
type Component struct {
	store   *store
	dirty   map[dirtyKey]struct{}
	cfg     Config
	drainer *drainer
}

// DirtyKey is one entry from DrainDirty — the owner uses it to build a
// SnapshotPayload and call WriteSnapshot. Public mirror of the internal
// dirtyKey so callers can iterate without touching Component internals.
type DirtyKey struct {
	SessionID string
	AccountID string
}

// NewComponent builds a Component from injected connectivity handles and a
// country adapter. All three arguments SHOULD be non-nil in production;
// nils are tolerated only for tests that exercise a subset (e.g. seam
// tests without Postgres). Nil-deref surfaces at the first offending method
// call, not here — keeping this constructor allocation-only.
//
// cfg is copied and passed through withDefaults; the caller's Config value
// is unaffected. KeyNamespace stays as the caller set it (empty in prod,
// unique per test in tests).
func NewComponent(
	pool *pgxpool.Pool,
	redisClient *redis.Client,
	adapter CountryCostAdapter,
	cfg Config,
) *Component {
	filled := cfg.withDefaults()
	storeInstance := newStore(pool, redisClient, adapter, filled)
	return &Component{
		store:   storeInstance,
		dirty:   make(map[dirtyKey]struct{}),
		cfg:     filled,
		drainer: newDrainer(storeInstance, filled),
	}
}

// Start launches the closed-trade drainer goroutine (LAYER_5_PERSISTENCE
// §5). Call at wiring root once Postgres + Redis are connected; return
// error surfaces a double-start bug in the caller.
//
// The snapshotter stays synchronous / owner-driven in Phase 2 — Start does
// NOT spawn a snapshotter goroutine. See snapshot_api.go / DrainDirty.
func (component *Component) Start(ctx context.Context) error {
	if component.drainer == nil {
		// Tolerated only in tests that constructed a Component by hand
		// without a drainer. Production always goes through NewComponent.
		return nil
	}
	return component.drainer.Start(ctx)
}

// Stop signals the drainer goroutine to exit and waits for it. Idempotent
// — safe to call from lifecycle shutdown even if Start was never called.
// Un-drained rows stay in Redis for the next boot (D5).
func (component *Component) Stop() {
	if component.drainer == nil {
		return
	}
	component.drainer.Stop()
}
