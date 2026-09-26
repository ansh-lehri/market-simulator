package capstone

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"simulator/connectivity/config"
	"simulator/connectivity/db"
	"simulator/economy/cost"
	"simulator/economy/lifecycle"
	"simulator/economy/persistence"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// Options is the caller-supplied input to Wire. Every field is
// required: the wiring root is deliberately explicit about which config
// and rulebook it composes against.
type Options struct {
	// ConnConfig carries the pre-loaded Postgres + Redis config (DSN,
	// ping timeouts, pool sizing). Wire does NOT read env; the caller
	// loads once via config.Load (or supplies overrides in a test) and
	// hands the resulting Config here. Keeps the "only Load reads env"
	// rule from connectivity/CLAUDE.md intact.
	ConnConfig config.Config

	// RedisNamespace is the prefix Wire hands to persistence.Config as
	// KeyNamespace. Empty in prod (keys match LAYER_5_PERSISTENCE §7
	// verbatim); unique per test run in the capstone test harness to
	// isolate namespaces from prod and from parallel runs.
	RedisNamespace string

	// Cost is the pre-built cost module for the country stack this
	// wiring composes against. Caller instantiates it — e.g.
	// cost.NewIndiaCostModule(indiaRates) — and hands the interface
	// value here. This package holds NO country-specific rate/rulebook
	// shape; every country-specific rate name and mechanic stays inside
	// the country module (per feedback_country_specifics_stay_country_specific).
	Cost cost.CostModule

	// CostAdapter is the pre-built country adapter persistence hands to
	// the SQL layer to flatten a CostBreakdown's country-keyed Lines
	// map into that country's trade_costs column layout. Same
	// injection pattern as Cost; wiring never mentions a country here.
	//
	// Correlation invariant (caller-enforced, not checked here):
	// CostAdapter MUST match Cost's country — an India cost module
	// with a US adapter would route India line names into US columns
	// and fail as ErrUnknownCostLine at first EnqueueClosedTrade.
	CostAdapter persistence.CountryCostAdapter
}

// Bundle is every wired handle the capstone (and, later, a real cmd/
// main) needs. Fields are populated by Wire in dependency order; Close
// tears them down in the reverse order (LIFO).
//
// Ownership: Pool and Redis are opened by Wire and closed by
// Bundle.Close; the caller MUST NOT close them separately. Persist.Stop
// is invoked by Bundle.Close (idempotent). The compute types (Cost,
// Ledger, Realizer, Reserve, Lifecycle) hold no OS handles and spawn no
// goroutines — GC reclaims them when the Bundle value is dropped.
type Bundle struct {
	Pool  *pgxpool.Pool
	Redis *redis.Client

	Persist *persistence.Component

	Cost     cost.CostModule
	Ledger   *memoryLedger
	Realizer *realize.Realizer
	Reserve  *reserve.ReservationService

	Lifecycle *lifecycle.Service
}

// Wire composes every economy compute package against the real infra
// handles opened from opts.ConnConfig. Populated top-down in dependency
// order (infra -> persistence -> compute -> lifecycle). On any step's
// failure Wire closes what it already opened and returns a wrapped
// error — a partial Bundle never escapes.
//
// Stage 5: full stack — infra + persistence.Component + compute (cost,
// ledger, realize, reserve) + lifecycle. Wire is complete after this
// stage; capstone tests can drive the whole loop end-to-end.
func Wire(ctx context.Context, opts Options) (*Bundle, error) {
	pool, err := db.NewPostgres(ctx, opts.ConnConfig.Postgres)
	if err != nil {
		return nil, fmt.Errorf("capstone.Wire: postgres: %w", err)
	}

	client, err := db.NewRedis(ctx, opts.ConnConfig.Redis)
	if err != nil {
		// Rollback: close what we already opened before returning.
		pool.Close()
		return nil, fmt.Errorf("capstone.Wire: redis: %w", err)
	}

	slog.Info("capstone.Wire: infra opened",
		"redis_namespace", opts.RedisNamespace,
	)

	// persistence.Config: only KeyNamespace is caller-controlled. The
	// remaining fields (DrainerTick / DrainerBatch / EnqueueTimeout /
	// TimeEncoding / SideEncoding) stay zero-valued so withDefaults
	// applies the operational defaults (100 ms / 64 / 1 s / RFC3339 /
	// string). Persistence stores the filled config internally; we
	// keep our copy minimal and let that be the source of truth.
	persistCfg := persistence.Config{
		KeyNamespace: opts.RedisNamespace,
	}
	component := persistence.NewComponent(pool, client, opts.CostAdapter, persistCfg)
	if err := component.Start(ctx); err != nil {
		// Rollback in LIFO order: nothing to stop on Component (Start
		// failed before spawning the drainer), then Redis, then Pool.
		if closeErr := client.Close(); closeErr != nil {
			slog.Warn("capstone.Wire: redis close error during rollback (non-fatal)", "cause", closeErr)
		}
		pool.Close()
		return nil, fmt.Errorf("capstone.Wire: persistence.Start: %w", err)
	}

	slog.Info("capstone.Wire: persistence started",
		"redis_namespace", opts.RedisNamespace,
	)

	// ---- compute stack ---------------------------------------------------
	// Ledger is the capstone-local *Account registry (stand-in for the
	// broker funnel's owner-side registry). Empty at construction; the
	// caller Puts a *Account into it after every session_load.
	ledger := newMemoryLedger()

	// Realize MUST be built before reserve — reserve takes it as its
	// RealizerHandoff (called from Convert's sell branch). Both take the
	// SAME *persistence.Component: for realize it satisfies the full
	// Persistence interface (MarkSnapshotDirty + EnqueueClosedTrade); for
	// reserve it satisfies the narrower one (MarkSnapshotDirty only).
	// Structural typing — no adapter, no wrapper.
	realizer := realize.NewRealizer(ledger, component, opts.Cost)

	// Reserve is the top of the compute stack. Every collaborator is
	// injected: ledger, cost module, realizer (RealizerHandoff), events
	// (nop stand-in — the event system is deferred), and the persistence
	// seam (same *Component again).
	reserveService := reserve.NewReservationService(
		ledger,
		opts.Cost,
		realizer,
		nopEvents{},
		component,
	)

	slog.Info("capstone.Wire: compute wired")

	// ---- lifecycle -------------------------------------------------------
	// Session-boundary orchestration. Takes the SAME *persistence.Component
	// as its store — *Component satisfies lifecycle's unexported store
	// interface (OpenAccount / LoadAccount / SaveAccount / WriteSnapshot /
	// ReadSnapshot / DeleteSnapshot), verified at compile time inside
	// economy/lifecycle/service.go. Stateless — no goroutine, no handle,
	// nothing for Close to tear down.
	lifecycleService := lifecycle.NewService(component)

	slog.Info("capstone.Wire: ready",
		"redis_namespace", opts.RedisNamespace,
	)

	return &Bundle{
		Pool:      pool,
		Redis:     client,
		Persist:   component,
		Cost:      opts.Cost,
		Ledger:    ledger,
		Realizer:  realizer,
		Reserve:   reserveService,
		Lifecycle: lifecycleService,
	}, nil
}

// Close tears the bundle down in reverse dependency order (LIFO): stop
// the drainer goroutine first (so its final tick never touches a closed
// handle), then close Redis, then close Postgres. Idempotent and
// nil-safe — Wire's partial-failure rollback path also runs this same
// sequence, and a caller's defer-Close never panics on a nil or
// twice-closed handle.
//
// Sequence: drainer.Stop -> Redis close -> Postgres close. Compute
// types (Cost/Ledger/Realizer/Reserve/Lifecycle) hold no resources and
// need no teardown here.
func (bundle *Bundle) Close() {
	if bundle == nil {
		return
	}
	if bundle.Persist != nil {
		// Idempotent by contract (persistence/component.go). Stopping
		// before the Redis/Postgres close so the drainer's final tick
		// never lands on a closed handle.
		bundle.Persist.Stop()
	}
	if bundle.Redis != nil {
		if err := bundle.Redis.Close(); err != nil {
			slog.Warn("capstone.Close: redis close error (non-fatal)", "cause", err)
		}
	}
	if bundle.Pool != nil {
		bundle.Pool.Close()
	}
}
