package capstone

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"simulator/connectivity/config"
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

	// IndiaRates is the injected cost rulebook. Injected — not
	// hardcoded in this package — because the rulebook home is
	// undecided (see [[project_broker_rulebook_injection_tbd]]). The
	// capstone test supplies a fixture matching
	// [[project_india_rate_locks]].
	IndiaRates cost.IndiaRates
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
// Stage 1: skeleton only. Stages 2–5 fill this in.
func Wire(ctx context.Context, opts Options) (*Bundle, error) {
	return nil, errors.New("capstone.Wire: not implemented (stage 1 skeleton)")
}

// Close tears the bundle down in reverse dependency order (LIFO): stop
// the drainer goroutine first (so its final tick never touches a closed
// handle), then close Redis, then close Postgres. Idempotent and
// nil-safe — Wire's partial-failure rollback path also runs this same
// sequence, and a caller's defer-Close never panics on a nil or
// twice-closed handle.
//
// Stage 1: no-op skeleton. Stages 2–3 add the real teardown.
func (bundle *Bundle) Close() {
	if bundle == nil {
		return
	}
}
