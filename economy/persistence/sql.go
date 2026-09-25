package persistence

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
)

// dbConn is the narrow Postgres surface the SQL layer uses. Satisfied by
// *pgxpool.Pool (production) and by pgx.Tx (integration tests, so each test
// runs its whole flow inside an outer transaction that rolls back — nested
// tx via savepoints keeps store.withTx working the same way in both). This
// interface is the point of testability for the SQL layer.
type dbConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// store is the SQL layer — the "hands" of persistence. Every SQL statement
// (Postgres) and every Redis command in the package originates here or (for
// the country-specific trade_costs row) inside the injected
// CountryCostAdapter. Method signatures use only primitives + flat DTOs; no
// domain type crosses this layer.
//
// Held by Component (Stage 3); never exposed publicly.
type store struct {
	db          dbConn
	redis       *redis.Client
	cfg         Config
	costAdapter CountryCostAdapter
}

// newStore builds a store. The caller (Component) has already applied
// cfg.withDefaults() and validated the adapter is non-nil.
func newStore(db dbConn, redisClient *redis.Client, adapter CountryCostAdapter, cfg Config) *store {
	return &store{
		db:          db,
		redis:       redisClient,
		cfg:         cfg,
		costAdapter: adapter,
	}
}

// withTx runs fn inside a Postgres transaction. On success it commits; on
// any error (from fn OR commit) it rolls back and returns the error wrapped
// with context. When db is a pgx.Tx (test path), Begin creates a savepoint
// so the outer tx-rollback discipline still holds.
func (component *store) withTx(ctx context.Context, fn func(tx pgx.Tx) error) (err error) {
	tx, beginErr := component.db.Begin(ctx)
	if beginErr != nil {
		return fmt.Errorf("persistence: begin tx: %w", beginErr)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx) // best-effort; primary error already set
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		err = fmt.Errorf("persistence: commit tx: %w", commitErr)
	}
	return err
}
