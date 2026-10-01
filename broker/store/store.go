package store

import "github.com/jackc/pgx/v5/pgxpool"

// Store is the broker-owned durable layer. It holds ONLY a Postgres pool —
// no Redis (the snapshot mirror is a per-shard bundle's concern), no shard
// references, no economy references.
//
// The pool is handed in by the wiring root (Phase-1 Stage D). Store does
// NOT close the pool itself — the wiring root's Shutdown closes it LAST,
// after every per-shard bundle has been torn down. See BROKER_PHASE1
// §1.5 and this package's doc.go.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a Postgres pool as a Store. Pool must be non-nil at
// runtime; a nil pool here is a programming error (the wiring root is the
// one caller, and it builds the pool first). We let it panic naturally
// at first query rather than guarding here — a nil pool at this stage is
// a dev-fatal, consistent with the root's "invariant violation → panic"
// rule for programming errors.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}
