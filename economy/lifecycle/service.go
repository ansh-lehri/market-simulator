package lifecycle

import (
	"context"

	"simulator/economy/persistence"
)

// store is the narrow interface lifecycle takes on the store side. It
// lists EXACTLY the six persistence.Component methods lifecycle calls —
// nothing more. *persistence.Component satisfies this interface
// structurally (Go picks it up with no changes on the persistence side).
//
// Why an interface (not *persistence.Component directly):
//   1. Testability — unit tests inject a fake with configurable per-method
//      returns without spinning up Postgres / Redis. A concrete type can't
//      be substituted; an interface can.
//   2. Explicit dependency surface — anyone auditing lifecycle sees these
//      six method signatures, not the whole *Component API (which includes
//      Start / Stop / DrainDirty / MarkSnapshotDirty / EnqueueClosedTrade
//      that lifecycle has no business calling).
//
// Kept unexported: this interface is a lifecycle-internal seam. Production
// wiring passes *persistence.Component (a public type) which satisfies it;
// no other package needs to name the interface.
type store interface {
	OpenAccount(ctx context.Context, durable persistence.DurableAccount) (string, error)
	LoadAccount(ctx context.Context, accountID string) (persistence.DurableAccount, error)
	SaveAccount(ctx context.Context, durable persistence.DurableAccount) error
	WriteSnapshot(ctx context.Context, payload persistence.SnapshotPayload) error
	ReadSnapshot(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error)
	DeleteSnapshot(ctx context.Context, sessionID, accountID string) error
}

// Compile-time proof that *persistence.Component satisfies store. Placed
// here so any drift in the persistence API surfaces at build time in
// lifecycle, not at the wiring root.
var _ store = (*persistence.Component)(nil)

// Service is the lifecycle package's public entry point — one instance
// per broker-owner goroutine (or one shared, since lifecycle runs off the
// hot path and holds no per-call state). Every flow (item 16, 18–23) is
// a method on *Service.
//
// Holds only the store; no other state. Lifecycle is stateless
// orchestration — the account state lives with the caller.
type Service struct {
	persist store
}

// NewService returns a *Service bound to the given store. Production
// wiring passes *persistence.Component; tests pass a fake store.
func NewService(persist store) *Service {
	return &Service{persist: persist}
}
