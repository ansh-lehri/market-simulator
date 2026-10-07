package order

import (
	"context"

	"simulator/broker/funnel"
)

// brokerPort is the minimal shape the three order HTTP handlers need
// from the funnel broker. Two methods — mirror of Broker.PlaceOrder /
// Broker.CancelOrder verbatim.
//
// Why one interface for all three endpoints (not three): the modify
// composer needs BOTH methods on the SAME broker instance; splitting
// into placePort / cancelPort would force the modify handler to depend
// on two interfaces that must happen to be satisfied by the same object
// (an awkwardness Go doesn't express well). One joined interface is
// simpler; a handler that only uses one method stays dependency-honest
// via the usual "unused argument is fine when it satisfies an iface"
// Go idiom.
//
// *funnel.Broker satisfies this interface implicitly; nothing on the
// funnel changes.
//
// Scope note: this interface lives on the consumer side (broker/order/),
// not on the funnel — Go idiom, same rationale as broker/transfer's
// brokerPort. The producer stays unaware; the HTTP shape is the HTTP
// shape's problem.
type brokerPort interface {
	// PlaceOrder forwards an OrderSpec to the owning shard's
	// handlePlaceOrder, blocking on the reply or ctx deadline. Returns
	// funnel.ErrBrokerTimeout on ctx.Done. The sessionID argument is
	// vestigial on the entry-point today (BROKER_PHASE5 §4.3) — the
	// handler resolves the session via the shard's sessions map; HTTP
	// callers pass "" and let the shard answer. Kept on the signature so
	// *funnel.Broker satisfies this interface with no shim.
	PlaceOrder(
		ctx context.Context,
		accountID funnel.AccountID,
		sessionID funnel.SessionID,
		order funnel.OrderSpec,
	) (funnel.PlaceResult, error)

	// CancelOrder forwards a cancel to the owning shard's handleCancel,
	// blocking on the reply or ctx deadline. Returns
	// funnel.ErrBrokerTimeout on ctx.Done.
	CancelOrder(
		ctx context.Context,
		accountID funnel.AccountID,
		orderID funnel.OrderID,
	) (funnel.CancelResult, error)
}
