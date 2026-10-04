package transfer

import (
	"context"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/account"
)

// brokerPort is the minimal shape the HTTP handler needs from the funnel
// broker. One method, mirroring funnel.Broker.Transfer verbatim. Keeping
// the surface to one method lets tests inject a stub for the 400 pre-
// funnel validation paths (where no broker call fires) and the 504
// timeout path (where a hanging stub makes the ctx deadline fire
// deterministically — the live broker's shard would race the deadline).
//
// *funnel.Broker satisfies this interface implicitly; nothing on the
// funnel changes.
//
// Scope note: this interface lives inside broker/transfer/ (the consumer),
// not inside broker/funnel/. Go idiom: interfaces belong on the caller
// side so the producer stays unaware. The alternative (an exported
// funnel.BrokerPort) would couple the funnel to the HTTP shape and leak
// a seam that only one caller needs.
type brokerPort interface {
	Transfer(
		ctx context.Context,
		accountID funnel.AccountID,
		fromBucket account.Bucket,
		toBucket account.Bucket,
		amount int64,
	) (funnel.TransferResult, error)
}

// storePort is the minimal shape the HTTP handler needs from the Store.
// One method — DurableTransfer — the Phase-1 guarded UPDATE used on the
// idle-account branch (BROKER_PHASE3 §2.3). *store.Store satisfies it.
//
// Same scoping rationale as brokerPort: consumer-side interface.
type storePort interface {
	DurableTransfer(
		ctx context.Context,
		accountID store.AccountID,
		direction store.TransferDirection,
		amount int64,
	) (available int64, bank int64, err error)
}
