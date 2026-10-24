package seedinventory

import (
	"context"

	"simulator/broker/funnel"
)

// brokerPort is the minimal shape the HTTP handler needs from the
// funnel broker. One method, mirroring funnel.Broker.SeedInventory
// verbatim (same consumer-side-interface rationale as broker/transfer's
// brokerPort: tests inject a stub for the 400 pre-funnel paths and the
// 504 timeout path without needing a live shard to race a deadline).
//
// *funnel.Broker satisfies this interface implicitly; nothing on the
// funnel changes to accommodate it.
type brokerPort interface {
	SeedInventory(
		ctx context.Context,
		accountID funnel.AccountID,
		instrument funnel.InstrumentID,
		qty int64,
		entryPrice int64,
	) (funnel.SeedInventoryResult, error)
}
