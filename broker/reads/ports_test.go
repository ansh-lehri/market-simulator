package reads

import (
	"simulator/broker/funnel"
	"simulator/broker/store"
)

// Compile-time guarantees that the production types still satisfy the
// consumer-side ports. If a facade signature on *funnel.Broker drifts
// (an extra arg, a renamed return) or a Store method is renamed, the
// build stops here before any handler can compile — no runtime test
// needed.
var (
	_ brokerPort = (*funnel.Broker)(nil)
	_ storePort  = (*store.Store)(nil)
)
