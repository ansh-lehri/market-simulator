package reads

import (
	"simulator/broker/funnel"
	"simulator/broker/store"
)

// Compile-time guarantees that the production types satisfy the
// consumer-side ports — a missing method on either side is a build
// break, caught at `go build ./broker/reads/...` time with no runtime
// test needed.
//
// Mirror of the "var _ Command = ..." pattern in broker/funnel/commands_test.go.
// If a Phase-6 facade signature on *funnel.Broker ever drifts (an extra
// arg, a renamed return), or if a Store method is renamed, the build
// stops here before any handler can compile.
var (
	_ brokerPort = (*funnel.Broker)(nil)
	_ storePort  = (*store.Store)(nil)
)
