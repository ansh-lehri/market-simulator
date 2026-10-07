package order

import (
	"simulator/broker/funnel"
)

// Compile-time assertion that *funnel.Broker satisfies brokerPort. If
// the funnel ever drops / renames PlaceOrder or CancelOrder, this
// package stops compiling at this line — much earlier than the first
// handler test that would otherwise flag a stale shape.
//
// No test function needed; the var-declaration IS the assertion.
var _ brokerPort = (*funnel.Broker)(nil)
