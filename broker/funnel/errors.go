package funnel

import "errors"

// ErrBrokerTimeout is the sentinel returned by a request/response Broker
// method whose context deadline (or cancel) fires before the shard's
// reply lands (BROKER_TIER0 §Point-7 / §8.1). It maps to HTTP 503 at the
// transport layer.
//
// A timeout is the CALLER's problem, not the shard's: the shard still
// processes the command (already in its inbox) and still fires the
// result into the caller's buffered(1) reply channel; the value drops
// into the buffer, is never read, and is garbage-collected. The caller
// reconciles later via pull-based reads — a timeout is not "nothing
// happened."
var ErrBrokerTimeout = errors.New("funnel: request timed out")
