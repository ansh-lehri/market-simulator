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

// Phase-2 StartSession / InstallSession sentinels (BROKER_PHASE2 §3 error
// table). Category sentinels per [[feedback_error_category_sentinels]];
// callers dispatch with errors.Is.

// ErrBadBinding — StartSession rejected the SessionStartCmd because its
// ExchangeInbound was nil. Validated off-shard before any Store I/O so a
// malformed command wastes zero resources (no Postgres read, no Redis
// write, no funnel command routed).
var ErrBadBinding = errors.New("funnel: SessionStartCmd has nil ExchangeInbound")

// ErrBadParticipant — StartSession rejected the SessionStartCmd because
// its Participant was empty. The participant stamp rides every outbound
// punch (Tier-1 FundedOrder); an empty participant would propagate as a
// silent self-match bug, so we hard-fail at the ingress.
var ErrBadParticipant = errors.New("funnel: SessionStartCmd has empty ParticipantID")

// ErrSessionAlreadyLive — handleInstallSession refused to install because
// an ownedSession already exists for this AccountID. The one-live-session
// XOR invariant (BROKER_TIER0 §Point-5 + Phase-2 §3): a canonical and
// an on-demand session for the same account cannot coexist on the shard.
// Install never overwrites; the first live session wins until teardown
// clears the slot.
var ErrSessionAlreadyLive = errors.New("funnel: session already live for account")
