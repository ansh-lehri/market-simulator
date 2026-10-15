package funnel

import "errors"

// ErrBrokerTimeout is the sentinel returned by a request/response Broker
// method whose context deadline (or cancel) fires before the shard's
// reply lands (BROKER_TIER0 §Point-7 / §8.1).
//
// HTTP mapping is owned by the handler of each transport-facing endpoint;
// Phase 3 locks the transfer endpoint's mapping as HTTP 504 (P3-D3,
// supersedes the Tier-0 doc's older "503" note). Other endpoints inherit
// the same 504 convention from BROKER_READ_SURFACES tier-4 §0.5. The
// sentinel value itself is transport-agnostic.
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

// Session-teardown sentinels (canonical detach-keep path).

// ErrBadAccountID — EndSession rejected the SessionEndCmd because its
// AccountID was empty. Off-shard guard before any routing.
var ErrBadAccountID = errors.New("funnel: SessionEndCmd has empty AccountID")

// ErrBadSessionID — EndSession rejected the SessionEndCmd because its
// SessionID was empty. Off-shard guard before any routing.
var ErrBadSessionID = errors.New("funnel: SessionEndCmd has empty SessionID")

// ErrNoLiveSession — handleUninstallSession or handleDiscardSession
// found no ownedSession for the AccountID. Treat as idempotent on the
// caller side (the session was already uninstalled/discarded, or the
// account was never installed) rather than as a hard failure.
var ErrNoLiveSession = errors.New("funnel: no live session for account")

// ErrSessionMismatch — handleUninstallSession or handleDiscardSession
// found a live session for the AccountID but its binding.SessionID did
// not match the command's SessionID. A stale teardown racing a fresh
// install, or a conductor retry pointed at a replaced session. Refusal
// preserves the live binding.
var ErrSessionMismatch = errors.New("funnel: SessionID does not match live session")

// ErrDiscardFailed — on-demand teardown dropped the ephemeral account
// from the shard's in-memory maps, but the Redis snapshot delete failed
// (or the shard was built without a session discarder). Category
// sentinel: callers dispatch with errors.Is; the underlying cause is
// wrapped via %w and reachable through errors.Unwrap.
//
// The shard-side maps are ALREADY clean on this error — a stale Redis
// key is TTL-cleaned; the account itself is gone from the shard and
// cannot be retried. The error exists so the caller logs the Redis
// failure for operability without changing the forward-progress of the
// teardown.
var ErrDiscardFailed = errors.New("funnel: on-demand session discard failed")

// ErrFinalizeIncomplete — FinalizeMarket's fan-out across shards
// completed, but one or more per-shard sweeps surfaced errors. The
// caller still saw a response from every shard (forward-progress at
// the shard level too), but the aggregate is not fully clean. Wrap of
// the first per-shard cause rides along via %w.
var ErrFinalizeIncomplete = errors.New("funnel: FinalizeMarket completed with per-shard errors")

// ErrNonCanonicalDuringFinalize — FinalizeMarket encountered a
// LoadedAccount with PersistOnClose=false. This phase does not sweep
// on-demand accounts (the on-demand teardown owns their discard path);
// the invariant is that only canonical accounts land in a canonical
// market's loadedByExchange index. Dev-fatal — surfaces as a shard
// panic per the project constitution.
var ErrNonCanonicalDuringFinalize = errors.New("funnel: FinalizeMarket encountered a non-canonical account (invariant violation)")
