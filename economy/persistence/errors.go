package persistence

import "errors"

// ErrNoAccount is returned by loadAccount / LoadAccount when the row for the
// requested accountID does not exist. Callers use errors.Is to detect it. It
// is a business error (the account simply has not been created yet), not an
// invariant violation.
var ErrNoAccount = errors.New("persistence: account not found")

// ErrUnknownCostLine is returned by translateClosedTrade when the cost
// breakdown carries a key that is not in the India cost-line allowlist
// (D1: "an unrecognized key is an error, not a silent drop"). Callers at the
// seam (EnqueueClosedTrade, added in Stage 3) treat this as a programming
// error — a cost line without a column is a bug in cost/realize, not
// something to swallow.
var ErrUnknownCostLine = errors.New("persistence: unknown cost line")

// ErrAccountExists is returned by OpenAccount when an accounts row already
// exists for the given (owner_type, owner_id) pair (uq_accounts_owner). It
// is a business error, not an invariant violation — the caller (agent
// re-spawn / human re-login) is expected to look up the existing handle
// rather than treat it as fatal. Callers use errors.Is to detect it.
//
// UNIQUE-as-arbiter (LAYER_5_2_LIFECYCLE_FLOWS §2): we do not check-then-
// insert; the DB constraint is the sole gate on "one account per participant",
// and this sentinel is the translated form of that constraint firing.
var ErrAccountExists = errors.New("persistence: account already exists for owner")

// ErrSnapshotMissing is returned by ReadSnapshot / readSnapshot when the
// account's cash key is absent from Redis. Lifecycle needs to distinguish
// this "no snapshot for this session/account" state from a transport
// failure and from a legitimately-zero snapshot (a brand-new session with
// zero starting capital would otherwise be indistinguishable from missing).
//
// Only the cash key drives this sentinel: it is the load-bearing seed the
// session bootstrap writes first, so its absence means "no snapshot at
// all." Position / reservation mid-scan absence stays a non-fatal race
// (key vanished between SCAN and GET) and is skipped internally.
var ErrSnapshotMissing = errors.New("persistence: snapshot cash key not found")
