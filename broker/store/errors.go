package store

import "errors"

// Sentinel errors exposed by this package. All consumers dispatch via
// errors.Is on these category sentinels — never errors.As on a struct
// (feedback_error_category_sentinels).
//
// ErrNoAccount is a SIGNAL, not a failure. Phase 2's EnsureAccount reads
// it as "owner has no account yet, create one"; DurableTransfer reuses it
// via an existence probe when a guarded UPDATE affects 0 rows. The
// difference between "missing" and "insufficient" is the whole reason
// ErrNoAccount lives alongside ErrInsufficient.
//
// ErrLoadFailed wraps infra/driver failures (bad DSN, dropped connection,
// context cancelled, malformed arguments) — anything that is NOT a
// business-level signal.
//
// ErrBadTransfer and ErrInsufficient are DurableTransfer's typed outcomes
// (Stage C); declared here so the sentinel surface for the whole Store is
// in one file.
var (
	ErrNoAccount    = errors.New("broker/store: no account for owner")
	ErrLoadFailed   = errors.New("broker/store: durable load failed")
	ErrBadTransfer  = errors.New("broker/store: bad transfer request")
	ErrInsufficient = errors.New("broker/store: insufficient funds for transfer")
)
