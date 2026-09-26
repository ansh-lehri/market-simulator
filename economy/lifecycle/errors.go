package lifecycle

import (
	"errors"
	"fmt"

	"simulator/economy/account"
)

// Sentinel roots — callers dispatch with errors.Is on these categories per
// [[feedback_error_category_sentinels]] regardless of which struct-typed
// error wraps them.
var (
	// ErrNoAccount — the durable accounts row was not found at
	// session_load / ondemand_session_load / crash_recovery_reload.
	// LAYER_5_2_LIFECYCLE_FLOWS §3–§4: never lazy-create; caller must
	// have run open_account first.
	ErrNoAccount = errors.New("lifecycle: durable account not found")

	// ErrAccountExists — open_account attempted to insert a row for an
	// (owner_type, owner_id) that already has one. Business error; the
	// caller (agent re-spawn / human re-login) looks up the existing
	// handle rather than treating this as fatal.
	ErrAccountExists = errors.New("lifecycle: account already exists for owner")

	// ErrLoadFailed — a store read failed with a transport / driver
	// error (Postgres unreachable, Redis timeout). Never seed a session
	// from a failed money read.
	ErrLoadFailed = errors.New("lifecycle: durable-account load failed")

	// ErrSaveFailed — the canonical session_save Postgres UPDATE failed.
	// The Redis snapshot is deliberately RETAINED so the final state has
	// one surviving copy (LAYER_5_2_LIFECYCLE_FLOWS §5 step 5:
	// don't-discard-on-failed-save).
	ErrSaveFailed = errors.New("lifecycle: session save failed")

	// ErrSnapshotWriteFailed — the initial Redis WriteSnapshot at
	// session_load failed. Canonical session start hard-fails (a session
	// with no snapshot is unrecoverable on crash). On-demand tolerates.
	ErrSnapshotWriteFailed = errors.New("lifecycle: initial snapshot write failed")

	// ErrSnapshotMissing — read_balance_snapshot (item 16) found no
	// snapshot for this (session, account). Bootstrap ran out of order;
	// hard stop, never fabricate a balance.
	ErrSnapshotMissing = errors.New("lifecycle: session snapshot missing")

	// ErrNoSnapshot — crash_recovery_reload (item 23) found no snapshot
	// to recover from. Distinct from ErrSnapshotMissing to make the
	// caller's decision explicit: recovery has nothing to restore.
	ErrNoSnapshot = errors.New("lifecycle: no snapshot to recover from")

	// ErrNotCanonical — session_save (item 21) was called on an
	// on-demand account (PersistOnClose=false). Defense-in-depth: refuse
	// rather than corrupt the durable account. Route to session_discard.
	ErrNotCanonical = errors.New("lifecycle: session_save requires PersistOnClose=true")

	// ErrCreateFailed — open_account's INSERT failed with a driver /
	// non-uniqueness error.
	ErrCreateFailed = errors.New("lifecycle: account creation failed")
)

// open_account input-validation sentinels (LAYER_5_2_LIFECYCLE_FLOWS §2
// step 1). Birth-time floors: trading_cash / bank_cash are BOTH >= 0 at
// birth (signed trading_cash exists for later debt, not at birth).
var (
	ErrBadOwnerType            = errors.New("lifecycle: owner_type must be one of {agent, human}")
	ErrBadOwnerID              = errors.New("lifecycle: owner_id must be non-empty")
	ErrNegativeStartingCapital = errors.New("lifecycle: starting_capital must be >= 0")
	ErrNegativeStartingBank    = errors.New("lifecycle: starting_bank must be >= 0")
)

// Struct-typed error carriers wrap the sentinels with context ids. Callers
// use errors.Is for category dispatch and errors.As for the ids.

// ErrNoAccountFor carries the missing accountID.
type ErrNoAccountFor struct{ AccountID account.AccountID }

func (errNoAccountFor ErrNoAccountFor) Error() string {
	return fmt.Sprintf("lifecycle: durable account not found: %s", errNoAccountFor.AccountID)
}
func (errNoAccountFor ErrNoAccountFor) Unwrap() error { return ErrNoAccount }

// ErrAccountExistsFor carries the (OwnerType, OwnerID) pair that collided.
type ErrAccountExistsFor struct {
	OwnerType OwnerType
	OwnerID   string
}

func (errAccountExistsFor ErrAccountExistsFor) Error() string {
	return fmt.Sprintf("lifecycle: account already exists for owner_type=%s owner_id=%s",
		errAccountExistsFor.OwnerType, errAccountExistsFor.OwnerID)
}
func (errAccountExistsFor ErrAccountExistsFor) Unwrap() error { return ErrAccountExists }

// ErrLoadFailedFor carries the accountID and the wrapped driver cause.
type ErrLoadFailedFor struct {
	AccountID account.AccountID
	Cause     error
}

func (errLoadFailedFor ErrLoadFailedFor) Error() string {
	return fmt.Sprintf("lifecycle: durable-account load failed for %s: %v",
		errLoadFailedFor.AccountID, errLoadFailedFor.Cause)
}
func (errLoadFailedFor ErrLoadFailedFor) Unwrap() []error {
	return []error{ErrLoadFailed, errLoadFailedFor.Cause}
}

// ErrSaveFailedFor carries the accountID and the wrapped driver cause.
type ErrSaveFailedFor struct {
	AccountID account.AccountID
	Cause     error
}

func (errSaveFailedFor ErrSaveFailedFor) Error() string {
	return fmt.Sprintf("lifecycle: session save failed for %s: %v",
		errSaveFailedFor.AccountID, errSaveFailedFor.Cause)
}
func (errSaveFailedFor ErrSaveFailedFor) Unwrap() []error {
	return []error{ErrSaveFailed, errSaveFailedFor.Cause}
}

// ErrSnapshotWriteFailedFor carries the (SessionID, AccountID) that failed
// to seed plus the wrapped Redis cause.
type ErrSnapshotWriteFailedFor struct {
	SessionID string
	AccountID account.AccountID
	Cause     error
}

func (errSnapshotWriteFailedFor ErrSnapshotWriteFailedFor) Error() string {
	return fmt.Sprintf("lifecycle: initial snapshot write failed for session=%s account=%s: %v",
		errSnapshotWriteFailedFor.SessionID, errSnapshotWriteFailedFor.AccountID, errSnapshotWriteFailedFor.Cause)
}
func (errSnapshotWriteFailedFor ErrSnapshotWriteFailedFor) Unwrap() []error {
	return []error{ErrSnapshotWriteFailed, errSnapshotWriteFailedFor.Cause}
}

// ErrSnapshotMissingFor carries (SessionID, AccountID) for read-16.
type ErrSnapshotMissingFor struct {
	SessionID string
	AccountID account.AccountID
}

func (errSnapshotMissingFor ErrSnapshotMissingFor) Error() string {
	return fmt.Sprintf("lifecycle: session snapshot missing for session=%s account=%s",
		errSnapshotMissingFor.SessionID, errSnapshotMissingFor.AccountID)
}
func (errSnapshotMissingFor ErrSnapshotMissingFor) Unwrap() error { return ErrSnapshotMissing }

// ErrNoSnapshotFor carries (SessionID, AccountID) for crash_recovery_reload.
type ErrNoSnapshotFor struct {
	SessionID string
	AccountID account.AccountID
}

func (errNoSnapshotFor ErrNoSnapshotFor) Error() string {
	return fmt.Sprintf("lifecycle: no snapshot to recover from for session=%s account=%s",
		errNoSnapshotFor.SessionID, errNoSnapshotFor.AccountID)
}
func (errNoSnapshotFor ErrNoSnapshotFor) Unwrap() error { return ErrNoSnapshot }

// ErrCreateFailedFor carries the identity that failed to create plus the
// wrapped driver cause.
type ErrCreateFailedFor struct {
	OwnerType OwnerType
	OwnerID   string
	Cause     error
}

func (errCreateFailedFor ErrCreateFailedFor) Error() string {
	return fmt.Sprintf("lifecycle: account creation failed for owner_type=%s owner_id=%s: %v",
		errCreateFailedFor.OwnerType, errCreateFailedFor.OwnerID, errCreateFailedFor.Cause)
}
func (errCreateFailedFor ErrCreateFailedFor) Unwrap() []error {
	return []error{ErrCreateFailed, errCreateFailedFor.Cause}
}
