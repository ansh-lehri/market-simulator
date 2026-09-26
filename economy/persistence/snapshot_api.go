package persistence

import (
	"context"
	"log/slog"
)

// WriteSnapshot writes ONE account's live-state snapshot to Redis. The owner
// goroutine calls this synchronously (Phase 2), passing a value-copy of the
// account's cash + positions + reservations. On success the dirty flag for
// that (sessionID, accountID) is cleared; a failed write RETAINS the flag
// so the owner's next tick retries the same account.
//
// This is the ONE public write path into the snapshot mirror. Owner code
// walks DrainDirty(), assembles a SnapshotPayload from its live Account +
// ReservationBook filtered to that account, and calls WriteSnapshot.
func (component *Component) WriteSnapshot(ctx context.Context, payload SnapshotPayload) error {
	if err := component.store.writeSnapshot(ctx, payload); err != nil {
		return err
	}
	delete(component.dirty, dirtyKey{SessionID: payload.SessionID, AccountID: payload.AccountID})
	slog.Debug("persistence: WriteSnapshot cleared dirty",
		"session_id", payload.SessionID,
		"account_id", payload.AccountID,
		"dirty_count", len(component.dirty),
	)
	return nil
}

// ReadSnapshot reads ONE account's snapshot back from Redis. Used by
// LAYER_5_1_READ_OPERATIONS item 16 (read_balance_snapshot) and by
// crash-recovery load at session start (before Postgres is consulted).
//
// A missing cash key returns ErrSnapshotMissing (with the ids-populated
// payload); any other Redis / decode error is wrapped. Callers use
// errors.Is(err, ErrSnapshotMissing) to distinguish "no snapshot at all"
// from a legitimately-zero snapshot and from transport failure.
func (component *Component) ReadSnapshot(ctx context.Context, sessionID, accountID string) (SnapshotPayload, error) {
	return component.store.readSnapshot(ctx, sessionID, accountID)
}

// DeleteSnapshot removes ONE account's snapshot from Redis. Called on a
// clean session close (LAYER_5_PERSISTENCE §7: "Snapshot is discarded on
// clean close — a within-session safety net, not a record"). Also clears
// the dirty flag if one lingered (a clean close should not leave a dirty
// entry pointing at a deleted snapshot).
func (component *Component) DeleteSnapshot(ctx context.Context, sessionID, accountID string) error {
	if err := component.store.deleteSnapshot(ctx, sessionID, accountID); err != nil {
		return err
	}
	delete(component.dirty, dirtyKey{SessionID: sessionID, AccountID: accountID})
	return nil
}

// DrainDirty returns a snapshot of the current dirty set as a slice. Does
// NOT clear the set — the caller (owner) iterates the slice, assembles a
// SnapshotPayload for each entry, and calls WriteSnapshot which then clears
// the flag on its own success path. This lets a partial iteration recover
// on the next tick.
//
// Iteration order is not defined (Go map iteration).
func (component *Component) DrainDirty() []DirtyKey {
	keys := make([]DirtyKey, 0, len(component.dirty))
	for key := range component.dirty {
		keys = append(keys, DirtyKey{SessionID: key.SessionID, AccountID: key.AccountID})
	}
	return keys
}
