package lifecycle

import (
	"context"

	"simulator/economy/account"
)

// OndemandSessionLoad is LAYER_5_2_LIFECYCLE_FLOWS item 20 — the
// on-demand variant of session load: reads the durable Postgres account
// read-only, builds an isolated throwaway working copy, and runs the
// session with NO write-back path. At session end everything is
// discarded (SessionDiscard, item 22); the durable account never learns
// the session happened.
//
// The single storage-level difference from SessionLoad (19):
//   - PersistOnClose=false on the returned Account. This one boolean
//     routes teardown to SessionDiscard instead of SessionSave and
//     structurally prevents any Postgres write from realize (5.4's
//     enqueue gate reads it).
//
// Seed source — Postgres durable figure (Option A, LOCKED per doc §4).
// The seed is the participant's real balance (read-only). Considered and
// set aside: seeding from a live canonical session's in-flight state.
// Under the single-active-session rule a participant cannot have both
// live simultaneously, so the options collapse.
//
// Snapshot-write failure tolerance. Unlike canonical, on-demand LOG-AND-
// CONTINUES on WriteSnapshot failure (doc §4 step 6): an on-demand
// crash-loss costs nothing (never writes back, isn't a record), so
// hard-failing THIS participant's session start would be over-strict.
// The tolerance branch is already coded inside loadSession (which
// SessionLoad also uses) — this method just picks the OnDemand branch.
//
// Isolation guarantees (structural, not conventional — doc §4):
//   - No shared memory during the session: loadSession news up a fresh
//     *Account; nothing aliases any canonical working state.
//   - No write-back after the session: PersistOnClose=false routes to
//     SessionDiscard (22), never SessionSave (21).
//
// Downstream contract (rule ORIGINATES here, enforced elsewhere): the
// realization path (5.4) MUST NOT enqueue on-demand closed trades to the
// durable buffer. That gate reads a.PersistOnClose and skips the enqueue
// when false — persistence has no on/off knob of its own.
func (service *Service) OndemandSessionLoad(ctx context.Context, sessionID string, accountID account.AccountID) (LoadedAccount, error) {
	return service.loadSession(ctx, sessionID, accountID, OnDemand)
}
