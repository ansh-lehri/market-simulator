package lifecycle

import (
	"context"
	"errors"
	"log/slog"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/persistence"
)

// RestoredSession is what CrashRecoveryReload returns: the L5 slice of a
// restarted session's in-memory working state, plus the raw reservation
// snapshots that the caller (funnel / reserve) will use to rehydrate the
// reservation book.
//
// Why the split (Account + ReservationSnaps carried separately):
//   - account.Account owns the cash split + positions. Those live in this
//     package's import graph.
//   - The reservation book lives in reserve.ReservationBook. Lifecycle
//     cannot import reserve (per lifecycle/CLAUDE.md: "Imports no ...
//     reserve/cost, no driver"). So the raw persistence.ReservationSnap
//     slice is passed through UNTOUCHED — the caller hands it to reserve
//     for hydration.
//   - This preserves the "lifecycle cares about what it owns" boundary
//     the doc's §7 flags: L5 rebuilds L5's state; cross-layer wiring
//     (reservation book → engine's resting orders, close-accumulator
//     restore, agent re-attach) is the funnel's joint pass.
//
// The full ReservationSnap carries OrigQty / EverFilled / BrokerageCharged,
// so reserve's brokerage-once guarantee survives a crash — the summary
// shape LAYER_5_PERSISTENCE §7 sketches would lose it (see the flag in
// persistence/CLAUDE.md and lifecycle/CLAUDE.md).
type RestoredSession struct {
	Account          *account.Account
	ReservationSnaps []persistence.ReservationSnap
}

// CrashRecoveryReload is LAYER_5_2_LIFECYCLE_FLOWS item 23 — restore a live
// session from Redis after a process crash mid-session. L5 SLICE ONLY: the
// cross-layer coordination (engine book rebuild, L2 agent re-attach,
// close-accumulator restore) is DEFERRED to the joint funnel pass per doc
// §7 status + lifecycle/CLAUDE.md flag. This flow restores L5's own state
// correctly; the multi-layer join is not designed here.
//
// Counterpart to SessionLoad (19) but sourced from Redis (not Postgres) and
// restoring the ALREADY-EVOLVED split verbatim (not synthesizing a fresh
// one). Mid-session Reserved / Settled may be non-zero, positions and lots
// populated, reservation records present. hydrateFreshAccount is NEVER
// called here — it belongs to the once-per-session split synthesis in
// SessionLoad.
//
// Difference from session_load (verbatim from doc §7): session_load reads
// one durable number and SYNTHESIZES a fresh split. crash_recovery_reload
// reads the already-evolved split and copies it verbatim. No synthesis, no
// reconciliation — the snapshot IS the working state.
//
// Flow (per doc §7):
//  1. ReadSnapshot(sessionID, accountID) — one Redis call reads the whole
//     payload (cash + positions + reservations).
//  2. Branch on result:
//     - snapshot missing (persistence.ErrSnapshotMissing) → ErrNoSnapshotFor.
//       Distinct from ErrSnapshotMissingFor (read_balance_snapshot) so the
//       caller's decision is explicit: "no in-flight state to recover from."
//     - other error → ErrLoadFailedFor{accountID, cause}.
//  3. Restore the cash split VERBATIM from CashSnap. Available passes
//     through signed (Option B — mid-session it may be negative).
//  4. Restore all positions VERBATIM: PositionSnap.OpenLots preserves FIFO
//     arrival order (§7 doc: "FIFO queue restored in arrival order").
//  5. Tag PersistOnClose from sessionType (same rule as SessionLoad —
//     canonical=true, on-demand=false).
//  6. Return RestoredSession — Account plus raw ReservationSnaps for the
//     caller to hand to reserve.
//
// Persistence: Reads Redis (one ReadSnapshot). Never touches Postgres —
// the restore is bounded to what the snapshotter mirrored. Writes nothing.
//
// Error surface:
//   - ErrNoSnapshotFor    — nothing to recover from (caller-policy fork:
//                           canonical could fall back to SessionLoad losing
//                           in-session progress; on-demand simply gone).
//   - ErrLoadFailedFor    — Redis transport / decode error.
func (service *Service) CrashRecoveryReload(
	ctx context.Context,
	sessionID string,
	accountID account.AccountID,
	sessionType SessionType,
) (RestoredSession, error) {
	slog.Info("lifecycle: crash_recovery_reload begin",
		"session_id", sessionID,
		"account_id", accountID,
		"session_type", sessionType,
	)

	payload, err := service.persist.ReadSnapshot(ctx, sessionID, string(accountID))
	if err != nil {
		if errors.Is(err, persistence.ErrSnapshotMissing) {
			slog.Warn("lifecycle: crash_recovery_reload ErrNoSnapshot",
				"session_id", sessionID,
				"account_id", accountID,
			)
			return RestoredSession{}, ErrNoSnapshotFor{SessionID: sessionID, AccountID: accountID}
		}
		slog.Error("lifecycle: crash_recovery_reload ErrLoadFailed",
			"session_id", sessionID,
			"account_id", accountID,
			"cause", err,
		)
		return RestoredSession{}, ErrLoadFailedFor{AccountID: accountID, Cause: err}
	}

	// Step 3 — cash split, VERBATIM (no synthesis; no reconciliation). This
	// is the evolved mid-session state as of the last snapshot cadence tick
	// before the crash. Reserved / Settled may be non-zero; Available may be
	// negative (Option B). All fields pass through.
	restored := &account.Account{
		ID: accountID,
		Cash: account.Cash{
			Available: payload.Cash.Available, // signed; may be negative
			Reserved:  payload.Cash.Reserved,
			Settled:   payload.Cash.Settled,
			Bank:      payload.Cash.BankCash,
		},
		Positions:      make(map[contract.InstrumentID]*account.Position, len(payload.Positions)),
		PersistOnClose: sessionType == Canonical,
		DripActive:     payload.Cash.DripActive,
		DripProgress:   payload.Cash.DripProgress,
	}

	// Step 4 — positions, VERBATIM. Each PositionSnap becomes one live
	// *Position keyed by instrument. OpenLots is restored in the exact
	// order it was persisted (§7: FIFO arrival order). LotSnap and
	// account.Lot share the same three fields — copy each element.
	for _, positionSnap := range payload.Positions {
		liveLots := make([]account.Lot, len(positionSnap.OpenLots))
		for lotIndex, lotSnap := range positionSnap.OpenLots {
			liveLots[lotIndex] = account.Lot{
				EntryPrice:   lotSnap.EntryPrice,
				EntryAt:      lotSnap.EntryAt,
				RemainingQty: lotSnap.RemainingQty,
			}
		}
		restored.Positions[contract.InstrumentID(positionSnap.InstrumentID)] = &account.Position{
			Available: positionSnap.AvailableUnits,
			Reserved:  positionSnap.ReservedUnits,
			OpenLots:  liveLots,
		}
	}

	slog.Info("lifecycle: crash_recovery_reload ok",
		"session_id", sessionID,
		"account_id", accountID,
		"session_type", sessionType,
		"available", restored.Cash.Available,
		"reserved", restored.Cash.Reserved,
		"settled", restored.Cash.Settled,
		"bank", restored.Cash.Bank,
		"position_count", len(restored.Positions),
		"reservation_count", len(payload.Reservations),
	)

	// Step 6 — hand back the L5 slice. ReservationSnaps travels through raw;
	// the caller passes it to reserve for book hydration (lifecycle cannot
	// import reserve). Cross-layer coordination (funnel joint pass) picks
	// up from here.
	return RestoredSession{
		Account:          restored,
		ReservationSnaps: payload.Reservations,
	}, nil
}
