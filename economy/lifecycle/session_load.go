package lifecycle

import (
	"context"
	"errors"
	"log/slog"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/persistence"
)

// SessionLoad is LAYER_5_2_LIFECYCLE_FLOWS item 19 — the canonical
// bootstrap for ONE participant entering the platform: turns THAT
// participant's durable Postgres account into a live, in-memory working
// session. Driven ON-DEMAND per arrival (human logging in, agent spawning
// into a live exchange) — NOT a batch load at market open. If 1000 users
// enter over the day, this runs 1000 times, once each for their own
// (sessionID, accountID) pair. The "once" is scoped to a single
// participant's session, and it must complete before THAT session accepts
// any order.
//
// "Session" here means "one participant's live session," not a
// shared/global trading session (see [[project_orchestrator_role]] — the
// Orchestrator owns the per-user entry that triggers each call).
//
// Flow (per doc §3):
//  1. LoadAccount(accountID) — durable row from Postgres.
//     Missing → ErrNoAccountFor (never lazy-create).
//     Transport error → ErrLoadFailedFor.
//  2. Synthesize the fresh split: Available=TradingCash, Reserved=0,
//     Settled=0, Bank=BankCash. This is the ONE time the split is
//     synthesized — from now on it evolves in memory + is mirrored to
//     Redis. crash_recovery_reload restores the evolved split, it does
//     NOT re-synthesize.
//  3. Build *Account with empty Positions (born flat; positions never
//     persist to Postgres) and PersistOnClose=true (the canonical/
//     on-demand storage difference lives ONLY on this boolean).
//  4. Assemble the initial SnapshotPayload (cash only — no positions, no
//     reservations at birth).
//  5. WriteSnapshot — the initial Redis seed that read_balance_snapshot
//     (16) and the hot-path reads (14/15) ride from here on. Canonical
//     failure hard-fails the session start (a session with no snapshot is
//     unrecoverable on crash; refuse THIS participant's session start
//     rather than run silently-unrecoverable). Durable account untouched.
//
// Returns LoadedAccount so session_save has the OwnerType / OwnerID it
// needs at teardown without those leaking into account.Account.
func (service *Service) SessionLoad(ctx context.Context, sessionID string, accountID account.AccountID) (LoadedAccount, error) {
	return service.loadSession(ctx, sessionID, accountID, Canonical)
}

// loadSession is the shared implementation for SessionLoad (canonical) and
// OndemandSessionLoad (D1). The two flows are identical except for
// (a) PersistOnClose (true for canonical, false for on-demand) and
// (b) the WriteSnapshot failure policy (canonical hard-fails; on-demand
// tolerates — implemented in Stage D). This helper is called by both.
func (service *Service) loadSession(ctx context.Context, sessionID string, accountID account.AccountID, sessionType SessionType) (LoadedAccount, error) {
	slog.Info("lifecycle: session_load begin",
		"session_id", sessionID,
		"account_id", accountID,
		"session_type", sessionType,
	)

	durable, err := service.persist.LoadAccount(ctx, string(accountID))
	if err != nil {
		if errors.Is(err, persistence.ErrNoAccount) {
			slog.Warn("lifecycle: session_load ErrNoAccount",
				"session_id", sessionID,
				"account_id", accountID,
			)
			return LoadedAccount{}, ErrNoAccountFor{AccountID: accountID}
		}
		slog.Error("lifecycle: session_load ErrLoadFailed",
			"session_id", sessionID,
			"account_id", accountID,
			"cause", err,
		)
		return LoadedAccount{}, ErrLoadFailedFor{AccountID: accountID, Cause: err}
	}

	loaded := hydrateFreshAccount(durable, sessionType)

	// Assemble the initial snapshot (cash only — born-flat account has no
	// positions and no reservations). The payload mirrors what
	// account.Account holds right now, verbatim.
	initialPayload := persistence.SnapshotPayload{
		SessionID: sessionID,
		AccountID: string(loaded.Account.ID),
		Cash: persistence.CashSnap{
			Available:    loaded.Account.Cash.Available,
			Reserved:     loaded.Account.Cash.Reserved,
			Settled:      loaded.Account.Cash.Settled,
			BankCash:     loaded.Account.Cash.Bank,
			DripActive:   loaded.Account.DripActive,
			DripProgress: loaded.Account.DripProgress,
		},
		// Positions, Reservations: nil at birth
	}

	if snapshotErr := service.persist.WriteSnapshot(ctx, initialPayload); snapshotErr != nil {
		// Canonical: hard-fail — a session with no snapshot is
		// unrecoverable on crash (doc §3 step 6). On-demand: the
		// wrapper (Stage D) intercepts this and log-and-continues; the
		// tolerance branch does not live in this shared helper.
		if sessionType == Canonical {
			slog.Error("lifecycle: session_load ErrSnapshotWriteFailed",
				"session_id", sessionID,
				"account_id", accountID,
				"cause", snapshotErr,
			)
			return LoadedAccount{}, ErrSnapshotWriteFailedFor{
				SessionID: sessionID,
				AccountID: accountID,
				Cause:     snapshotErr,
			}
		}
		// On-demand tolerance path — return the LoadedAccount with the
		// in-memory copy, no error, and a warning log. Doc §4 step 6:
		// "an on-demand crash-loss costs nothing (never writes back,
		// isn't a record)."
		slog.Warn("lifecycle: ondemand_session_load snapshot write failed (tolerated)",
			"session_id", sessionID,
			"account_id", accountID,
			"cause", snapshotErr,
		)
		return loaded, nil
	}

	slog.Info("lifecycle: session_load ok",
		"session_id", sessionID,
		"account_id", accountID,
		"session_type", sessionType,
		"available", loaded.Account.Cash.Available,
		"bank", loaded.Account.Cash.Bank,
	)
	return loaded, nil
}

// hydrateFreshAccount synthesizes the fresh in-memory split from a
// DurableAccount per doc §3 step 3. Called by both canonical
// (SessionLoad) and on-demand (OndemandSessionLoad, Stage D) load. The
// resulting Account is born flat (empty Positions map) and tagged with
// the caller's PersistOnClose policy. Identity fields (OwnerType,
// OwnerID) travel out in the LoadedAccount wrapper — they don't live on
// account.Account itself.
//
// This is the ONE synthesis point — crash_recovery_reload (F1) restores
// an already-evolved split verbatim and never calls this helper.
func hydrateFreshAccount(durable persistence.DurableAccount, sessionType SessionType) LoadedAccount {
	live := &account.Account{
		ID: durable.AccountID,
		Cash: account.Cash{
			Available: durable.TradingCash, // signed; may be negative — pass through
			Reserved:  0,                   // fresh session — nothing locked
			Settled:   0,                   // fresh session — nothing realized
			Bank:      durable.BankCash,
		},
		Positions:      make(map[contract.InstrumentID]*account.Position),
		PersistOnClose: sessionType == Canonical,
		DripActive:     durable.DripActive,
		DripProgress:   durable.DripProgress,
	}
	return LoadedAccount{
		Account:   live,
		OwnerType: ownerTypeFromColumn(durable.OwnerType),
		OwnerID:   durable.OwnerID,
	}
}

// ownerTypeFromColumn parses the accounts.owner_type TEXT column value
// back into the enum. Unknown strings map to a sentinel value the caller
// can spot (OwnerType(-1)) — persistence should never return one, but if
// it does, session_save's downstream logic must not silently coerce it
// to Agent/Human.
func ownerTypeFromColumn(column string) OwnerType {
	switch column {
	case "agent":
		return OwnerAgent
	case "human":
		return OwnerHuman
	default:
		return OwnerType(-1)
	}
}
