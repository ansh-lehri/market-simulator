package lifecycle

import (
	"context"
	"errors"

	"simulator/economy/account"
	"simulator/economy/persistence"
)

// ReadBalanceSnapshot is LAYER_5_1_READ_OPERATIONS item 16
// (read_balance_snapshot): the "first read" that seeds in-memory working
// state from the Redis session snapshot. NOT a hot-path read (the hot
// path is account.ReadAvailableCash / ReadAvailablePosition, both
// in-memory) — this is the boundary read the session bootstrap primes.
//
// Contract (per doc §5):
//   - success           → *account.Cash{Available, Reserved, Settled, Bank}
//                         built from the payload's CashSnap; Available
//                         passes through signed (may be negative under
//                         Option B).
//   - snapshot missing  → ErrSnapshotMissingFor (wraps ErrSnapshotMissing).
//                         Hard stop; never fabricate a balance. Means
//                         session bootstrap ran out of order (read
//                         before session_load wrote), or the session was
//                         already discarded.
//   - transport failure → ErrLoadFailedFor (wraps ErrLoadFailed + cause).
//                         Redis unreachable / decode error. Never seed a
//                         session from a failed money read.
//
// Runs once at session init, off the hot path — one Redis GET.
func (service *Service) ReadBalanceSnapshot(ctx context.Context, sessionID string, accountID account.AccountID) (*account.Cash, error) {
	payload, err := service.persist.ReadSnapshot(ctx, sessionID, string(accountID))
	switch {
	case errors.Is(err, persistence.ErrSnapshotMissing):
		return nil, ErrSnapshotMissingFor{SessionID: sessionID, AccountID: accountID}
	case err != nil:
		return nil, ErrLoadFailedFor{AccountID: accountID, Cause: err}
	}

	// Hydrate the in-memory working split directly from the snapshot — no
	// reconciliation math (the snapshot already holds the live split;
	// Postgres holds only trading_cash and is not touched by this read).
	return &account.Cash{
		Available: payload.Cash.Available, // signed; may be negative
		Reserved:  payload.Cash.Reserved,
		Settled:   payload.Cash.Settled,
		Bank:      payload.Cash.BankCash,
	}, nil
}
