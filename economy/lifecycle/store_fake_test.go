package lifecycle

import (
	"context"

	"simulator/economy/persistence"
)

// fakeStore is a hand-written test double satisfying the store interface.
// Each method reads its return values from configurable fields and records
// the call so tests can assert both the return path AND the store's usage
// pattern (call count, argument capture, sequence).
//
// Design notes:
//   - Deliberately hand-written (not code-generated) so the test double's
//     surface stays visibly aligned with the store interface. Any drift is
//     a compile-time break at the `var _ store = (*fakeStore)(nil)` guard
//     below.
//   - No mutex — lifecycle tests run single-threaded per economy/CLAUDE.md.
//   - No default returns — the zero-value error is nil and the zero-value
//     DurableAccount / SnapshotPayload are the natural "empty" results. A
//     test that wants a specific return sets the corresponding fields.
type fakeStore struct {
	// Return values (per method). Tests set the *ErrFn or *Ret* fields
	// they care about; unset ones stay at their zero value.
	OpenAccountFn     func(ctx context.Context, durable persistence.DurableAccount) (string, error)
	LoadAccountFn     func(ctx context.Context, accountID string) (persistence.DurableAccount, error)
	SaveAccountFn     func(ctx context.Context, durable persistence.DurableAccount) error
	WriteSnapshotFn   func(ctx context.Context, payload persistence.SnapshotPayload) error
	ReadSnapshotFn    func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error)
	DeleteSnapshotFn  func(ctx context.Context, sessionID, accountID string) error

	// Call logs — every call appends a record; tests inspect these to
	// assert "call happened / did not happen / happened once with X args".
	OpenAccountCalls    []persistence.DurableAccount
	LoadAccountCalls    []string
	SaveAccountCalls    []persistence.DurableAccount
	WriteSnapshotCalls  []persistence.SnapshotPayload
	ReadSnapshotCalls   []readSnapshotCall
	DeleteSnapshotCalls []deleteSnapshotCall
}

type readSnapshotCall struct{ SessionID, AccountID string }
type deleteSnapshotCall struct{ SessionID, AccountID string }

func (fake *fakeStore) OpenAccount(ctx context.Context, durable persistence.DurableAccount) (string, error) {
	fake.OpenAccountCalls = append(fake.OpenAccountCalls, durable)
	if fake.OpenAccountFn != nil {
		return fake.OpenAccountFn(ctx, durable)
	}
	return "", nil
}

func (fake *fakeStore) LoadAccount(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
	fake.LoadAccountCalls = append(fake.LoadAccountCalls, accountID)
	if fake.LoadAccountFn != nil {
		return fake.LoadAccountFn(ctx, accountID)
	}
	return persistence.DurableAccount{}, nil
}

func (fake *fakeStore) SaveAccount(ctx context.Context, durable persistence.DurableAccount) error {
	fake.SaveAccountCalls = append(fake.SaveAccountCalls, durable)
	if fake.SaveAccountFn != nil {
		return fake.SaveAccountFn(ctx, durable)
	}
	return nil
}

func (fake *fakeStore) WriteSnapshot(ctx context.Context, payload persistence.SnapshotPayload) error {
	fake.WriteSnapshotCalls = append(fake.WriteSnapshotCalls, payload)
	if fake.WriteSnapshotFn != nil {
		return fake.WriteSnapshotFn(ctx, payload)
	}
	return nil
}

func (fake *fakeStore) ReadSnapshot(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
	fake.ReadSnapshotCalls = append(fake.ReadSnapshotCalls, readSnapshotCall{SessionID: sessionID, AccountID: accountID})
	if fake.ReadSnapshotFn != nil {
		return fake.ReadSnapshotFn(ctx, sessionID, accountID)
	}
	return persistence.SnapshotPayload{SessionID: sessionID, AccountID: accountID}, nil
}

func (fake *fakeStore) DeleteSnapshot(ctx context.Context, sessionID, accountID string) error {
	fake.DeleteSnapshotCalls = append(fake.DeleteSnapshotCalls, deleteSnapshotCall{SessionID: sessionID, AccountID: accountID})
	if fake.DeleteSnapshotFn != nil {
		return fake.DeleteSnapshotFn(ctx, sessionID, accountID)
	}
	return nil
}

// Compile-time assertion: fakeStore satisfies the same store interface as
// *persistence.Component. If the interface grows a method, this line
// breaks and the fake must be extended in the same commit.
var _ store = (*fakeStore)(nil)
