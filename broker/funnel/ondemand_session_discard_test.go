package funnel

// handleDiscardSession unit tests. Pure in-memory on a shard (plus the
// injected fake discarder's bookkeeping) — same drive-the-handler
// pattern install_session_test uses. Covers:
//
//   - idempotent no-session reply (ErrNoLiveSession)
//   - stale SessionID refused (ErrSessionMismatch); live binding intact
//   - canonical account reaching the discard path panics (defense in
//     depth against a wiring bug where the loader's PersistOnClose and
//     the end-session Mode disagree)
//   - happy drop: sessions pruned, per-exchange index pruned (map
//     deleted when it drains to zero), discarder called exactly once
//   - happy drop with another account for the same exchange: index map
//     stays, only the discarded entry removed
//   - discarder error surfaces as a wrapped ErrDiscardFailed; shard
//     maps are STILL cleaned (forward-progress on the shard side)
//   - missing discarder (nil wiring) surfaces ErrDiscardFailed; maps
//     still cleaned
//
// The direct-handler approach is deliberate: driving the discarder-
// panics case through the running broker goroutine would crash the test
// binary. Calling the handler on the test goroutine lets a defer/recover
// observe the panic cleanly.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"simulator/economy/account"
	"simulator/economy/lifecycle"
)

// --- fake sessionDiscarder --------------------------------------------

// fakeSessionDiscarder records every SessionDiscard call and lets each
// test program a per-call outcome. Mirrors fakeSessionSaver's shape so
// the two interface-coverage tests look symmetric. Safe for the
// end-to-end broker tests (lock guards the slice).
type fakeSessionDiscarder struct {
	callLock      sync.Mutex
	calls         []fakeDiscardCall
	errByAccount  map[string]error
	defaultError  error
	alwaysSucceed bool
}

type fakeDiscardCall struct {
	SessionID string
	AccountID account.AccountID
}

func (fake *fakeSessionDiscarder) SessionDiscard(ctx context.Context, sessionID string, loaded lifecycle.LoadedAccount) error {
	fake.callLock.Lock()
	fake.calls = append(fake.calls, fakeDiscardCall{SessionID: sessionID, AccountID: loaded.Account.ID})
	if fake.alwaysSucceed {
		fake.callLock.Unlock()
		return nil
	}
	if override, present := fake.errByAccount[string(loaded.Account.ID)]; present {
		fake.callLock.Unlock()
		return override
	}
	returnedErr := fake.defaultError
	fake.callLock.Unlock()
	return returnedErr
}

func (fake *fakeSessionDiscarder) callCount() int {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	return len(fake.calls)
}

// installOnDemandSessionDirect seeds the shard's sessions + per-exchange
// index with one on-demand (PersistOnClose=false) session. Shortcut for
// the discard tests — bypasses the install handler because every one of
// these tests is about teardown, not install, and the install handler
// has its own coverage.
func installOnDemandSessionDirect(
	shardUnderTest *shard,
	accountID account.AccountID,
	sessionID SessionID,
	exchangeID ExchangeID,
) {
	loaded := loadedAccountFixture(accountID, false /*on-demand*/)
	binding := bindingFixture(sessionID, exchangeID)
	shardUnderTest.sessions[accountID] = &ownedSession{
		account:     loaded,
		binding:     binding,
		participant: "P-" + ParticipantID(accountID),
		instruments: permissiveInstruments{},
	}
	accountsForExchange := shardUnderTest.loadedByExchange[exchangeID]
	if accountsForExchange == nil {
		accountsForExchange = make(map[AccountID]struct{})
		shardUnderTest.loadedByExchange[exchangeID] = accountsForExchange
	}
	accountsForExchange[accountID] = struct{}{}
}

// No live session for the AccountID → reply ErrNoLiveSession, no
// discarder call. Idempotent-on-repeat contract.
func TestHandleDiscardSession_NoLiveSession(t *testing.T) {
	shardUnderTest := newShard("B-discard-nolive", 4, nil, nil)
	discarderFake := &fakeSessionDiscarder{alwaysSucceed: true}
	shardUnderTest.discarder = discarderFake

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: "acc-missing",
		SessionID: "sess-anything",
		Reply:     reply,
	})

	select {
	case err := <-reply:
		if !errors.Is(err, ErrNoLiveSession) {
			t.Fatalf("reply: got %v, want errors.Is ErrNoLiveSession", err)
		}
	default:
		t.Fatal("reply: no value sent — handler did not send on Reply")
	}
	if discarderFake.callCount() != 0 {
		t.Errorf("discarder called %d times; want 0 (no-session short-circuits)", discarderFake.callCount())
	}
}

// Live session exists but the command's SessionID does not match →
// reply ErrSessionMismatch, no discarder call, live binding unchanged.
func TestHandleDiscardSession_SessionMismatchKeepsLiveBinding(t *testing.T) {
	shardUnderTest := newShard("B-discard-mismatch", 4, nil, nil)
	discarderFake := &fakeSessionDiscarder{alwaysSucceed: true}
	shardUnderTest.discarder = discarderFake

	accountID := account.AccountID("acc-mismatch")
	liveSessionID := SessionID("sess-live")
	liveExchangeID := ExchangeID("X-mismatch")
	installOnDemandSessionDirect(shardUnderTest, accountID, liveSessionID, liveExchangeID)

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: accountID,
		SessionID: "sess-stale",
		Reply:     reply,
	})

	err := <-reply
	if !errors.Is(err, ErrSessionMismatch) {
		t.Fatalf("reply: got %v, want errors.Is ErrSessionMismatch", err)
	}
	if discarderFake.callCount() != 0 {
		t.Errorf("discarder called %d times; want 0 (mismatch short-circuits)", discarderFake.callCount())
	}
	if _, stillPresent := shardUnderTest.sessions[accountID]; !stillPresent {
		t.Error("sessions[acc]: dropped on mismatch; want preserved (live binding must survive a stale teardown)")
	}
	if _, stillIndexed := shardUnderTest.loadedByExchange[liveExchangeID][accountID]; !stillIndexed {
		t.Error("loadedByExchange: entry dropped on mismatch; want preserved")
	}
}

// A canonical account (PersistOnClose=true) reaching the on-demand
// discard path is a wiring bug — a silent discard would delete the
// Redis snapshot that still carries real money progress. Dev-fatal
// panic is the correct response (matches lifecycle.SessionDiscard's
// own guard).
func TestHandleDiscardSession_CanonicalAccountPanics(t *testing.T) {
	shardUnderTest := newShard("B-discard-canonpanic", 4, nil, nil)
	discarderFake := &fakeSessionDiscarder{alwaysSucceed: true}
	shardUnderTest.discarder = discarderFake

	accountID := account.AccountID("acc-canon-misrouted")
	// Install as canonical (PersistOnClose=true) directly so we can
	// drive the on-demand discard against it without going through the
	// install validators (which would reject the on-demand teardown of
	// a canonical account further upstream in a real wiring).
	loaded := loadedAccountFixture(accountID, true /*canonical*/)
	binding := bindingFixture("sess-canon", "X-canon")
	shardUnderTest.sessions[accountID] = &ownedSession{
		account:     loaded,
		binding:     binding,
		participant: "P-canon",
		instruments: permissiveInstruments{},
	}

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on canonical account in discard path; got none")
		}
		// The handler maps got touched BEFORE the panic? No — the panic
		// check runs before any map mutation. Assert the live binding
		// survived.
		if _, stillPresent := shardUnderTest.sessions[accountID]; !stillPresent {
			t.Error("sessions[acc]: dropped before panic; want preserved (panic must precede mutation)")
		}
		if discarderFake.callCount() != 0 {
			t.Errorf("discarder called %d times before panic; want 0", discarderFake.callCount())
		}
	}()

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: accountID,
		SessionID: "sess-canon",
		Reply:     reply,
	})
	t.Fatal("unreachable — the handler must have panicked")
}

// Happy drop with one account for one exchange: sessions pruned,
// per-exchange index map DELETED (drains to zero), discarder called
// exactly once with the expected ids.
func TestHandleDiscardSession_HappyDropPrunesEverything(t *testing.T) {
	shardUnderTest := newShard("B-discard-happy", 4, nil, nil)
	discarderFake := &fakeSessionDiscarder{alwaysSucceed: true}
	shardUnderTest.discarder = discarderFake

	accountID := account.AccountID("acc-happy-discard")
	sessionID := SessionID("sess-happy-discard")
	exchangeID := ExchangeID("X-happy-discard")
	installOnDemandSessionDirect(shardUnderTest, accountID, sessionID, exchangeID)

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: accountID,
		SessionID: sessionID,
		Reply:     reply,
	})

	if err := <-reply; err != nil {
		t.Fatalf("reply: got %v, want nil", err)
	}
	if _, stillPresent := shardUnderTest.sessions[accountID]; stillPresent {
		t.Error("sessions[acc]: still present; want dropped")
	}
	if _, stillIndexed := shardUnderTest.loadedByExchange[exchangeID]; stillIndexed {
		t.Error("loadedByExchange[exchange]: map still present; want deleted (last entry drained)")
	}
	if discarderFake.callCount() != 1 {
		t.Fatalf("discarder call count = %d, want 1", discarderFake.callCount())
	}
	gotCall := discarderFake.calls[0]
	if gotCall.AccountID != accountID {
		t.Errorf("discarder call AccountID = %q, want %q", gotCall.AccountID, accountID)
	}
	if gotCall.SessionID != string(sessionID) {
		t.Errorf("discarder call SessionID = %q, want %q", gotCall.SessionID, sessionID)
	}
}

// Two accounts share one exchange. Discarding one drops that one and
// prunes its index entry, but the exchange's index map stays (the other
// account still carries it). Guard against over-aggressive cleanup.
func TestHandleDiscardSession_HappyDropPreservesOtherAccountsForExchange(t *testing.T) {
	shardUnderTest := newShard("B-discard-happy-shared", 4, nil, nil)
	discarderFake := &fakeSessionDiscarder{alwaysSucceed: true}
	shardUnderTest.discarder = discarderFake

	exchangeID := ExchangeID("X-shared")
	targetAccountID := account.AccountID("acc-target")
	otherAccountID := account.AccountID("acc-other")
	installOnDemandSessionDirect(shardUnderTest, targetAccountID, "sess-target", exchangeID)
	installOnDemandSessionDirect(shardUnderTest, otherAccountID, "sess-other", exchangeID)

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: targetAccountID,
		SessionID: "sess-target",
		Reply:     reply,
	})
	if err := <-reply; err != nil {
		t.Fatalf("reply: got %v, want nil", err)
	}

	if _, stillPresent := shardUnderTest.sessions[targetAccountID]; stillPresent {
		t.Error("sessions[target]: still present; want dropped")
	}
	if _, stillPresent := shardUnderTest.sessions[otherAccountID]; !stillPresent {
		t.Error("sessions[other]: dropped; want preserved")
	}
	accountsForExchange, mapStillPresent := shardUnderTest.loadedByExchange[exchangeID]
	if !mapStillPresent {
		t.Fatal("loadedByExchange[exchange]: map deleted; want preserved (other account still bound)")
	}
	if _, stillIndexed := accountsForExchange[targetAccountID]; stillIndexed {
		t.Error("loadedByExchange: target still indexed; want pruned")
	}
	if _, stillIndexed := accountsForExchange[otherAccountID]; !stillIndexed {
		t.Error("loadedByExchange: other no longer indexed; want preserved")
	}
}

// SessionDiscard surfaces an error (Redis unreachable, e.g.) → reply
// wraps ErrDiscardFailed AND the underlying cause (both reachable via
// errors.Is). Shard maps are ALREADY cleaned — the ephemeral account is
// gone regardless, matching the forward-progress contract.
func TestHandleDiscardSession_DiscarderErrorSurfacesButMapsStillPruned(t *testing.T) {
	shardUnderTest := newShard("B-discard-err", 4, nil, nil)
	scriptedErr := errors.New("scripted redis delete failure")
	discarderFake := &fakeSessionDiscarder{defaultError: scriptedErr}
	shardUnderTest.discarder = discarderFake

	accountID := account.AccountID("acc-discard-err")
	sessionID := SessionID("sess-discard-err")
	exchangeID := ExchangeID("X-discard-err")
	installOnDemandSessionDirect(shardUnderTest, accountID, sessionID, exchangeID)

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: accountID,
		SessionID: sessionID,
		Reply:     reply,
	})

	gotErr := <-reply
	if !errors.Is(gotErr, ErrDiscardFailed) {
		t.Fatalf("reply: got %v, want errors.Is ErrDiscardFailed", gotErr)
	}
	if !errors.Is(gotErr, scriptedErr) {
		t.Errorf("reply: got %v, want errors.Is scriptedErr (underlying cause must be reachable)", gotErr)
	}
	if _, stillPresent := shardUnderTest.sessions[accountID]; stillPresent {
		t.Error("sessions[acc]: still present after discard error; want dropped (shard maps clean regardless of Redis)")
	}
	if _, stillIndexed := shardUnderTest.loadedByExchange[exchangeID]; stillIndexed {
		t.Error("loadedByExchange: entry still present; want deleted")
	}
	if discarderFake.callCount() != 1 {
		t.Errorf("discarder call count = %d, want 1 (one attempt even on failure)", discarderFake.callCount())
	}
}

// Shard built without a lifecycle wiring (nil discarder) still cleans
// its maps and surfaces ErrDiscardFailed — same contract as a real
// discard error. Mirrors the shard-saver-missing branch in
// handleFinalizeMarketOnShard.
func TestHandleDiscardSession_MissingDiscarderSurfacesCategoryError(t *testing.T) {
	shardUnderTest := newShard("B-discard-nildisc", 4, nil, nil)
	// discarder deliberately NOT set — simulates a bundle-less shard
	// reaching the discard path.

	accountID := account.AccountID("acc-nil-discarder")
	sessionID := SessionID("sess-nil-discarder")
	exchangeID := ExchangeID("X-nil-discarder")
	installOnDemandSessionDirect(shardUnderTest, accountID, sessionID, exchangeID)

	reply := make(chan error, 1)
	shardUnderTest.handleDiscardSession(DiscardSession{
		Ctx:       context.Background(),
		AccountID: accountID,
		SessionID: sessionID,
		Reply:     reply,
	})

	gotErr := <-reply
	if !errors.Is(gotErr, ErrDiscardFailed) {
		t.Fatalf("reply: got %v, want errors.Is ErrDiscardFailed", gotErr)
	}
	if _, stillPresent := shardUnderTest.sessions[accountID]; stillPresent {
		t.Error("sessions[acc]: still present after nil-discarder path; want dropped")
	}
	if _, stillIndexed := shardUnderTest.loadedByExchange[exchangeID]; stillIndexed {
		t.Error("loadedByExchange: entry still present; want deleted")
	}
}
