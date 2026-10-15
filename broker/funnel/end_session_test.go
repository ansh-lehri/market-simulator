package funnel

// EndSession unit tests. Two concerns:
//
//   - off-shard validation (empty AccountID / empty SessionID /
//     unsupported Mode / unknown Mode) rejects BEFORE any shard
//     routing — verified by driving against a NON-STARTED broker so
//     any accidental route() would block or crash.
//   - canonical end-to-end: start the broker, install a session,
//     EndSession returns nil, the shard moves state from sessions to
//     detached, and a second EndSession with the same SessionID now
//     reports ErrNoLiveSession (idempotency on repeat).
//
// Ctx-timeout is covered by the dedicated reply_test.go in the shipped
// package, which drives waitReply deterministically — re-testing it
// against a live shard would be timing-dependent.

import (
	"context"
	"errors"
	"testing"
	"time"

	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/lifecycle"
)

// Empty AccountID rejected off-shard. Running a non-started broker
// would hang on route(); a reject before route() must return cleanly
// without any routing attempt.
func TestEndSession_EmptyAccountIDRejectedOffShard(t *testing.T) {
	brokerUnderTest := NewBroker("B-endsession-badacc", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)
	// Deliberately NOT Start()ed — if we accidentally reached route(),
	// the send onto an empty inbox would block forever and the test
	// would time out.

	err := brokerUnderTest.EndSession(context.Background(), SessionEndCmd{
		AccountID: "",
		SessionID: "sess-x",
		Mode:      lifecycle.Canonical,
	})
	if !errors.Is(err, ErrBadAccountID) {
		t.Fatalf("err = %v, want errors.Is ErrBadAccountID", err)
	}
}

func TestEndSession_EmptySessionIDRejectedOffShard(t *testing.T) {
	brokerUnderTest := NewBroker("B-endsession-badsid", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)

	err := brokerUnderTest.EndSession(context.Background(), SessionEndCmd{
		AccountID: "acc-x",
		SessionID: "",
		Mode:      lifecycle.Canonical,
	})
	if !errors.Is(err, ErrBadSessionID) {
		t.Fatalf("err = %v, want errors.Is ErrBadSessionID", err)
	}
}

// OnDemand mode is intentionally unimplemented this phase; the on-demand
// teardown phase replaces this stub with the discard path.
func TestEndSession_OnDemandReturnsModeNotImplemented(t *testing.T) {
	brokerUnderTest := NewBroker("B-endsession-ondemand", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)

	err := brokerUnderTest.EndSession(context.Background(), SessionEndCmd{
		AccountID: "acc-x",
		SessionID: "sess-x",
		Mode:      lifecycle.OnDemand,
	})
	if !errors.Is(err, ErrModeNotImplemented) {
		t.Fatalf("err = %v, want errors.Is ErrModeNotImplemented", err)
	}
}

// A Mode value outside Canonical | OnDemand is a programming error at
// the caller. Returns a wrapped-string error (not a category sentinel)
// so the caller's error log carries the raw int value.
func TestEndSession_UnknownModeReturnsError(t *testing.T) {
	brokerUnderTest := NewBroker("B-endsession-unknown", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)

	err := brokerUnderTest.EndSession(context.Background(), SessionEndCmd{
		AccountID: "acc-x",
		SessionID: "sess-x",
		Mode:      lifecycle.SessionType(99),
	})
	if err == nil {
		t.Fatalf("err = nil, want unknown-mode error")
	}
	// Must NOT be any of the typed sentinels — those cover the known
	// branches; unknown-Mode is a bare fmt.Errorf.
	for _, sentinel := range []error{ErrBadAccountID, ErrBadSessionID, ErrModeNotImplemented} {
		if errors.Is(err, sentinel) {
			t.Errorf("err = %v, matched %v — unknown-mode must be its own category", err, sentinel)
		}
	}
}

// Canonical happy path through a running broker: install, EndSession,
// confirm detach via side-effects. The sessions map is unexported so we
// probe via a repeat EndSession — once the session is uninstalled a
// second end returns ErrNoLiveSession (idempotency), proving the first
// one landed on the shard.
func TestEndSession_CanonicalHappyAndRepeatReturnsNoLiveSession(t *testing.T) {
	brokerUnderTest := NewBroker("B-endsession-happy", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)
	brokerUnderTest.Start()
	t.Cleanup(brokerUnderTest.Stop)

	accountID := account.AccountID("acc-happy-end")
	loaded := loadedAccountFixture(accountID, true /*canonical*/)
	binding := bindingFixture("sess-happy-end", "X-happy-end")

	installReply := make(chan error, 1)
	brokerUnderTest.route(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-happy-end",
		Reply:       installReply,
	})
	if err := <-installReply; err != nil {
		t.Fatalf("precondition install: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := brokerUnderTest.EndSession(ctx, SessionEndCmd{
		AccountID: accountID,
		SessionID: "sess-happy-end",
		Mode:      lifecycle.Canonical,
	}); err != nil {
		t.Fatalf("first EndSession: %v, want nil", err)
	}

	// Repeat with same SessionID — the session is now parked in
	// `detached`, not live in `sessions`, so uninstall reports
	// ErrNoLiveSession.
	err := brokerUnderTest.EndSession(ctx, SessionEndCmd{
		AccountID: accountID,
		SessionID: "sess-happy-end",
		Mode:      lifecycle.Canonical,
	})
	if !errors.Is(err, ErrNoLiveSession) {
		t.Fatalf("second EndSession: got %v, want errors.Is ErrNoLiveSession (idempotent)", err)
	}
}

// EndSession with a stale SessionID on an account whose live binding
// carries a different SessionID returns ErrSessionMismatch. Proves the
// mismatch guard rides end-to-end through EndSession, not just the
// shard handler.
func TestEndSession_SessionMismatchViaRunningBroker(t *testing.T) {
	brokerUnderTest := NewBroker("B-endsession-mismatch", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)
	brokerUnderTest.Start()
	t.Cleanup(brokerUnderTest.Stop)

	accountID := account.AccountID("acc-mismatch-end")
	loaded := loadedAccountFixture(accountID, true)
	binding := bindingFixture("sess-live-end", "X-mismatch-end")

	installReply := make(chan error, 1)
	brokerUnderTest.route(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-mismatch-end",
		Reply:       installReply,
	})
	if err := <-installReply; err != nil {
		t.Fatalf("precondition install: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := brokerUnderTest.EndSession(ctx, SessionEndCmd{
		AccountID: accountID,
		SessionID: "sess-stale-end",
		Mode:      lifecycle.Canonical,
	})
	if !errors.Is(err, ErrSessionMismatch) {
		t.Fatalf("EndSession = %v, want errors.Is ErrSessionMismatch", err)
	}
}
