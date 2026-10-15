package funnel

// handleUninstallSession unit tests. Pure in-memory on a shard; no infra,
// no bundle, no started goroutine — a shard constructed directly via
// newShard with a nil bundle is enough. Drives the handler synchronously
// from the test goroutine; safe only because shard.run() is NOT running
// so the test is the sole writer.
//
// Covers:
//   - happy detach: live session for AccountID + matching SessionID →
//     session removed, detached record populated, loadedByExchange entry
//     retained, reply nil
//   - no-live: AccountID absent from sessions → reply ErrNoLiveSession,
//     maps untouched
//   - SessionID mismatch: live session but different SessionID → reply
//     ErrSessionMismatch, live session preserved
//   - dispatch wiring: a routed UninstallSession through a running
//     broker lands on the owning shard and performs the detach

import (
	"errors"
	"testing"
	"time"

	"simulator/economy/account"
	"simulator/economy/capstone"
)

// Happy detach: live session + matching SessionID.
//
//   - shard.sessions[account] removed
//   - shard.detached[account] populated with the LoadedAccount + the
//     SessionID + the ExchangeID
//   - shard.loadedByExchange[exchangeID] still contains the AccountID
//     (account is still loaded on the shard, just parked)
//   - reply is nil
func TestHandleUninstallSession_HappyDetach(t *testing.T) {
	shardUnderTest := newShard("B-uninstall-test", 4, nil /*bundle*/, nil)

	accountID := account.AccountID("acc-detach")
	loaded := loadedAccountFixture(accountID, true /*canonical*/)
	binding := bindingFixture("sess-detach", "X-canon")
	installReply := make(chan error, 1)
	shardUnderTest.handleInstallSession(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-detach",
		Reply:       installReply,
	})
	if err := <-installReply; err != nil {
		t.Fatalf("precondition install: %v", err)
	}

	uninstallReply := make(chan error, 1)
	shardUnderTest.handleUninstallSession(UninstallSession{
		AccountID: accountID,
		SessionID: "sess-detach",
		Reply:     uninstallReply,
	})

	select {
	case err := <-uninstallReply:
		if err != nil {
			t.Fatalf("uninstall reply: got %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("uninstall reply not sent within 1s")
	}

	if _, present := shardUnderTest.sessions[accountID]; present {
		t.Error("sessions map still contains the account after uninstall")
	}
	parked, present := shardUnderTest.detached[accountID]
	if !present {
		t.Fatal("detached map does not contain the account after uninstall")
	}
	if parked.lastSessionID != "sess-detach" {
		t.Errorf("parked.lastSessionID = %q, want %q", parked.lastSessionID, "sess-detach")
	}
	if parked.exchangeID != "X-canon" {
		t.Errorf("parked.exchangeID = %q, want %q", parked.exchangeID, "X-canon")
	}
	if parked.loaded.Account == nil {
		t.Error("parked.loaded.Account is nil (should carry the whole LoadedAccount envelope)")
	}
	if parked.loaded.Account != nil && parked.loaded.Account.ID != accountID {
		t.Errorf("parked.loaded.Account.ID = %q, want %q", parked.loaded.Account.ID, accountID)
	}
	indexed, present := shardUnderTest.loadedByExchange["X-canon"]
	if !present {
		t.Fatal("loadedByExchange[X-canon] missing after uninstall (detach-keep must retain the index)")
	}
	if _, inIndex := indexed[accountID]; !inIndex {
		t.Errorf("loadedByExchange[X-canon] does not contain the account after uninstall")
	}
}

// AccountID has no live session → reply ErrNoLiveSession, maps unchanged.
func TestHandleUninstallSession_NoLiveSession(t *testing.T) {
	shardUnderTest := newShard("B-uninstall-nolive", 4, nil, nil)

	reply := make(chan error, 1)
	shardUnderTest.handleUninstallSession(UninstallSession{
		AccountID: "acc-missing",
		SessionID: "sess-missing",
		Reply:     reply,
	})

	err := <-reply
	if !errors.Is(err, ErrNoLiveSession) {
		t.Fatalf("reply = %v, want errors.Is ErrNoLiveSession", err)
	}
	if len(shardUnderTest.sessions) != 0 {
		t.Errorf("sessions map mutated on no-live uninstall: %d entries", len(shardUnderTest.sessions))
	}
	if len(shardUnderTest.detached) != 0 {
		t.Errorf("detached map mutated on no-live uninstall: %d entries", len(shardUnderTest.detached))
	}
}

// Live session's binding.SessionID ≠ cmd.SessionID → reply
// ErrSessionMismatch, live session preserved intact.
func TestHandleUninstallSession_SessionMismatch(t *testing.T) {
	shardUnderTest := newShard("B-uninstall-mismatch", 4, nil, nil)

	accountID := account.AccountID("acc-mismatch")
	loaded := loadedAccountFixture(accountID, true)
	binding := bindingFixture("sess-live", "X-canon")
	installReply := make(chan error, 1)
	shardUnderTest.handleInstallSession(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-mismatch",
		Reply:       installReply,
	})
	if err := <-installReply; err != nil {
		t.Fatalf("precondition install: %v", err)
	}

	// Different SessionID.
	reply := make(chan error, 1)
	shardUnderTest.handleUninstallSession(UninstallSession{
		AccountID: accountID,
		SessionID: "sess-stale",
		Reply:     reply,
	})

	err := <-reply
	if !errors.Is(err, ErrSessionMismatch) {
		t.Fatalf("reply = %v, want errors.Is ErrSessionMismatch", err)
	}
	live, present := shardUnderTest.sessions[accountID]
	if !present {
		t.Fatal("live session was removed on mismatch (should be preserved)")
	}
	if live.binding.SessionID != "sess-live" {
		t.Errorf("live session binding.SessionID = %q, want %q", live.binding.SessionID, "sess-live")
	}
	if len(shardUnderTest.detached) != 0 {
		t.Errorf("detached map populated on mismatch: %d entries (should be empty)", len(shardUnderTest.detached))
	}
}

// Dispatch wiring: a routed UninstallSession through a running broker
// lands on the owning shard and performs the detach — proves the
// dispatch case in shard_handlers.go is correctly wired and the shard's
// serial loop processes the command.
func TestHandleUninstallSession_DispatchViaRunningShard(t *testing.T) {
	brokerUnderTest := NewBroker("B-uninstall-dispatch", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{}, nil)
	brokerUnderTest.Start()
	t.Cleanup(brokerUnderTest.Stop)

	accountID := account.AccountID("acc-dispatch")
	loaded := loadedAccountFixture(accountID, true)
	binding := bindingFixture("sess-dispatch", "X-dispatch")

	installReply := make(chan error, 1)
	brokerUnderTest.route(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-dispatch",
		Reply:       installReply,
	})
	if err := <-installReply; err != nil {
		t.Fatalf("precondition install: %v", err)
	}

	uninstallReply := make(chan error, 1)
	brokerUnderTest.route(UninstallSession{
		AccountID: accountID,
		SessionID: "sess-dispatch",
		Reply:     uninstallReply,
	})
	select {
	case err := <-uninstallReply:
		if err != nil {
			t.Fatalf("dispatched uninstall: got %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatched uninstall: no reply within 2s — dispatch wiring broken")
	}
}
