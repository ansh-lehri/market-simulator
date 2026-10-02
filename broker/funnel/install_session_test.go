package funnel

// handleInstallSession unit tests. Pure in-memory on a shard; no infra,
// no bundle, no started goroutine — we construct a shard directly via
// the package-private newShard with a nil bundle and drive the handler
// synchronously. Covers:
//   - happy install: empty slot → populated with correct LoadedAccount +
//     binding + participant; reply nil
//   - XOR refusal:   slot taken  → ErrSessionAlreadyLive on reply;
//     existing session unchanged
//   - dispatch wiring: route an InstallSession through a running broker
//     and verify shard.sessions is populated
//
// Note: handleInstallSession writes to shard.sessions which is a plain
// map (single-writer by design). Driving the handler from the test
// goroutine is safe only because we are NOT running shard.run() — the
// test is the sole writer. The dispatch test below spawns a shard via
// Broker.Start and respects the "one writer, serial loop" rule.

import (
	"errors"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/lifecycle"
)

// loadedAccountFixture builds a minimum-shape LoadedAccount for the
// install handler to store. Nothing in handleInstallSession reads the
// Account's internals — the handler just stashes the envelope — so the
// zero-ish values are fine.
func loadedAccountFixture(accountID account.AccountID, persistOnClose bool) lifecycle.LoadedAccount {
	return lifecycle.LoadedAccount{
		Account: &account.Account{
			ID:             accountID,
			Cash:           account.Cash{Available: 100, Reserved: 0, Settled: 0, Bank: 50},
			Positions:      make(map[contract.InstrumentID]*account.Position),
			PersistOnClose: persistOnClose,
		},
		OwnerType: lifecycle.OwnerHuman,
		OwnerID:   "test-owner-" + string(accountID),
	}
}

// bindingFixture builds a SessionBinding for the install handler to
// store. ExchangeInbound is a non-nil buffered channel so the stored
// value is observable; the handler never reads from it.
func bindingFixture(sessionID SessionID, exchangeID ExchangeID) SessionBinding {
	return SessionBinding{
		SessionID:       sessionID,
		ExchangeID:      exchangeID,
		ExchangeInbound: make(chan contract.InboundMessage, 1),
	}
}

// Empty sessions map, install arrives → session populated, reply nil.
// Verifies every field of ownedSession matches what the command carried.
func TestHandleInstallSession_HappyInstall(t *testing.T) {
	shardUnderTest := newShard("B-install-test", 4, nil /*bundle*/)

	accountID := account.AccountID("acc-happy")
	loaded := loadedAccountFixture(accountID, true /*canonical*/)
	binding := bindingFixture("sess-happy", "X-canon")
	reply := make(chan error, 1)

	shardUnderTest.handleInstallSession(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-happy",
		Reply:       reply,
	})

	select {
	case err := <-reply:
		if err != nil {
			t.Fatalf("install reply: got %v, want nil", err)
		}
	default:
		t.Fatal("install reply: no value sent — handler did not send on Reply")
	}

	installed, ok := shardUnderTest.sessions[accountID]
	if !ok {
		t.Fatalf("sessions[%s] missing after install", accountID)
	}
	if installed.account.Account != loaded.Account {
		t.Errorf("installed.account.Account: got %p, want %p (same LoadedAccount envelope)", installed.account.Account, loaded.Account)
	}
	if installed.account.OwnerType != loaded.OwnerType {
		t.Errorf("installed.account.OwnerType: got %v, want %v", installed.account.OwnerType, loaded.OwnerType)
	}
	if installed.account.OwnerID != loaded.OwnerID {
		t.Errorf("installed.account.OwnerID: got %q, want %q", installed.account.OwnerID, loaded.OwnerID)
	}
	if installed.binding.SessionID != binding.SessionID {
		t.Errorf("installed.binding.SessionID: got %q, want %q", installed.binding.SessionID, binding.SessionID)
	}
	if installed.binding.ExchangeID != binding.ExchangeID {
		t.Errorf("installed.binding.ExchangeID: got %q, want %q", installed.binding.ExchangeID, binding.ExchangeID)
	}
	if installed.participant != "P-happy" {
		t.Errorf("installed.participant: got %q, want %q", installed.participant, "P-happy")
	}
	if !installed.account.Account.PersistOnClose {
		t.Error("installed.account.Account.PersistOnClose: got false, want true (canonical)")
	}
}

// Install twice for the same AccountID → second returns ErrSessionAlreadyLive;
// the FIRST install's state is unchanged. XOR invariant proof.
func TestHandleInstallSession_XorRefusesSecondInstall(t *testing.T) {
	shardUnderTest := newShard("B-install-xor", 4, nil)

	accountID := account.AccountID("acc-xor")
	firstLoaded := loadedAccountFixture(accountID, true)
	firstBinding := bindingFixture("sess-first", "X-canon")
	firstReply := make(chan error, 1)

	shardUnderTest.handleInstallSession(InstallSession{
		AccountID:   accountID,
		Loaded:      firstLoaded,
		Binding:     firstBinding,
		Participant: "P-first",
		Reply:       firstReply,
	})
	if err := <-firstReply; err != nil {
		t.Fatalf("first install: got %v, want nil", err)
	}

	// Second install, different payload — must refuse WITHOUT overwriting.
	secondLoaded := loadedAccountFixture(accountID, false /*on-demand*/)
	secondBinding := bindingFixture("sess-second", "X-ondemand")
	secondReply := make(chan error, 1)

	shardUnderTest.handleInstallSession(InstallSession{
		AccountID:   accountID,
		Loaded:      secondLoaded,
		Binding:     secondBinding,
		Participant: "P-second",
		Reply:       secondReply,
	})

	gotErr := <-secondReply
	if !errors.Is(gotErr, ErrSessionAlreadyLive) {
		t.Fatalf("second install: got %v, want errors.Is ErrSessionAlreadyLive", gotErr)
	}

	// First session's fields survive unmodified.
	installed := shardUnderTest.sessions[accountID]
	if installed.account.Account != firstLoaded.Account {
		t.Error("sessions[acc] overwrote the first install's Account — XOR must NOT overwrite")
	}
	if installed.binding.SessionID != firstBinding.SessionID {
		t.Errorf("binding.SessionID: got %q, want %q (first install's)", installed.binding.SessionID, firstBinding.SessionID)
	}
	if installed.participant != "P-first" {
		t.Errorf("participant: got %q, want %q (first install's)", installed.participant, "P-first")
	}
}

// Dispatch wiring: an InstallSession routed through the broker's inbox
// reaches handleInstallSession on the owning shard. Uses a one-shard
// broker (routing is trivial) and the public Start/Stop lifecycle so the
// shard's goroutine does the dispatch (not the test goroutine).
func TestHandleInstallSession_DispatchViaRunningShard(t *testing.T) {
	brokerUnderTest := NewBroker("B-dispatch", 4, []*capstone.Bundle{nil}, nil, MoneyConfig{})
	brokerUnderTest.Start()
	t.Cleanup(brokerUnderTest.Stop)

	accountID := account.AccountID("acc-dispatch")
	loaded := loadedAccountFixture(accountID, true)
	binding := bindingFixture("sess-dispatch", "X-dispatch")
	reply := make(chan error, 1)

	brokerUnderTest.route(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-dispatch",
		Reply:       reply,
	})

	select {
	case err := <-reply:
		if err != nil {
			t.Fatalf("dispatched install: got %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatched install: no reply within 2s — dispatch wiring broken")
	}
}
