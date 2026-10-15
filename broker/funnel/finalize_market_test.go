package funnel

// FinalizeMarket + handleFinalizeMarketOnShard tests. Two concerns:
//
//   - shard-handler semantics: for each account indexed under ExchangeID,
//     call SessionSave, delete on success, keep on failure (don't-discard-
//     on-fail), honor the live-vs-detached preference, panic on a non-
//     canonical account.
//   - broker-method fan-out: push one FinalizeMarketOnShard to every
//     shard, aggregate replies, surface aggregated failures as
//     ErrFinalizeIncomplete, honor ctx deadline mid-wait.
//
// Uses a hand-written fake sessionSaver so tests can run entirely in
// memory (no Postgres, no Redis) while still exercising every branch.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/lifecycle"
)

// --- fake sessionSaver -------------------------------------------------

// fakeSessionSaver records every SessionSave call and lets each test
// program a per-call outcome (nil or a scripted error). Safe for the
// concurrent fan-out tests (lock guards the slice + map).
type fakeSessionSaver struct {
	callLock      sync.Mutex
	calls         []fakeSaveCall
	errByAccount  map[string]error
	defaultError  error
	alwaysSucceed bool
}

type fakeSaveCall struct {
	SessionID string
	AccountID account.AccountID
}

func (fake *fakeSessionSaver) SessionSave(ctx context.Context, sessionID string, loaded lifecycle.LoadedAccount) error {
	fake.callLock.Lock()
	fake.calls = append(fake.calls, fakeSaveCall{SessionID: sessionID, AccountID: loaded.Account.ID})
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

func (fake *fakeSessionSaver) callCount() int {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	return len(fake.calls)
}

// --- shard-handler semantics -------------------------------------------

// Happy sweep: install three accounts under one exchangeID, call
// handleFinalizeMarketOnShard directly, confirm the sweep saves each,
// clears both sessions and loadedByExchange, and returns the right
// aggregates. Detach-keep tested separately below.
func TestHandleFinalizeMarketOnShard_HappySweep(t *testing.T) {
	shardUnderTest := newShard("B-finalize-happy", 4, nil, nil)
	saverFake := &fakeSessionSaver{alwaysSucceed: true}
	shardUnderTest.saver = saverFake

	for _, suffix := range []string{"a", "b", "c"} {
		accountID := account.AccountID("acc-" + suffix)
		installOnePresent(t, shardUnderTest, accountID, SessionID("sess-"+suffix), "X-happy")
	}

	reply := make(chan FinalizeMarketReply, 1)
	shardUnderTest.handleFinalizeMarketOnShard(FinalizeMarketOnShard{
		Ctx:        context.Background(),
		ExchangeID: "X-happy",
		Reply:      reply,
	})

	aggregate := <-reply
	if aggregate.AccountsProcessed != 3 {
		t.Errorf("AccountsProcessed = %d, want 3", aggregate.AccountsProcessed)
	}
	if aggregate.SaveErrors != 0 {
		t.Errorf("SaveErrors = %d, want 0", aggregate.SaveErrors)
	}
	if aggregate.FirstErr != nil {
		t.Errorf("FirstErr = %v, want nil", aggregate.FirstErr)
	}
	if saverFake.callCount() != 3 {
		t.Errorf("SessionSave call count = %d, want 3", saverFake.callCount())
	}
	if len(shardUnderTest.sessions) != 0 {
		t.Errorf("sessions map not drained: %d entries", len(shardUnderTest.sessions))
	}
	if _, present := shardUnderTest.loadedByExchange["X-happy"]; present {
		t.Error("loadedByExchange[X-happy] still present after a clean sweep")
	}
}

// Detached-and-live mix: one account is parked in `detached` (user
// pressed Leave before close); another is still live in `sessions`
// (user never left). Both get swept by the same FinalizeMarket.
func TestHandleFinalizeMarketOnShard_SweepsBothLiveAndDetached(t *testing.T) {
	shardUnderTest := newShard("B-finalize-mixed", 4, nil, nil)
	saverFake := &fakeSessionSaver{alwaysSucceed: true}
	shardUnderTest.saver = saverFake

	// One account kept live.
	installOnePresent(t, shardUnderTest, "acc-live", "sess-live", "X-mixed")
	// One account installed then uninstalled → parked in detached.
	installOnePresent(t, shardUnderTest, "acc-parked", "sess-parked", "X-mixed")
	uninstallOne(t, shardUnderTest, "acc-parked", "sess-parked")

	reply := make(chan FinalizeMarketReply, 1)
	shardUnderTest.handleFinalizeMarketOnShard(FinalizeMarketOnShard{
		Ctx:        context.Background(),
		ExchangeID: "X-mixed",
		Reply:      reply,
	})

	aggregate := <-reply
	if aggregate.AccountsProcessed != 2 {
		t.Errorf("AccountsProcessed = %d, want 2 (one live + one detached)", aggregate.AccountsProcessed)
	}
	if saverFake.callCount() != 2 {
		t.Errorf("SessionSave call count = %d, want 2", saverFake.callCount())
	}
	if len(shardUnderTest.sessions) != 0 || len(shardUnderTest.detached) != 0 {
		t.Errorf("maps not drained: sessions=%d detached=%d", len(shardUnderTest.sessions), len(shardUnderTest.detached))
	}
}

// Per-account save error: one SessionSave returns a scripted error. The
// sweep continues (forward-progress), the account whose save failed
// stays in the sessions map AND in the loadedByExchange index, the
// aggregate records SaveErrors=1 with FirstErr wrapping the cause.
func TestHandleFinalizeMarketOnShard_SaveErrorKeepsAccountLoaded(t *testing.T) {
	shardUnderTest := newShard("B-finalize-err", 4, nil, nil)
	scriptedErr := errors.New("scripted save failure")
	saverFake := &fakeSessionSaver{
		errByAccount: map[string]error{"acc-fail": scriptedErr},
	}
	shardUnderTest.saver = saverFake

	installOnePresent(t, shardUnderTest, "acc-ok", "sess-ok", "X-err")
	installOnePresent(t, shardUnderTest, "acc-fail", "sess-fail", "X-err")

	reply := make(chan FinalizeMarketReply, 1)
	shardUnderTest.handleFinalizeMarketOnShard(FinalizeMarketOnShard{
		Ctx:        context.Background(),
		ExchangeID: "X-err",
		Reply:      reply,
	})

	aggregate := <-reply
	if aggregate.AccountsProcessed != 2 {
		t.Errorf("AccountsProcessed = %d, want 2", aggregate.AccountsProcessed)
	}
	if aggregate.SaveErrors != 1 {
		t.Errorf("SaveErrors = %d, want 1", aggregate.SaveErrors)
	}
	if !errors.Is(aggregate.FirstErr, scriptedErr) {
		t.Errorf("FirstErr = %v, want errors.Is scripted", aggregate.FirstErr)
	}
	// The succeeding account is gone; the failing one stays.
	if _, present := shardUnderTest.sessions["acc-ok"]; present {
		t.Error("sessions[acc-ok] not drained (should have been saved + unloaded)")
	}
	if _, present := shardUnderTest.sessions["acc-fail"]; !present {
		t.Error("sessions[acc-fail] drained despite save failure (should stay for retry)")
	}
	indexed, present := shardUnderTest.loadedByExchange["X-err"]
	if !present {
		t.Fatal("loadedByExchange[X-err] missing despite one account still loaded for retry")
	}
	if _, inIndex := indexed["acc-fail"]; !inIndex {
		t.Error("loadedByExchange[X-err] lost its acc-fail entry")
	}
	if _, inIndex := indexed["acc-ok"]; inIndex {
		t.Error("loadedByExchange[X-err] still has acc-ok (should have been removed)")
	}
}

// A non-canonical account in a canonical FinalizeMarket index is an
// invariant violation — the handler panics per the project constitution.
func TestHandleFinalizeMarketOnShard_PanicsOnNonCanonicalAccount(t *testing.T) {
	shardUnderTest := newShard("B-finalize-noncan", 4, nil, nil)
	saverFake := &fakeSessionSaver{alwaysSucceed: true}
	shardUnderTest.saver = saverFake

	// Install with PersistOnClose=false (on-demand) under a canonical
	// market index.
	installOne(t, shardUnderTest, "acc-ondemand", "sess-ondemand", "X-noncan", false /*persistOnClose*/)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on non-canonical account in finalize sweep")
		}
		if msg, ok := recovered.(string); !ok {
			t.Fatalf("panic value is not a string: %T %v", recovered, recovered)
		} else if msg == "" {
			t.Fatal("panic message empty")
		}
	}()

	reply := make(chan FinalizeMarketReply, 1)
	shardUnderTest.handleFinalizeMarketOnShard(FinalizeMarketOnShard{
		Ctx:        context.Background(),
		ExchangeID: "X-noncan",
		Reply:      reply,
	})
}

// Shard with no saver wired (bundle==nil at construction) records a
// per-account save-error for every account rather than panicking.
func TestHandleFinalizeMarketOnShard_NoSaverRecordsSaveErrors(t *testing.T) {
	shardUnderTest := newShard("B-finalize-nosaver", 4, nil, nil)
	// saver intentionally not set.

	installOnePresent(t, shardUnderTest, "acc-x", "sess-x", "X-nosaver")

	reply := make(chan FinalizeMarketReply, 1)
	shardUnderTest.handleFinalizeMarketOnShard(FinalizeMarketOnShard{
		Ctx:        context.Background(),
		ExchangeID: "X-nosaver",
		Reply:      reply,
	})

	aggregate := <-reply
	if aggregate.AccountsProcessed != 1 {
		t.Errorf("AccountsProcessed = %d, want 1", aggregate.AccountsProcessed)
	}
	if aggregate.SaveErrors != 1 {
		t.Errorf("SaveErrors = %d, want 1 (nil saver should record a save error)", aggregate.SaveErrors)
	}
	if aggregate.FirstErr == nil {
		t.Error("FirstErr nil, want a save-missing error")
	}
}

// Empty index for an unknown ExchangeID is a no-op success.
func TestHandleFinalizeMarketOnShard_UnknownExchangeIsNoop(t *testing.T) {
	shardUnderTest := newShard("B-finalize-unknown", 4, nil, nil)
	saverFake := &fakeSessionSaver{alwaysSucceed: true}
	shardUnderTest.saver = saverFake

	reply := make(chan FinalizeMarketReply, 1)
	shardUnderTest.handleFinalizeMarketOnShard(FinalizeMarketOnShard{
		Ctx:        context.Background(),
		ExchangeID: "X-unknown",
		Reply:      reply,
	})

	aggregate := <-reply
	if aggregate.AccountsProcessed != 0 {
		t.Errorf("AccountsProcessed = %d, want 0", aggregate.AccountsProcessed)
	}
	if saverFake.callCount() != 0 {
		t.Errorf("SessionSave called %d times for an unknown ExchangeID", saverFake.callCount())
	}
}

// --- broker-method fan-out --------------------------------------------

// FinalizeMarket fans out to every shard. Build a two-shard broker,
// install accounts whose AccountIDs hash to both shards, confirm every
// shard reports its sweep and the aggregate is nil on all-success.
func TestFinalizeMarket_FansOutAcrossShards(t *testing.T) {
	brokerUnderTest := NewBroker("B-finalize-fanout", 4, []*capstone.Bundle{nil, nil}, nil, MoneyConfig{}, nil)
	// Inject the saver on every shard.
	saverFake := &fakeSessionSaver{alwaysSucceed: true}
	for _, shardOwner := range brokerUnderTest.shards {
		shardOwner.saver = saverFake
	}
	brokerUnderTest.Start()
	t.Cleanup(brokerUnderTest.Stop)

	// Enough accounts that at least one lands on each of the two shards
	// regardless of hash. 16 is plenty with a 50/50 split; verify at
	// test end that both shards saw at least one install.
	for index := 0; index < 16; index++ {
		accountID := account.AccountID(fmt.Sprintf("acc-fan-%02d", index))
		sessionID := SessionID(fmt.Sprintf("sess-fan-%02d", index))
		installOnePresentViaRouter(t, brokerUnderTest, accountID, sessionID, "X-fan")
	}
	// Confirm both shards saw installs.
	for shardIndex, shardOwner := range brokerUnderTest.shards {
		if indexForExchange := shardOwner.loadedByExchange["X-fan"]; len(indexForExchange) == 0 {
			t.Fatalf("shard %d has no accounts for X-fan — hash distribution did not split; cannot assert fan-out", shardIndex)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := brokerUnderTest.FinalizeMarket(ctx, "X-fan"); err != nil {
		t.Fatalf("FinalizeMarket: %v, want nil", err)
	}
	if saverFake.callCount() != 16 {
		t.Errorf("SessionSave call count = %d, want 16 (every account visited once)", saverFake.callCount())
	}
	for shardIndex, shardOwner := range brokerUnderTest.shards {
		if len(shardOwner.loadedByExchange) != 0 {
			t.Errorf("shard %d loadedByExchange still has %d entries after FinalizeMarket", shardIndex, len(shardOwner.loadedByExchange))
		}
	}
}

// Aggregated error: one shard's sweep hits a SessionSave failure.
// FinalizeMarket returns ErrFinalizeIncomplete wrapping the first cause,
// but still waits for every shard to reply (forward progress).
func TestFinalizeMarket_SurfaceAggregatedError(t *testing.T) {
	brokerUnderTest := NewBroker("B-finalize-aggerr", 4, []*capstone.Bundle{nil, nil}, nil, MoneyConfig{}, nil)
	scriptedErr := errors.New("one scripted save failure")
	for _, shardOwner := range brokerUnderTest.shards {
		shardOwner.saver = &fakeSessionSaver{
			errByAccount: map[string]error{"acc-fail-only": scriptedErr},
		}
	}
	brokerUnderTest.Start()
	t.Cleanup(brokerUnderTest.Stop)

	// Install one that always succeeds, one that always fails.
	installOnePresentViaRouter(t, brokerUnderTest, "acc-fail-only", "sess-fail", "X-aggerr")
	installOnePresentViaRouter(t, brokerUnderTest, "acc-ok", "sess-ok", "X-aggerr")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := brokerUnderTest.FinalizeMarket(ctx, "X-aggerr")
	if !errors.Is(err, ErrFinalizeIncomplete) {
		t.Fatalf("err = %v, want errors.Is ErrFinalizeIncomplete", err)
	}
	if !errors.Is(err, scriptedErr) {
		t.Errorf("err does not wrap scripted cause: %v", err)
	}
}

// --- helpers -----------------------------------------------------------

// installOnePresent installs one canonical account on the shard directly
// (bypasses dispatch — the test is the sole writer on a non-running
// shard). Returns after the shard-local maps show the install landed.
func installOnePresent(t *testing.T, shardUnderTest *shard, accountID account.AccountID, sessionID SessionID, exchangeID ExchangeID) {
	t.Helper()
	installOne(t, shardUnderTest, accountID, sessionID, exchangeID, true /*persistOnClose*/)
}

func installOne(t *testing.T, shardUnderTest *shard, accountID account.AccountID, sessionID SessionID, exchangeID ExchangeID, persistOnClose bool) {
	t.Helper()
	loaded := loadedAccountFixture(accountID, persistOnClose)
	binding := bindingFixture(sessionID, exchangeID)
	reply := make(chan error, 1)
	shardUnderTest.handleInstallSession(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-" + ParticipantID(accountID),
		Reply:       reply,
	})
	if err := <-reply; err != nil {
		t.Fatalf("install %q: %v", accountID, err)
	}
}

// uninstallOne drives handleUninstallSession synchronously.
func uninstallOne(t *testing.T, shardUnderTest *shard, accountID account.AccountID, sessionID SessionID) {
	t.Helper()
	reply := make(chan error, 1)
	shardUnderTest.handleUninstallSession(UninstallSession{
		AccountID: accountID,
		SessionID: sessionID,
		Reply:     reply,
	})
	if err := <-reply; err != nil {
		t.Fatalf("uninstall %q: %v", accountID, err)
	}
}

// installOnePresentViaRouter installs one account through the broker's
// route() so the shard goroutine (not the test goroutine) does the
// handler call — the correct discipline for a running broker.
func installOnePresentViaRouter(t *testing.T, brokerUnderTest *Broker, accountID account.AccountID, sessionID SessionID, exchangeID ExchangeID) {
	t.Helper()
	loaded := loadedAccountFixture(accountID, true)
	binding := bindingFixture(sessionID, exchangeID)
	reply := make(chan error, 1)
	brokerUnderTest.route(InstallSession{
		AccountID:   accountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: "P-" + ParticipantID(accountID),
		Reply:       reply,
	})
	select {
	case err := <-reply:
		if err != nil {
			t.Fatalf("install %q: %v", accountID, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("install %q: no reply within 2s", accountID)
	}
}
