package conductor

// closeCanonicalMarket + armNextCanonicalClose tests. Fakes:
//
//   - fakeLeaveBroker (from session_leave_test.go) — now also exposes
//     a programmable FinalizeMarket outcome and an invocation list.
//   - recordingWindDownAgents — AgentOrchestrator with a WindDown
//     recorder (BringUp stays unused by the close path).
//   - recordingExchangeHandle — exchangeHandle that counts Stop() calls.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/contract"
)

// --- test doubles ----------------------------------------------------

type recordingWindDownAgents struct {
	windDownCalls []stubWindDownCall
	callLock      sync.Mutex
}

func (recorder *recordingWindDownAgents) BringUp(ctx context.Context, market MarketTag, mode Mode, instruments []contract.InstrumentID) error {
	return nil
}

func (recorder *recordingWindDownAgents) WindDown(market MarketTag, mode Mode) {
	recorder.callLock.Lock()
	recorder.windDownCalls = append(recorder.windDownCalls, stubWindDownCall{Market: market, Mode: mode})
	recorder.callLock.Unlock()
}

func (recorder *recordingWindDownAgents) windDownCallCount() int {
	recorder.callLock.Lock()
	defer recorder.callLock.Unlock()
	return len(recorder.windDownCalls)
}

func (recorder *recordingWindDownAgents) windDownCallsSnapshot() []stubWindDownCall {
	recorder.callLock.Lock()
	defer recorder.callLock.Unlock()
	copied := make([]stubWindDownCall, len(recorder.windDownCalls))
	copy(copied, recorder.windDownCalls)
	return copied
}

var _ AgentOrchestrator = (*recordingWindDownAgents)(nil)

// recordingExchangeHandle counts Stop() calls so tests can assert the
// close path called exchange.Stop exactly once. Also exposes a blocker
// channel so the lock-discipline test can park Stop mid-call and probe
// reg.mu.
type recordingExchangeHandle struct {
	stopCount atomic.Int32
	blockOn   chan struct{} // nil → no block
	enteredCh chan struct{} // optional: non-nil → send one struct{}
}

func (handle *recordingExchangeHandle) Stop() {
	if handle.enteredCh != nil {
		handle.enteredCh <- struct{}{}
	}
	if handle.blockOn != nil {
		<-handle.blockOn
	}
	handle.stopCount.Add(1)
}

var _ exchangeHandle = (*recordingExchangeHandle)(nil)

// --- harness ---------------------------------------------------------

type closeHarness struct {
	conductor *Conductor
	broker    *fakeLeaveBroker
	agents    *recordingWindDownAgents
	exchange  *recordingExchangeHandle
}

func newCloseHarness(t *testing.T) *closeHarness {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := &fakeLeaveBroker{}
	agents := &recordingWindDownAgents{}
	conductor, err := NewConductor(Config{
		CfgDir:    t.TempDir(),
		OHLCVPool: pool,
		Broker:    broker,
		Market:    stubMarketDataPort{},
		Agents:    agents,
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	return &closeHarness{
		conductor: conductor,
		broker:    broker,
		agents:    agents,
		exchange:  &recordingExchangeHandle{},
	}
}

// installLiveMarketWithMembers seeds the registry with one Live canonical
// market carrying the given set of members (each a user with an Active
// session on this market). Returns the MarketID.
func installLiveMarketWithMembers(t *testing.T, harness *closeHarness, tag MarketTag, memberUsers []UserID) MarketID {
	t.Helper()
	marketID := MarketID(string(tag) + ":close-test")
	market := &Market{
		ID:      marketID,
		Mode:    Canonical,
		Tag:     tag,
		State:   Live,
		exch:    harness.exchange,
		closeAt: time.Now().Add(time.Hour),
		members: map[SessionID]struct{}{},
	}
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID] = market
	for _, userID := range memberUsers {
		sessionID := SessionID("sid-" + string(userID))
		market.members[sessionID] = struct{}{}
		session := &Session{
			ID:          sessionID,
			User:        userID,
			Market:      marketID,
			Account:     AccountID("acc-" + string(userID)),
			AccountType: Durable,
			Participant: ParticipantID("pid-" + string(userID)),
			State:       Active,
		}
		harness.conductor.reg.sessions[sessionID] = session
		harness.conductor.reg.byUser[userID] = session
	}
	harness.conductor.reg.mu.Unlock()
	return marketID
}

// --- happy path ------------------------------------------------------

// Full flow: FinalizeMarket once with the exchangeID, WindDown once
// with the tag + Canonical, exchange.Stop once, all member sessions +
// byUser slots reaped, market record gone.
func TestCloseCanonicalMarket_HappyPath(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1", "human-2", "human-3"})

	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	// FinalizeMarket called exactly once with the right exchangeID.
	if harness.broker.finalizeMarketCalls() != 1 {
		t.Errorf("FinalizeMarket calls = %d, want 1", harness.broker.finalizeMarketCalls())
	}
	harness.broker.callLock.Lock()
	finalizeArg := ""
	if len(harness.broker.finalizeMarketInvocations) > 0 {
		finalizeArg = string(harness.broker.finalizeMarketInvocations[0])
	}
	harness.broker.callLock.Unlock()
	if finalizeArg != string(marketID) {
		t.Errorf("FinalizeMarket arg = %q, want %q", finalizeArg, marketID)
	}

	// WindDown called exactly once with the tag + Canonical.
	if harness.agents.windDownCallCount() != 1 {
		t.Errorf("WindDown call count = %d, want 1", harness.agents.windDownCallCount())
	}
	windDownSnapshot := harness.agents.windDownCallsSnapshot()
	if len(windDownSnapshot) == 1 {
		if windDownSnapshot[0].Market != "IN" {
			t.Errorf("WindDown Market = %q, want \"IN\"", windDownSnapshot[0].Market)
		}
		if windDownSnapshot[0].Mode != Canonical {
			t.Errorf("WindDown Mode = %v, want Canonical", windDownSnapshot[0].Mode)
		}
	}

	// exchange.Stop called exactly once.
	if stops := harness.exchange.stopCount.Load(); stops != 1 {
		t.Errorf("exchange.Stop call count = %d, want 1", stops)
	}

	// Registry reaped — no market, no sessions, no byUser entries.
	harness.conductor.reg.mu.Lock()
	_, marketPresent := harness.conductor.reg.markets[marketID]
	sessionCount := len(harness.conductor.reg.sessions)
	byUserCount := len(harness.conductor.reg.byUser)
	harness.conductor.reg.mu.Unlock()
	if marketPresent {
		t.Error("market record still present after reap")
	}
	if sessionCount != 0 {
		t.Errorf("sessions residue after reap: %d", sessionCount)
	}
	if byUserCount != 0 {
		t.Errorf("byUser residue after reap: %d", byUserCount)
	}
}

// FinalizeMarket error: the close proceeds — WindDown + Stop + reap
// still run (forward-progress contract).
func TestCloseCanonicalMarket_FinalizeErrorForwardProgress(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})
	harness.broker.finalizeMarketFunc = func(context.Context, funnel.ExchangeID) error {
		return errors.New("scripted finalize failure")
	}

	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	if harness.agents.windDownCallCount() != 1 {
		t.Errorf("WindDown call count = %d, want 1 (forward-progress on FinalizeMarket error)", harness.agents.windDownCallCount())
	}
	if stops := harness.exchange.stopCount.Load(); stops != 1 {
		t.Errorf("exchange.Stop call count = %d, want 1 (forward-progress on FinalizeMarket error)", stops)
	}
	harness.conductor.reg.mu.Lock()
	_, marketPresent := harness.conductor.reg.markets[marketID]
	harness.conductor.reg.mu.Unlock()
	if marketPresent {
		t.Error("market record not reaped after FinalizeMarket error (forward-progress contract broken)")
	}
}

// Run-once guard: a second close for the same marketID is a no-op.
func TestCloseCanonicalMarket_RunOnceGuard(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})

	harness.conductor.closeCanonicalMarket(context.Background(), marketID)
	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	if harness.broker.finalizeMarketCalls() != 1 {
		t.Errorf("FinalizeMarket calls = %d, want 1 (second close must be a no-op)", harness.broker.finalizeMarketCalls())
	}
	if stops := harness.exchange.stopCount.Load(); stops != 1 {
		t.Errorf("exchange.Stop calls = %d, want 1 (second close must be a no-op)", stops)
	}
}

// Racing join after the TearingDown flip: a Join for the closing
// market lands as ErrMarketNotLive because findLiveCanonical skips
// non-Live markets.
func TestCloseCanonicalMarket_RacingJoinRejected(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{})
	_ = marketID

	// Flip the market to TearingDown manually (simulates "we're mid
	// close, between guard-flip and reap").
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].State = TearingDown
	harness.conductor.reg.mu.Unlock()

	_, err := harness.conductor.JoinCanonical(context.Background(), UserID("human-latecomer"), "IN")
	if !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("Join during TearingDown err = %v, want ErrMarketNotLive", err)
	}
}

// Racing reconnect after flip: same error path — the TearingDown market
// is not a reconnect target.
func TestCloseCanonicalMarket_RacingReconnectRejected(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-attached"})

	// Make the user's session still-Active but market TearingDown.
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].State = TearingDown
	harness.conductor.reg.mu.Unlock()

	_, err := harness.conductor.JoinCanonical(context.Background(), UserID("human-attached"), "IN")
	if !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("Reconnect during TearingDown err = %v, want ErrMarketNotLive (findLiveCanonical skips non-Live)", err)
	}
}

// Racing WebSocket: fanoutForSession returns nil for a session whose
// market is TearingDown — dispatcher writes 404.
func TestCloseCanonicalMarket_RacingWSResolveMisses(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-attached"})

	// Flip to TearingDown; the member session is still Active in map.
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].State = TearingDown
	harness.conductor.reg.mu.Unlock()

	fanout := harness.conductor.fanoutForSession("sid-human-attached")
	if fanout != nil {
		t.Error("fanoutForSession returned non-nil for a TearingDown market (dispatcher would wrongly delegate)")
	}
}

// A user who LEFT before close is not in members and must NOT be
// touched by the conductor's reap. The broker's FinalizeMarket is
// where that account gets swept (via the exchange→accounts index) —
// we assert the conductor stayed out of that user's state.
func TestCloseCanonicalMarket_DetachedUserNotTouchedByConductor(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-still-attached"})

	// Simulate a user who left earlier: no entry in sessions, no entry
	// in byUser, no entry in market.members. The broker fake will see
	// FinalizeMarket called and could sweep that user via its own index
	// — the conductor's job is only not to interfere.
	// (We don't need to add anything to the conductor registry for the
	// detached user; the assertion is that the conductor touched zero
	// bytes outside the member set.)

	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	harness.conductor.reg.mu.Lock()
	sessionsAfter := len(harness.conductor.reg.sessions)
	byUserAfter := len(harness.conductor.reg.byUser)
	harness.conductor.reg.mu.Unlock()

	// Attached user got reaped (one member → zero sessions). A detached
	// user never had a conductor entry, so a zero count is also correct.
	if sessionsAfter != 0 || byUserAfter != 0 {
		t.Errorf("conductor touched detached state: sessions=%d byUser=%d, want 0 0",
			sessionsAfter, byUserAfter)
	}
	// FinalizeMarket was called — detached users ride that sweep.
	if harness.broker.finalizeMarketCalls() != 1 {
		t.Errorf("FinalizeMarket calls = %d, want 1 (detached users are swept via the broker barrier)", harness.broker.finalizeMarketCalls())
	}
}

// Nil exchange handle on the Market record (test-mode market injection
// without an actual exchange): close path must tolerate it as a no-op
// Stop. The production spin-up always sets a real handle.
func TestCloseCanonicalMarket_NilExchangeHandleIsSafe(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].exch = nil
	harness.conductor.reg.mu.Unlock()

	// Must not panic.
	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	// Everything else should still have run.
	if harness.broker.finalizeMarketCalls() != 1 {
		t.Errorf("FinalizeMarket calls = %d, want 1", harness.broker.finalizeMarketCalls())
	}
	if harness.agents.windDownCallCount() != 1 {
		t.Errorf("WindDown call count = %d, want 1", harness.agents.windDownCallCount())
	}
}

// Lock discipline: reg.mu must NOT be held across FinalizeMarket,
// WindDown, or exchange.Stop. We park the exchange.Stop and verify a
// fresh reg.mu take succeeds; same for broker.FinalizeMarket.
func TestCloseCanonicalMarket_LockReleasedAcrossOffLockWork(t *testing.T) {
	t.Run("during FinalizeMarket", func(t *testing.T) {
		harness := newCloseHarness(t)
		marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})
		barrierBlock := make(chan struct{})
		barrierEntered := make(chan struct{}, 1)
		harness.broker.finalizeMarketFunc = func(context.Context, funnel.ExchangeID) error {
			barrierEntered <- struct{}{}
			<-barrierBlock
			return nil
		}

		closeReturned := make(chan struct{})
		go func() {
			harness.conductor.closeCanonicalMarket(context.Background(), marketID)
			close(closeReturned)
		}()

		select {
		case <-barrierEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("FinalizeMarket not reached within 2s")
		}

		acquired := make(chan struct{})
		go func() {
			harness.conductor.reg.mu.Lock()
			harness.conductor.reg.mu.Unlock()
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("reg.mu held across FinalizeMarket — THE RULE violated")
		}

		close(barrierBlock)
		<-closeReturned
	})

	t.Run("during exchange.Stop", func(t *testing.T) {
		harness := newCloseHarness(t)
		marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})
		harness.exchange.blockOn = make(chan struct{})
		harness.exchange.enteredCh = make(chan struct{}, 1)

		closeReturned := make(chan struct{})
		go func() {
			harness.conductor.closeCanonicalMarket(context.Background(), marketID)
			close(closeReturned)
		}()

		select {
		case <-harness.exchange.enteredCh:
		case <-time.After(2 * time.Second):
			t.Fatal("exchange.Stop not reached within 2s")
		}

		acquired := make(chan struct{})
		go func() {
			harness.conductor.reg.mu.Lock()
			harness.conductor.reg.mu.Unlock()
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("reg.mu held across exchange.Stop — THE RULE violated")
		}

		close(harness.exchange.blockOn)
		<-closeReturned
	})
}

// --- MountRoutes smoke-check for existing routes (ensure no regression)
// Deliberately absent — Stage 4 adds no HTTP route; the timer-only
// trigger is covered end-to-end in the next test.

// --- timer: close-arm after successful spin-up -----------------------

// armNextCanonicalClose with closeAt set to now+Δ fires
// closeCanonicalMarket after the sleep. We bypass runCanonicalTimer
// and call armNextCanonicalClose directly — the full timer loop is
// covered by the existing shipped tests.
func TestArmNextCanonicalClose_FiresClose(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})

	// Set closeAt to just past "now" so sleepOrQuit's delay <= 0 path
	// fires immediately.
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].closeAt = time.Now().Add(-time.Second)
	harness.conductor.reg.mu.Unlock()

	// armNextCanonicalClose needs timerQuit; the Conductor's Start
	// usually initializes it but tests that bypass Start must do so
	// themselves.
	harness.conductor.timerQuit = make(chan struct{})

	if ok := harness.conductor.armNextCanonicalClose("IN", marketID); !ok {
		t.Fatal("armNextCanonicalClose returned false (should return true after a completed close)")
	}
	if harness.broker.finalizeMarketCalls() != 1 {
		t.Errorf("FinalizeMarket calls = %d, want 1", harness.broker.finalizeMarketCalls())
	}
	if stops := harness.exchange.stopCount.Load(); stops != 1 {
		t.Errorf("exchange.Stop calls = %d, want 1", stops)
	}
}

// A Stop() signal during the close-wait sleep must abort the sleep and
// return false (loop exits before firing close).
func TestArmNextCanonicalClose_StopDuringSleepReturnsFalse(t *testing.T) {
	harness := newCloseHarness(t)
	marketID := installLiveMarketWithMembers(t, harness, "IN", []UserID{"human-1"})

	// Close is far in the future so the sleep is long enough for us to
	// close timerQuit mid-wait.
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].closeAt = time.Now().Add(time.Hour)
	harness.conductor.reg.mu.Unlock()
	harness.conductor.timerQuit = make(chan struct{})

	result := make(chan bool, 1)
	go func() {
		result <- harness.conductor.armNextCanonicalClose("IN", marketID)
	}()

	// Let the goroutine enter sleep.
	time.Sleep(50 * time.Millisecond)
	close(harness.conductor.timerQuit)

	select {
	case ok := <-result:
		if ok {
			t.Error("armNextCanonicalClose returned true after Stop (should return false to exit the loop)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("armNextCanonicalClose did not return within 2s of Stop")
	}
	if harness.broker.finalizeMarketCalls() != 0 {
		t.Errorf("FinalizeMarket called despite Stop-during-sleep: %d", harness.broker.finalizeMarketCalls())
	}
	if stops := harness.exchange.stopCount.Load(); stops != 0 {
		t.Errorf("exchange.Stop called despite Stop-during-sleep: %d", stops)
	}
}

