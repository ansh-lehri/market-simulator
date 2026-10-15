package conductor

// Canonical-leave tests. Fake broker records EndSession / FinalizeMarket
// calls; a programmable endSessionFunc lets each test script a per-call
// outcome. The join-flow fake (fakeJoinBroker) is deliberately NOT
// reused — leave adds its own EndSession + FinalizeMarket surface, and
// mixing them would couple the two test suites.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/economy/lifecycle"
)

// --- fake broker for the leave flow ----------------------------------

// fakeLeaveBroker records EndSession + FinalizeMarket invocations and
// lets each test program a per-call endSessionFunc. Reuses the join
// fake's stubBrokerPort embedding for the EnsureAccount / StartSession /
// QualifyOnDemand / WireInstance scaffold defaults — leave tests do not
// exercise those surfaces.
type fakeLeaveBroker struct {
	stubBrokerPort

	callLock sync.Mutex

	endSessionInvocations []funnel.SessionEndCmd
	endSessionFunc        func(ctx context.Context, cmd funnel.SessionEndCmd) error

	finalizeMarketInvocations []funnel.ExchangeID
	finalizeMarketFunc        func(ctx context.Context, exchangeID funnel.ExchangeID) error
	finalizeMarketCount       int
}

func (fake *fakeLeaveBroker) EndSession(ctx context.Context, cmd funnel.SessionEndCmd) error {
	fake.callLock.Lock()
	fake.endSessionInvocations = append(fake.endSessionInvocations, cmd)
	hook := fake.endSessionFunc
	fake.callLock.Unlock()
	if hook == nil {
		return nil
	}
	return hook(ctx, cmd)
}

func (fake *fakeLeaveBroker) FinalizeMarket(ctx context.Context, exchangeID funnel.ExchangeID) error {
	fake.callLock.Lock()
	fake.finalizeMarketCount++
	fake.finalizeMarketInvocations = append(fake.finalizeMarketInvocations, exchangeID)
	hook := fake.finalizeMarketFunc
	fake.callLock.Unlock()
	if hook == nil {
		return nil
	}
	return hook(ctx, exchangeID)
}

func (fake *fakeLeaveBroker) endSessionCount() int {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	return len(fake.endSessionInvocations)
}

func (fake *fakeLeaveBroker) lastEndSessionCmd() funnel.SessionEndCmd {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	if len(fake.endSessionInvocations) == 0 {
		return funnel.SessionEndCmd{}
	}
	return fake.endSessionInvocations[len(fake.endSessionInvocations)-1]
}

func (fake *fakeLeaveBroker) finalizeMarketCalls() int {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	return fake.finalizeMarketCount
}

var _ BrokerPort = (*fakeLeaveBroker)(nil)

// --- harness ---------------------------------------------------------

type leaveHarness struct {
	conductor *Conductor
	broker    *fakeLeaveBroker
}

func newLeaveHarness(t *testing.T) *leaveHarness {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := &fakeLeaveBroker{}
	conductor, err := NewConductor(Config{
		CfgDir:    t.TempDir(),
		OHLCVPool: pool,
		Broker:    broker,
		Market:    stubMarketDataPort{},
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	return &leaveHarness{conductor: conductor, broker: broker}
}

// installActiveSession seats an Active canonical session for userID on
// the given market so leave can operate on it. Returns the sessionID
// and the market record.
func installActiveSession(t *testing.T, harness *leaveHarness, userID UserID, tag MarketTag, mode Mode) SessionID {
	t.Helper()
	marketID := MarketID(string(tag) + ":leave-test")
	sessionID := SessionID("sid-" + string(userID))
	market := &Market{
		ID:      marketID,
		Mode:    mode,
		Tag:     tag,
		State:   Live,
		members: map[SessionID]struct{}{sessionID: {}},
	}
	session := &Session{
		ID:          sessionID,
		User:        userID,
		Market:      marketID,
		Account:     AccountID("acc-" + string(userID)),
		AccountType: Durable,
		Participant: ParticipantID("pid-" + string(userID)),
		State:       Active,
	}
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID] = market
	harness.conductor.reg.sessions[sessionID] = session
	harness.conductor.reg.byUser[userID] = session
	harness.conductor.reg.mu.Unlock()
	return sessionID
}

// postExit drives handleSessionExit directly with ?session=<sid>.
func postExit(t *testing.T, conductor *Conductor, method string, sessionID SessionID) *httptest.ResponseRecorder {
	t.Helper()
	url := PathSessionExit
	if sessionID != "" {
		url += "?session=" + string(sessionID)
	}
	request := httptest.NewRequest(method, url, strings.NewReader(""))
	recorder := httptest.NewRecorder()
	conductor.handleSessionExit(recorder, request)
	return recorder
}

// --- 404 unknown session --------------------------------------------

func TestHandleCanonicalExit_UnknownSession_Returns404(t *testing.T) {
	harness := newLeaveHarness(t)

	recorder := postExit(t, harness.conductor, http.MethodPost, "sid-ghost")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404. body=%q", recorder.Code, recorder.Body.String())
	}
	if harness.broker.endSessionCount() != 0 {
		t.Errorf("broker.EndSession called %d times for unknown session, want 0", harness.broker.endSessionCount())
	}
	assertErrorBody(t, recorder)
}

// --- 409 session not Active -----------------------------------------

func TestHandleCanonicalExit_SessionNotActive_Returns409(t *testing.T) {
	for _, state := range []SessionState{Starting, Stopping, Ended} {
		t.Run(state.String(), func(t *testing.T) {
			harness := newLeaveHarness(t)
			sessionID := installActiveSession(t, harness, "human-42", "IN", Canonical)
			harness.conductor.reg.mu.Lock()
			harness.conductor.reg.sessions[sessionID].State = state
			harness.conductor.reg.mu.Unlock()

			recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
			if recorder.Code != http.StatusConflict {
				t.Fatalf("state=%s: status = %d, want 409. body=%q",
					state, recorder.Code, recorder.Body.String())
			}
			if harness.broker.endSessionCount() != 0 {
				t.Errorf("state=%s: broker.EndSession called despite non-Active state", state)
			}
		})
	}
}

// --- 200 happy path --------------------------------------------------

func TestHandleCanonicalExit_HappyPath_ReapsAndReturns200(t *testing.T) {
	harness := newLeaveHarness(t)
	sessionID := installActiveSession(t, harness, "human-42", "IN", Canonical)
	marketID := harness.conductor.reg.sessions[sessionID].Market

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}

	var body sessionExitResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ended" {
		t.Errorf("status = %q, want \"ended\"", body.Status)
	}

	// Session / member / byUser all reaped.
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	_, memberPresent := harness.conductor.reg.markets[marketID].members[sessionID]
	_, byUserPresent := harness.conductor.reg.byUser["human-42"]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent {
		t.Error("session still present after successful leave")
	}
	if memberPresent {
		t.Error("member entry still present after successful leave")
	}
	if byUserPresent {
		t.Error("byUser slot still present after successful leave")
	}

	// Exactly one broker.EndSession call carrying the canonical cmd.
	if harness.broker.endSessionCount() != 1 {
		t.Fatalf("broker.EndSession called %d times, want 1", harness.broker.endSessionCount())
	}
	cmd := harness.broker.lastEndSessionCmd()
	if cmd.Mode != lifecycle.Canonical {
		t.Errorf("EndSession Mode = %v, want Canonical", cmd.Mode)
	}
	if string(cmd.SessionID) != string(sessionID) {
		t.Errorf("EndSession SessionID = %q, want %q", cmd.SessionID, sessionID)
	}
	if cmd.AccountID == "" {
		t.Error("EndSession AccountID empty")
	}

	// No finalize touch on leave.
	if harness.broker.finalizeMarketCalls() != 0 {
		t.Errorf("broker.FinalizeMarket called on leave: %d", harness.broker.finalizeMarketCalls())
	}
}

// --- idempotency: broker says ErrNoLiveSession → still reap, 200 ----

func TestHandleCanonicalExit_BrokerNoLiveSession_IdempotentSuccess(t *testing.T) {
	harness := newLeaveHarness(t)
	sessionID := installActiveSession(t, harness, "human-42", "IN", Canonical)
	harness.broker.endSessionFunc = func(context.Context, funnel.SessionEndCmd) error {
		return funnel.ErrNoLiveSession
	}

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (ErrNoLiveSession is idempotent). body=%q",
			recorder.Code, recorder.Body.String())
	}
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent {
		t.Error("session not reaped on idempotent-success leave")
	}
}

// --- revert on broker error -----------------------------------------

func TestHandleCanonicalExit_BrokerErrorsRevertSessionAndMapToStatus(t *testing.T) {
	cases := []struct {
		name       string
		brokerErr  error
		wantStatus int
	}{
		{"ErrBrokerTimeout to 504", funnel.ErrBrokerTimeout, http.StatusGatewayTimeout},
		{"lifecycle.ErrLoadFailed to 503", lifecycle.ErrLoadFailed, http.StatusServiceUnavailable},
		{"lifecycle.ErrSnapshotWriteFailed to 503", lifecycle.ErrSnapshotWriteFailed, http.StatusServiceUnavailable},
		{"funnel.ErrSessionMismatch to 500", funnel.ErrSessionMismatch, http.StatusInternalServerError},
		{"funnel.ErrBadAccountID to 500", funnel.ErrBadAccountID, http.StatusInternalServerError},
		{"unknown error to 500", errors.New("something unexpected"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newLeaveHarness(t)
			sessionID := installActiveSession(t, harness, "human-42", "IN", Canonical)
			harness.broker.endSessionFunc = func(context.Context, funnel.SessionEndCmd) error {
				return tc.brokerErr
			}

			recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q",
					recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			assertErrorBody(t, recorder)

			// Session reverted to Active — never half-detached.
			harness.conductor.reg.mu.Lock()
			state := harness.conductor.reg.sessions[sessionID].State
			byUserPresent := harness.conductor.reg.byUser["human-42"] != nil
			harness.conductor.reg.mu.Unlock()
			if state != Active {
				t.Errorf("session state after revert = %v, want Active", state)
			}
			if !byUserPresent {
				t.Error("byUser slot missing after revert (should be preserved)")
			}
		})
	}
}

// --- request validation ---------------------------------------------

func TestHandleCanonicalExit_RequestValidation(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		sessionID  SessionID
		wantStatus int
	}{
		{"GET method -> 405", http.MethodGet, "sid-x", http.StatusMethodNotAllowed},
		{"PUT method -> 405", http.MethodPut, "sid-x", http.StatusMethodNotAllowed},
		{"missing ?session= -> 400", http.MethodPost, "", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newLeaveHarness(t)
			recorder := postExit(t, harness.conductor, tc.method, tc.sessionID)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q",
					recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if harness.broker.endSessionCount() != 0 {
				t.Errorf("broker.EndSession called despite request-validation reject: %d",
					harness.broker.endSessionCount())
			}
		})
	}
}

// --- panic recovery -------------------------------------------------

func TestHandleCanonicalExit_PanicReturns500(t *testing.T) {
	harness := newLeaveHarness(t)
	sessionID := installActiveSession(t, harness, "human-42", "IN", Canonical)
	harness.broker.endSessionFunc = func(context.Context, funnel.SessionEndCmd) error {
		panic("induced panic for recovery test")
	}

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
}

// --- lock discipline: reg.mu never held across broker.EndSession -----

// A slow broker.EndSession must not block an unrelated reg.mu take.
// Spin a probe goroutine that acquires + releases reg.mu while the
// broker is parked; the probe must complete well before the broker
// unblocks.
func TestHandleCanonicalExit_LockReleasedDuringBrokerCall(t *testing.T) {
	harness := newLeaveHarness(t)
	sessionID := installActiveSession(t, harness, "human-42", "IN", Canonical)

	brokerUnblock := make(chan struct{})
	brokerEntered := make(chan struct{}, 1)
	harness.broker.endSessionFunc = func(context.Context, funnel.SessionEndCmd) error {
		brokerEntered <- struct{}{}
		<-brokerUnblock
		return nil
	}

	// Fire the leave on a goroutine so this test can probe mid-call.
	leaveReturned := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		leaveReturned <- postExit(t, harness.conductor, http.MethodPost, sessionID)
	}()

	// Wait for the broker to be inside EndSession — at that point the
	// conductor is off-lock between reserve and reap.
	select {
	case <-brokerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("broker.EndSession not reached within 2s")
	}

	// Probe: can we acquire reg.mu right now? If the conductor is
	// correctly off-lock, this returns immediately.
	acquired := make(chan struct{})
	go func() {
		harness.conductor.reg.mu.Lock()
		harness.conductor.reg.mu.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("reg.mu is held across broker.EndSession — THE RULE violated")
	}

	// Let the broker finish, confirm the leave 200s.
	close(brokerUnblock)
	select {
	case recorder := <-leaveReturned:
		if recorder.Code != http.StatusOK {
			t.Errorf("leave status after unblock = %d, want 200", recorder.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leave handler did not return after broker unblocked")
	}
}

// --- MountRoutes registers /session/exit -----------------------------

func TestMountRoutes_RegistersSessionExit(t *testing.T) {
	harness := newLeaveHarness(t)
	sessionID := installActiveSession(t, harness, "human-via-mux", "IN", Canonical)

	mux := http.NewServeMux()
	harness.conductor.MountRoutes(mux)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := http.Post(
		server.URL+PathSessionExit+"?session="+string(sessionID),
		"application/json",
		strings.NewReader(""),
	)
	if err != nil {
		t.Fatalf("POST through mux: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mux POST status = %d, want 200", response.StatusCode)
	}
}

// =====================================================================
// On-demand teardown tests
// =====================================================================
//
// Coverage:
//   - onDemandReaperDelay (pure) — armed off closeAt minus now, both for
//     a fresh market and a market whose warm-up window has partially
//     elapsed. Proves the delay is NOT "duration from now."
//   - tearDownOnDemandInstance (body) — happy path reaps everything,
//     unknown-market / not-Live no-ops (run-once guard), EndSession
//     error forward-progresses to Stop + reap, FinalizeMarket is never
//     called, two concurrent callers resolve to exactly one body run.
//   - spawnOnDemandReaper (goroutine) — fires at closeAt, exits on
//     timerQuit without firing, firing after a prior reap is a safe
//     no-op.
//   - handleSessionExit on-demand branch — happy 200, already-reaped
//     404 (preflight-level), EndSession err still returns 200 (forward
//     progress).
//
// Reuses recordingWindDownAgents / recordingExchangeHandle from
// canonical_close_test.go (same package).

// onDemandTeardownHarness extends the leaveHarness surface with the
// agents + exchange recorders the on-demand teardown exercises. The
// shared broker fake already supports Mode-blind EndSession recording;
// the on-demand branch surfaces Mode=OnDemand on the recorded command.
type onDemandTeardownHarness struct {
	conductor *Conductor
	broker    *fakeLeaveBroker
	agents    *recordingWindDownAgents
	exchange  *recordingExchangeHandle
	nowValue  time.Time // written through newOnDemandTeardownHarness setClock
}

func newOnDemandTeardownHarness(t *testing.T) *onDemandTeardownHarness {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := &fakeLeaveBroker{}
	agents := &recordingWindDownAgents{}
	harness := &onDemandTeardownHarness{
		broker:   broker,
		agents:   agents,
		exchange: &recordingExchangeHandle{},
		nowValue: time.Date(2026, time.March, 15, 10, 0, 0, 0, time.UTC),
	}
	conductor, err := NewConductor(Config{
		CfgDir:    t.TempDir(),
		OHLCVPool: pool,
		Broker:    broker,
		Market:    stubMarketDataPort{},
		Agents:    agents,
		Now:       func() time.Time { return harness.nowValue },
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	// timerQuit is normally allocated by Conductor.Start. For reaper
	// tests we allocate it directly so the reaper's sleepOrQuit selects
	// a real channel rather than falling into the nil-chan "never
	// fires" branch.
	conductor.timerQuit = make(chan struct{})
	harness.conductor = conductor
	return harness
}

// installActiveOnDemandInstance seats a Live on-demand market with one
// Active session member and the harness's recording exchange handle
// stamped on Market.exch. Returns the (sessionID, marketID).
func installActiveOnDemandInstance(
	t *testing.T,
	harness *onDemandTeardownHarness,
	userID UserID,
	tag MarketTag,
	closeAt time.Time,
) (SessionID, MarketID) {
	t.Helper()
	sessionID := SessionID("od-sid-" + string(userID))
	marketID := MarketID("od:" + string(sessionID))
	market := &Market{
		ID:           marketID,
		Mode:         OnDemand,
		Tag:          tag,
		State:        Live,
		exch:         harness.exchange,
		closeAt:      closeAt,
		warmupEndsAt: closeAt.Add(-5 * time.Minute), // positive warm-up for realism; untested here
		members:      map[SessionID]struct{}{sessionID: {}},
	}
	session := &Session{
		ID:          sessionID,
		User:        userID,
		Market:      marketID,
		Account:     AccountID("od-acc-" + string(userID)),
		AccountType: Ephemeral,
		Participant: ParticipantID("od-pid-" + string(userID)),
		State:       Active,
	}
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID] = market
	harness.conductor.reg.sessions[sessionID] = session
	harness.conductor.reg.byUser[userID] = session
	harness.conductor.reg.mu.Unlock()
	return sessionID, marketID
}

// --- pure delay helper ------------------------------------------------

func TestOnDemandReaperDelay_FreshMarket_IsFullCloseAtMinusNow(t *testing.T) {
	constructInstant := time.Date(2026, time.March, 15, 10, 0, 0, 0, time.UTC)
	closeAt := constructInstant.Add(90 * time.Second) // 5s warm-up + 85s session, e.g.
	gotDelay := onDemandReaperDelay(constructInstant, closeAt)
	wantDelay := 90 * time.Second
	if gotDelay != wantDelay {
		t.Errorf("onDemandReaperDelay(now=%v, closeAt=%v) = %v, want %v", constructInstant, closeAt, gotDelay, wantDelay)
	}
}

// A reaper spawned mid-warm-up (e.g. after a slow finalize) must sleep
// for closeAt - now, NOT for the configured session duration. This is
// the specification's "armed off m.closeAt, not duration from now."
func TestOnDemandReaperDelay_PartiallyElapsed_StillClosesAtMinusNow(t *testing.T) {
	constructInstant := time.Date(2026, time.March, 15, 10, 0, 0, 0, time.UTC)
	closeAt := constructInstant.Add(90 * time.Second)
	nowMidWarmup := constructInstant.Add(30 * time.Second)
	gotDelay := onDemandReaperDelay(nowMidWarmup, closeAt)
	wantDelay := 60 * time.Second
	if gotDelay != wantDelay {
		t.Errorf("onDemandReaperDelay(now=T0+30s, closeAt=T0+90s) = %v, want %v (NOT the session duration from now)", gotDelay, wantDelay)
	}
}

// closeAt already in the past (e.g. reaper spawned after a long
// finalize + context switch) must return a non-positive delay so
// sleepOrQuit's "immediate fire" branch runs.
func TestOnDemandReaperDelay_PastCloseAt_NonPositive(t *testing.T) {
	constructInstant := time.Date(2026, time.March, 15, 10, 0, 0, 0, time.UTC)
	closeAt := constructInstant.Add(-5 * time.Second)
	gotDelay := onDemandReaperDelay(constructInstant, closeAt)
	if gotDelay > 0 {
		t.Errorf("onDemandReaperDelay(past closeAt) = %v, want <= 0", gotDelay)
	}
}

// --- tearDownOnDemandInstance body -----------------------------------

// Happy path: EndSession called once with Mode=OnDemand, WindDown
// called once with the tag + OnDemand, exchange.Stop called once, every
// registry map entry for the market + its member session gone.
func TestTearDownOnDemandInstance_HappyPath_ReapsAndCallsEndSessionWindDownStop(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)

	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession calls = %d, want 1", harness.broker.endSessionCount())
	}
	gotCmd := harness.broker.lastEndSessionCmd()
	if gotCmd.Mode != lifecycle.OnDemand {
		t.Errorf("EndSession Mode = %v, want OnDemand", gotCmd.Mode)
	}
	if string(gotCmd.SessionID) != string(sessionID) {
		t.Errorf("EndSession SessionID = %q, want %q", gotCmd.SessionID, sessionID)
	}
	if harness.agents.windDownCallCount() != 1 {
		t.Errorf("WindDown calls = %d, want 1", harness.agents.windDownCallCount())
	}
	windDowns := harness.agents.windDownCallsSnapshot()
	if len(windDowns) == 1 && (windDowns[0].Market != "IN" || windDowns[0].Mode != OnDemand) {
		t.Errorf("WindDown call = {Market:%q, Mode:%v}, want {IN, OnDemand}", windDowns[0].Market, windDowns[0].Mode)
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Errorf("exchange.Stop calls = %d, want 1", harness.exchange.stopCount.Load())
	}

	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	_, marketPresent := harness.conductor.reg.markets[marketID]
	_, byUserPresent := harness.conductor.reg.byUser["u1"]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent {
		t.Error("session still present after teardown")
	}
	if marketPresent {
		t.Error("market still present after teardown")
	}
	if byUserPresent {
		t.Error("byUser still present after teardown")
	}
}

// Teardown against a market that was never registered → no body, no
// call-outs. Covers the reaper firing after an exit already reaped.
func TestTearDownOnDemandInstance_UnknownMarket_NoOp(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	harness.conductor.tearDownOnDemandInstance(context.Background(), MarketID("od:does-not-exist"))

	if harness.broker.endSessionCount() != 0 {
		t.Errorf("EndSession calls = %d, want 0", harness.broker.endSessionCount())
	}
	if harness.agents.windDownCallCount() != 0 {
		t.Errorf("WindDown calls = %d, want 0", harness.agents.windDownCallCount())
	}
	if harness.exchange.stopCount.Load() != 0 {
		t.Errorf("Stop calls = %d, want 0", harness.exchange.stopCount.Load())
	}
}

// Market exists but is already TearingDown (losing racer in exit-vs-
// reaper race) → run-once guard fires, body skipped.
func TestTearDownOnDemandInstance_NotLive_RunOnceGuard(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	_, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].State = TearingDown
	harness.conductor.reg.mu.Unlock()

	harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)

	if harness.broker.endSessionCount() != 0 {
		t.Errorf("EndSession calls = %d, want 0 (already TearingDown)", harness.broker.endSessionCount())
	}
	if harness.exchange.stopCount.Load() != 0 {
		t.Errorf("Stop calls = %d, want 0 (already TearingDown)", harness.exchange.stopCount.Load())
	}
}

// EndSession returns an error (e.g. shard reports Redis delete failure
// wrapped as ErrDiscardFailed). The teardown must still call WindDown,
// Stop, and reap — forward-progress contract.
func TestTearDownOnDemandInstance_EndSessionErr_ForwardProgresses(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	harness.broker.endSessionFunc = func(ctx context.Context, cmd funnel.SessionEndCmd) error {
		return errors.New("scripted discard failure")
	}
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)

	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession attempts = %d, want 1", harness.broker.endSessionCount())
	}
	if harness.agents.windDownCallCount() != 1 {
		t.Error("WindDown skipped after EndSession error — forward-progress contract violated")
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Error("exchange.Stop skipped after EndSession error — forward-progress contract violated")
	}
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	_, marketPresent := harness.conductor.reg.markets[marketID]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent || marketPresent {
		t.Error("registry not reaped after EndSession error — forward-progress contract violated")
	}
}

// FinalizeMarket is a canonical-close concept and must NEVER be called
// from the on-demand teardown — the ephemeral account is discarded
// wholesale, not swept + persisted.
func TestTearDownOnDemandInstance_NeverCallsFinalizeMarket(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	_, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)

	if harness.broker.finalizeMarketCalls() != 0 {
		t.Errorf("FinalizeMarket calls = %d, want 0 (on-demand never finalizes)", harness.broker.finalizeMarketCalls())
	}
}

// Exit-vs-reaper race: two goroutines call the body concurrently. The
// mutex-held flip resolves to exactly one body run; the loser observes
// TearingDown and no-ops. Only one EndSession, one WindDown, one Stop.
func TestTearDownOnDemandInstance_ExitVsReaperRace_RunsBodyExactlyOnce(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	_, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	// Prolong the OFF-LOCK region slightly so both goroutines reliably
	// enter the body window — without this the first caller might
	// finish before the second even starts. 20ms is comfortable under
	// -race.
	harness.broker.endSessionFunc = func(ctx context.Context, cmd funnel.SessionEndCmd) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)
	}()
	go func() {
		defer wg.Done()
		harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)
	}()
	wg.Wait()

	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession calls = %d, want 1 (exactly one body run under race)", harness.broker.endSessionCount())
	}
	if harness.agents.windDownCallCount() != 1 {
		t.Errorf("WindDown calls = %d, want 1", harness.agents.windDownCallCount())
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Errorf("Stop calls = %d, want 1", harness.exchange.stopCount.Load())
	}
}

// --- spawnOnDemandReaper goroutine -----------------------------------

// Reaper armed with closeAt a short delay in the future fires
// tearDownOnDemandInstance. Observes the fire via the recording stops.
// Uses real wall-clock for the Timer wait, with the injected `now`
// pinned at test start so the delay computation is deterministic.
func TestSpawnOnDemandReaper_FiresAtCloseAt(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	// closeAt 50ms from the harness's pinned now. The reaper computes
	// delay off conductor.now(), which returns harness.nowValue; the
	// Timer then waits that real wall-clock interval.
	closeAt := harness.nowValue.Add(50 * time.Millisecond)
	_, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	harness.conductor.spawnOnDemandReaper(marketID, closeAt)
	harness.conductor.wg.Wait()

	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession calls = %d, want 1 (reaper fire)", harness.broker.endSessionCount())
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Errorf("Stop calls = %d, want 1", harness.exchange.stopCount.Load())
	}
	harness.conductor.reg.mu.Lock()
	_, marketPresent := harness.conductor.reg.markets[marketID]
	harness.conductor.reg.mu.Unlock()
	if marketPresent {
		t.Error("market not reaped after reaper fire")
	}
}

// Reaper armed with a closeAt well in the future, timerQuit closes
// before the Timer fires → reaper exits cleanly, no teardown call-outs.
// Proves conductor.Stop drains outstanding reapers.
func TestSpawnOnDemandReaper_ExitsOnTimerQuit(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(10 * time.Second) // long — ensures Quit wins
	_, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	harness.conductor.spawnOnDemandReaper(marketID, closeAt)

	// Close timerQuit directly; the harness bypassed Start so Stop's
	// stopOnce path isn't armed. Close is the shutdown signal the
	// shipped sleepOrQuit selects on.
	close(harness.conductor.timerQuit)
	harness.conductor.wg.Wait()

	if harness.broker.endSessionCount() != 0 {
		t.Errorf("EndSession calls = %d, want 0 (reaper exited on shutdown)", harness.broker.endSessionCount())
	}
	if harness.exchange.stopCount.Load() != 0 {
		t.Errorf("Stop calls = %d, want 0 (reaper exited on shutdown)", harness.exchange.stopCount.Load())
	}
}

// Reaper fires after another path (an explicit exit) already reaped
// the market. The body's "nil market" guard turns the fire into a safe
// no-op — no panic, no second Stop call.
func TestSpawnOnDemandReaper_FireAfterReap_SafeNoOp(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(30 * time.Millisecond)
	_, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	// Explicit exit happens first, before the reaper fires.
	harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)
	preReapEndSessions := harness.broker.endSessionCount()
	preReapStops := harness.exchange.stopCount.Load()

	harness.conductor.spawnOnDemandReaper(marketID, closeAt)
	harness.conductor.wg.Wait()

	if harness.broker.endSessionCount() != preReapEndSessions {
		t.Errorf("EndSession calls after reaper late-fire = %d, want unchanged %d", harness.broker.endSessionCount(), preReapEndSessions)
	}
	if harness.exchange.stopCount.Load() != preReapStops {
		t.Errorf("Stop calls after reaper late-fire = %d, want unchanged %d", harness.exchange.stopCount.Load(), preReapStops)
	}
}

// --- handleSessionExit on-demand branch ------------------------------

// Full flow via the handler: on-demand session Live → 200 with
// {status:"ended"}, EndSession called with Mode=OnDemand, Stop called,
// registry reaped.
func TestHandleSessionExit_OnDemandHappyPath_Returns200(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	var body sessionExitResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ended" {
		t.Errorf("status payload = %q, want \"ended\"", body.Status)
	}

	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession calls = %d, want 1", harness.broker.endSessionCount())
	}
	if harness.broker.lastEndSessionCmd().Mode != lifecycle.OnDemand {
		t.Errorf("EndSession Mode = %v, want OnDemand", harness.broker.lastEndSessionCmd().Mode)
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Errorf("Stop calls = %d, want 1", harness.exchange.stopCount.Load())
	}
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	_, marketPresent := harness.conductor.reg.markets[marketID]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent || marketPresent {
		t.Error("registry not reaped after on-demand exit")
	}
}

// On-demand session already reaped (reaper fired first) → preflight
// sees no session and the handler routes through the canonical path's
// ErrSessionNotFound → 404. Documents the status-table deviation from
// the orientation spec: "already-torn → 404 (preflight-honest)" rather
// than a 200 idempotent reply.
func TestHandleSessionExit_OnDemandAlreadyReaped_Returns404(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)
	// Explicit prior teardown reaps the session + market.
	harness.conductor.tearDownOnDemandInstance(context.Background(), marketID)
	preExitEndSessions := harness.broker.endSessionCount()

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404. body=%q", recorder.Code, recorder.Body.String())
	}
	if harness.broker.endSessionCount() != preExitEndSessions {
		t.Errorf("EndSession calls increased on already-reaped exit = %d, want unchanged %d",
			harness.broker.endSessionCount(), preExitEndSessions)
	}
}

// EndSession returns an error from the handler path (shard timeout
// scripted onto the fake). The handler still returns 200 because the
// teardown body forward-progresses — the user's intent is honored.
func TestHandleSessionExit_OnDemandEndSessionErr_StillReturns200AndReaps(t *testing.T) {
	harness := newOnDemandTeardownHarness(t)
	harness.broker.endSessionFunc = func(ctx context.Context, cmd funnel.SessionEndCmd) error {
		return errors.New("scripted shard failure")
	}
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, marketID := installActiveOnDemandInstance(t, harness, "u1", "IN", closeAt)

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (forward progress). body=%q", recorder.Code, recorder.Body.String())
	}
	// Reap still happened.
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	_, marketPresent := harness.conductor.reg.markets[marketID]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent || marketPresent {
		t.Error("registry not reaped despite EndSession error — forward-progress contract violated at handler level")
	}
}
