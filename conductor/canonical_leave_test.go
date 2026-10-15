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

// postExit drives handleCanonicalExit directly with ?session=<sid>.
func postExit(t *testing.T, conductor *Conductor, method string, sessionID SessionID) *httptest.ResponseRecorder {
	t.Helper()
	url := PathSessionExit
	if sessionID != "" {
		url += "?session=" + string(sessionID)
	}
	request := httptest.NewRequest(method, url, strings.NewReader(""))
	recorder := httptest.NewRecorder()
	conductor.handleCanonicalExit(recorder, request)
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

// --- 501 on-demand session branch ------------------------------------

func TestHandleCanonicalExit_OnDemandSession_Returns501(t *testing.T) {
	harness := newLeaveHarness(t)
	sessionID := installActiveSession(t, harness, "human-42", "IN", OnDemand)

	recorder := postExit(t, harness.conductor, http.MethodPost, sessionID)
	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501. body=%q", recorder.Code, recorder.Body.String())
	}
	// Session must stay Active — the branch refused, nothing was touched.
	harness.conductor.reg.mu.Lock()
	stillActive := harness.conductor.reg.sessions[sessionID].State == Active
	harness.conductor.reg.mu.Unlock()
	if !stillActive {
		t.Error("on-demand session state mutated despite 501 refusal")
	}
	if harness.broker.endSessionCount() != 0 {
		t.Errorf("broker.EndSession called despite 501 refusal: %d", harness.broker.endSessionCount())
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
