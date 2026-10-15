package conductor

// End-to-end lifecycle scenarios. Drives the shipped HTTP handlers +
// closeCanonicalMarket against a stateful fake broker that models:
//
//   - identity: ownerID → AccountID is idempotent across repeat
//     EnsureAccount calls (same input → same output). Modelled because
//     the rejoin flow depends on it: "fresh" rejoin must land on the
//     same durable AccountID as the first join.
//   - XOR + binding: AccountID → SessionID binding, enforced per
//     account; a second StartSession for a live binding returns
//     funnel.ErrSessionAlreadyLive, EndSession clears the slot.
//   - exchange→accounts index: ExchangeID → set[AccountID], written
//     at StartSession, retained through EndSession (detach-keep),
//     drained by FinalizeMarket. Models the broker-side index that
//     makes the EOD sweep find detached accounts the conductor no
//     longer tracks.
//   - position carry: a per-account counter the test increments on
//     each StartSession to prove the account survived the detach.
//     "Positions" here is a stand-in for the whole LoadedAccount
//     payload; the invariant we lock is "the broker remembers us
//     across leave + rejoin."
//
// All four scenarios execute real conductor code (handleJoinCanonical,
// handleCanonicalExit, closeCanonicalMarket) and assert on observable
// state: HTTP status codes, response bodies, conductor registry maps,
// and the stateful broker's visible side effects.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/contract"
	"simulator/economy/lifecycle"
)

// --- stateful broker --------------------------------------------------

type statefulBroker struct {
	callLock sync.Mutex

	// Identity — stable AccountID for each ownerID. First call creates;
	// subsequent calls return the same handle.
	accountByOwner map[string]funnel.AccountID
	accountCounter int

	// Per-account live SessionID. Empty slot ⇒ XOR free (another
	// StartSession is admissible). Non-empty ⇒ XOR held.
	liveSession map[funnel.AccountID]funnel.SessionID

	// Exchange-to-accounts index — models the broker-side sweep target
	// for FinalizeMarket. An account is added on StartSession, retained
	// across EndSession (detach-keep), removed on FinalizeMarket.
	exchangeIndex map[funnel.ExchangeID]map[funnel.AccountID]struct{}

	// Positions proxy: a counter per account that increments on each
	// StartSession. A rejoin that re-installs over a surviving loaded
	// account sees the previous counter — the carry invariant.
	positionCounter map[funnel.AccountID]int

	// Programmable hooks per method.
	ensureAccountFunc  func(ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
	startSessionFunc   func(cmd funnel.SessionStartCmd) error
	endSessionFunc     func(cmd funnel.SessionEndCmd) error
	finalizeMarketFunc func(exchangeID funnel.ExchangeID) error

	// Call tallies.
	ensureAccountCalls  int
	startSessionCalls   int
	endSessionCalls     int
	finalizeMarketCalls int

	// FinalizeMarket sweep record — AccountIDs swept per call. Keyed by
	// ExchangeID so a test with multiple close rounds can inspect each
	// pass.
	sweptByFinalize map[funnel.ExchangeID][]funnel.AccountID
}

func newStatefulBroker() *statefulBroker {
	return &statefulBroker{
		accountByOwner:  map[string]funnel.AccountID{},
		liveSession:     map[funnel.AccountID]funnel.SessionID{},
		exchangeIndex:   map[funnel.ExchangeID]map[funnel.AccountID]struct{}{},
		positionCounter: map[funnel.AccountID]int{},
		sweptByFinalize: map[funnel.ExchangeID][]funnel.AccountID{},
	}
}

func (fake *statefulBroker) EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error) {
	fake.callLock.Lock()
	fake.ensureAccountCalls++
	hook := fake.ensureAccountFunc
	fake.callLock.Unlock()
	if hook != nil {
		return hook(ownerType, ownerID)
	}
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	if existing, present := fake.accountByOwner[ownerID]; present {
		return existing, nil
	}
	fake.accountCounter++
	accountID := funnel.AccountID(fmt.Sprintf("stateful-acct-%d", fake.accountCounter))
	fake.accountByOwner[ownerID] = accountID
	return accountID, nil
}

func (fake *statefulBroker) StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error {
	fake.callLock.Lock()
	fake.startSessionCalls++
	hook := fake.startSessionFunc
	fake.callLock.Unlock()
	if hook != nil {
		return hook(cmd)
	}
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	if existing := fake.liveSession[cmd.AccountID]; existing != "" {
		return funnel.ErrSessionAlreadyLive
	}
	fake.liveSession[cmd.AccountID] = cmd.SessionID
	indexForExchange := fake.exchangeIndex[cmd.ExchangeID]
	if indexForExchange == nil {
		indexForExchange = map[funnel.AccountID]struct{}{}
		fake.exchangeIndex[cmd.ExchangeID] = indexForExchange
	}
	indexForExchange[cmd.AccountID] = struct{}{}
	fake.positionCounter[cmd.AccountID]++
	return nil
}

func (fake *statefulBroker) QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error) {
	return true, nil
}

func (fake *statefulBroker) WireInstance(exchangeID string, privateStream <-chan contract.EngineResult) {
}

func (fake *statefulBroker) EndSession(ctx context.Context, cmd funnel.SessionEndCmd) error {
	fake.callLock.Lock()
	fake.endSessionCalls++
	hook := fake.endSessionFunc
	fake.callLock.Unlock()
	if hook != nil {
		return hook(cmd)
	}
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	live, present := fake.liveSession[cmd.AccountID]
	if !present {
		return funnel.ErrNoLiveSession
	}
	if live != cmd.SessionID {
		return funnel.ErrSessionMismatch
	}
	delete(fake.liveSession, cmd.AccountID)
	// Detach-keep: KEEP the account in exchangeIndex + positionCounter.
	return nil
}

func (fake *statefulBroker) FinalizeMarket(ctx context.Context, exchangeID funnel.ExchangeID) error {
	fake.callLock.Lock()
	fake.finalizeMarketCalls++
	hook := fake.finalizeMarketFunc
	fake.callLock.Unlock()
	if hook != nil {
		return hook(exchangeID)
	}
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	indexForExchange := fake.exchangeIndex[exchangeID]
	var swept []funnel.AccountID
	for accountID := range indexForExchange {
		swept = append(swept, accountID)
		delete(fake.liveSession, accountID)
		delete(fake.positionCounter, accountID)
	}
	delete(fake.exchangeIndex, exchangeID)
	fake.sweptByFinalize[exchangeID] = swept
	return nil
}

func (fake *statefulBroker) snapshot() statefulBrokerSnapshot {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	// Deep-copy to avoid aliasing on later access.
	indexCopy := map[funnel.ExchangeID][]funnel.AccountID{}
	for exID, accts := range fake.exchangeIndex {
		row := make([]funnel.AccountID, 0, len(accts))
		for a := range accts {
			row = append(row, a)
		}
		indexCopy[exID] = row
	}
	sweepCopy := map[funnel.ExchangeID][]funnel.AccountID{}
	for exID, accts := range fake.sweptByFinalize {
		row := make([]funnel.AccountID, len(accts))
		copy(row, accts)
		sweepCopy[exID] = row
	}
	liveCopy := map[funnel.AccountID]funnel.SessionID{}
	for a, s := range fake.liveSession {
		liveCopy[a] = s
	}
	positionCopy := map[funnel.AccountID]int{}
	for a, p := range fake.positionCounter {
		positionCopy[a] = p
	}
	return statefulBrokerSnapshot{
		ensureAccountCalls:  fake.ensureAccountCalls,
		startSessionCalls:   fake.startSessionCalls,
		endSessionCalls:     fake.endSessionCalls,
		finalizeMarketCalls: fake.finalizeMarketCalls,
		liveSession:         liveCopy,
		exchangeIndex:       indexCopy,
		positionCounter:     positionCopy,
		sweptByFinalize:     sweepCopy,
	}
}

type statefulBrokerSnapshot struct {
	ensureAccountCalls  int
	startSessionCalls   int
	endSessionCalls     int
	finalizeMarketCalls int
	liveSession         map[funnel.AccountID]funnel.SessionID
	exchangeIndex       map[funnel.ExchangeID][]funnel.AccountID
	positionCounter     map[funnel.AccountID]int
	sweptByFinalize     map[funnel.ExchangeID][]funnel.AccountID
}

var _ BrokerPort = (*statefulBroker)(nil)

// --- harness ---------------------------------------------------------

type lifecycleHarness struct {
	conductor *Conductor
	broker    *statefulBroker
	exchange  *recordingExchangeHandle
}

func newLifecycleHarness(t *testing.T) *lifecycleHarness {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := newStatefulBroker()
	conductor, err := NewConductor(Config{
		CfgDir:    t.TempDir(),
		OHLCVPool: pool,
		Broker:    broker,
		Market:    stubMarketDataPort{},
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	return &lifecycleHarness{
		conductor: conductor,
		broker:    broker,
		exchange:  &recordingExchangeHandle{},
	}
}

func (harness *lifecycleHarness) installLiveMarket(t *testing.T, tag MarketTag) MarketID {
	t.Helper()
	marketID := MarketID(string(tag) + ":e2e")
	market := &Market{
		ID:      marketID,
		Mode:    Canonical,
		Tag:     tag,
		State:   Live,
		exch:    harness.exchange,
		closeAt: time.Now().Add(time.Hour),
		inbound: make(chan contract.InboundMessage, 1),
		members: map[SessionID]struct{}{},
	}
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID] = market
	harness.conductor.reg.mu.Unlock()
	return marketID
}

func (harness *lifecycleHarness) postJoin(t *testing.T, userID, tag string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"user":{"email":"%s@x","user_id":"%s"},"market":"%s"}`, userID, userID, tag)
	request := httptest.NewRequest(http.MethodPost, PathSessionCanonical, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	harness.conductor.handleJoinCanonical(recorder, request)
	return recorder
}

func (harness *lifecycleHarness) postExit(t *testing.T, sessionID SessionID) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, PathSessionExit+"?session="+string(sessionID), strings.NewReader(""))
	recorder := httptest.NewRecorder()
	harness.conductor.handleCanonicalExit(recorder, request)
	return recorder
}

func (harness *lifecycleHarness) postCurrent(t *testing.T, userID string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"user":{"email":"%s@x","user_id":"%s"}}`, userID, userID)
	request := httptest.NewRequest(http.MethodPost, PathSessionCurrent, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	harness.conductor.handleSessionCurrent(recorder, request)
	return recorder
}

func decodeJoinResponse(t *testing.T, recorder *httptest.ResponseRecorder) joinResponseBody {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("join status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	var body joinResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode join body: %v (raw=%q)", err, recorder.Body.String())
	}
	return body
}

// --- scenario 1 — join → leave → rejoin preserves identity + positions

func TestLifecycle_JoinLeaveRejoin_SameAccount(t *testing.T) {
	harness := newLifecycleHarness(t)
	marketID := harness.installLiveMarket(t, "IN")

	// First join. Broker's XOR + exchange index populated. Positions = 1.
	firstJoin := decodeJoinResponse(t, harness.postJoin(t, "human-42", "IN"))
	firstSID := SessionID(firstJoin.SessionID)

	snapshot1 := harness.broker.snapshot()
	if snapshot1.ensureAccountCalls != 1 || snapshot1.startSessionCalls != 1 {
		t.Fatalf("after first join: ensure=%d start=%d, want 1 each",
			snapshot1.ensureAccountCalls, snapshot1.startSessionCalls)
	}
	firstAccountID := harness.broker.accountByOwner["human-42"]
	if firstAccountID == "" {
		t.Fatal("broker did not record an AccountID for the user")
	}
	if snapshot1.liveSession[firstAccountID] == "" {
		t.Fatal("broker XOR not held after first join (should be live)")
	}
	if snapshot1.positionCounter[firstAccountID] != 1 {
		t.Errorf("positions after first join = %d, want 1", snapshot1.positionCounter[firstAccountID])
	}
	// Account is indexed under the market's exchangeID.
	if _, present := snapshot1.exchangeIndex[funnel.ExchangeID(marketID)]; !present {
		t.Error("broker exchange index missing the market after first join")
	}

	// Leave. XOR frees, exchange index retained, positions carry.
	exitRecorder := harness.postExit(t, firstSID)
	if exitRecorder.Code != http.StatusOK {
		t.Fatalf("exit status = %d, want 200. body=%q", exitRecorder.Code, exitRecorder.Body.String())
	}
	snapshot2 := harness.broker.snapshot()
	if snapshot2.endSessionCalls != 1 {
		t.Errorf("endSessionCalls = %d, want 1", snapshot2.endSessionCalls)
	}
	if snapshot2.liveSession[firstAccountID] != "" {
		t.Error("broker XOR still held after leave (should be free)")
	}
	if snapshot2.positionCounter[firstAccountID] != 1 {
		t.Errorf("positions after leave = %d, want 1 (detach-keep must preserve)", snapshot2.positionCounter[firstAccountID])
	}
	indexAfterLeave := snapshot2.exchangeIndex[funnel.ExchangeID(marketID)]
	foundInIndex := false
	for _, a := range indexAfterLeave {
		if a == firstAccountID {
			foundInIndex = true
			break
		}
	}
	if !foundInIndex {
		t.Error("broker exchange index lost the account after leave (detach-keep must retain)")
	}

	// Rejoin — same user, same market. Must land on the SAME AccountID,
	// positions should carry forward (counter increments to 2).
	secondJoin := decodeJoinResponse(t, harness.postJoin(t, "human-42", "IN"))
	if secondJoin.SessionID == firstJoin.SessionID {
		t.Error("rejoin returned the SAME SessionID as the first join (should be a fresh sid)")
	}
	snapshot3 := harness.broker.snapshot()
	secondAccountID := harness.broker.accountByOwner["human-42"]
	if secondAccountID != firstAccountID {
		t.Errorf("rejoin landed on AccountID %q, want %q (idempotent EnsureAccount)", secondAccountID, firstAccountID)
	}
	if snapshot3.positionCounter[firstAccountID] != 2 {
		t.Errorf("positions after rejoin = %d, want 2 (install must have run over the surviving account)",
			snapshot3.positionCounter[firstAccountID])
	}
	if snapshot3.liveSession[firstAccountID] == "" {
		t.Error("broker XOR not held after rejoin")
	}
	if string(snapshot3.liveSession[firstAccountID]) != secondJoin.SessionID {
		t.Errorf("broker XOR = %q, want %q (new sid)", snapshot3.liveSession[firstAccountID], secondJoin.SessionID)
	}
}

// --- scenario 2 — join + leave + close sweeps detached and attached ---

func TestLifecycle_JoinLeaveClose_SweepsBothAttachedAndDetached(t *testing.T) {
	harness := newLifecycleHarness(t)
	marketID := harness.installLiveMarket(t, "IN")

	leaverJoin := decodeJoinResponse(t, harness.postJoin(t, "human-leaver", "IN"))
	leaverSID := SessionID(leaverJoin.SessionID)
	attachedJoin := decodeJoinResponse(t, harness.postJoin(t, "human-attached", "IN"))
	_ = attachedJoin

	// Leaver detaches; attached user stays through close.
	if exit := harness.postExit(t, leaverSID); exit.Code != http.StatusOK {
		t.Fatalf("leaver exit status = %d, want 200", exit.Code)
	}

	// Pre-close snapshot: index carries BOTH accounts (one live, one
	// detached). Live XOR holds for the attached user only.
	preClose := harness.broker.snapshot()
	leaverAccountID := harness.broker.accountByOwner["human-leaver"]
	attachedAccountID := harness.broker.accountByOwner["human-attached"]
	indexPreClose := preClose.exchangeIndex[funnel.ExchangeID(marketID)]
	foundLeaver, foundAttached := false, false
	for _, a := range indexPreClose {
		if a == leaverAccountID {
			foundLeaver = true
		}
		if a == attachedAccountID {
			foundAttached = true
		}
	}
	if !(foundLeaver && foundAttached) {
		t.Fatalf("broker exchange index pre-close: leaver=%v attached=%v, want both true", foundLeaver, foundAttached)
	}
	if preClose.liveSession[leaverAccountID] != "" {
		t.Error("leaver XOR still held pre-close")
	}
	if preClose.liveSession[attachedAccountID] == "" {
		t.Error("attached user XOR not held pre-close")
	}

	// Close fires. Broker.FinalizeMarket sweeps BOTH accounts via its
	// index. Conductor reaps the attached session; the leaver was never
	// in the member set so the conductor has nothing to do for them.
	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	postClose := harness.broker.snapshot()
	if postClose.finalizeMarketCalls != 1 {
		t.Errorf("FinalizeMarket calls = %d, want 1", postClose.finalizeMarketCalls)
	}
	swept := postClose.sweptByFinalize[funnel.ExchangeID(marketID)]
	if len(swept) != 2 {
		t.Fatalf("FinalizeMarket swept %d accounts, want 2 (leaver + attached). swept=%v", len(swept), swept)
	}
	sweptLeaver, sweptAttached := false, false
	for _, a := range swept {
		if a == leaverAccountID {
			sweptLeaver = true
		}
		if a == attachedAccountID {
			sweptAttached = true
		}
	}
	if !(sweptLeaver && sweptAttached) {
		t.Errorf("FinalizeMarket sweep: leaver=%v attached=%v, want both true", sweptLeaver, sweptAttached)
	}
	// Exchange handle was Stop()ed exactly once by the close flow.
	if stops := harness.exchange.stopCount.Load(); stops != 1 {
		t.Errorf("exchange.Stop count = %d, want 1", stops)
	}
	// Conductor registry reaped entirely.
	harness.conductor.reg.mu.Lock()
	_, marketPresent := harness.conductor.reg.markets[marketID]
	sessionsAfter := len(harness.conductor.reg.sessions)
	byUserAfter := len(harness.conductor.reg.byUser)
	harness.conductor.reg.mu.Unlock()
	if marketPresent || sessionsAfter != 0 || byUserAfter != 0 {
		t.Errorf("registry residue after close: market=%v sessions=%d byUser=%d",
			marketPresent, sessionsAfter, byUserAfter)
	}
}

// --- scenario 3 — close denies reconnect + session-current -----------

func TestLifecycle_JoinCloseReconnectDenied(t *testing.T) {
	harness := newLifecycleHarness(t)
	marketID := harness.installLiveMarket(t, "IN")

	decodeJoinResponse(t, harness.postJoin(t, "human-42", "IN"))

	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	// /session/current for the user → 404 (byUser cleared on reap).
	currentRecorder := harness.postCurrent(t, "human-42")
	if currentRecorder.Code != http.StatusNotFound {
		t.Fatalf("/session/current after close: status = %d, want 404", currentRecorder.Code)
	}

	// /session/canonical for the same market → 409 ErrMarketNotLive
	// (market record deleted on reap, findLiveCanonical returns nil).
	rejoinRecorder := harness.postJoin(t, "human-42", "IN")
	if rejoinRecorder.Code != http.StatusConflict {
		t.Fatalf("rejoin after close: status = %d, want 409. body=%q",
			rejoinRecorder.Code, rejoinRecorder.Body.String())
	}
}

// --- scenario 4 — leave failure then retry succeeds ------------------

func TestLifecycle_LeaveFailureThenRetry(t *testing.T) {
	harness := newLifecycleHarness(t)
	harness.installLiveMarket(t, "IN")

	firstJoin := decodeJoinResponse(t, harness.postJoin(t, "human-42", "IN"))
	sid := SessionID(firstJoin.SessionID)

	// First leave: broker scripted to fail. Expect 504, session reverted
	// to Active, byUser still points at it.
	harness.broker.endSessionFunc = func(cmd funnel.SessionEndCmd) error {
		return funnel.ErrBrokerTimeout
	}
	firstExit := harness.postExit(t, sid)
	if firstExit.Code != http.StatusGatewayTimeout {
		t.Fatalf("first exit status = %d, want 504. body=%q",
			firstExit.Code, firstExit.Body.String())
	}
	harness.conductor.reg.mu.Lock()
	stateAfterRevert := harness.conductor.reg.sessions[sid].State
	byUserPresent := harness.conductor.reg.byUser["human-42"] != nil
	harness.conductor.reg.mu.Unlock()
	if stateAfterRevert != Active {
		t.Errorf("session state after revert = %v, want Active", stateAfterRevert)
	}
	if !byUserPresent {
		t.Error("byUser missing after revert (should be preserved)")
	}

	// Retry — unhook the error, broker now succeeds. Expect 200 + reap.
	harness.broker.callLock.Lock()
	harness.broker.endSessionFunc = nil
	harness.broker.callLock.Unlock()
	secondExit := harness.postExit(t, sid)
	if secondExit.Code != http.StatusOK {
		t.Fatalf("retry exit status = %d, want 200. body=%q",
			secondExit.Code, secondExit.Body.String())
	}
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sid]
	byUserAfterRetry := harness.conductor.reg.byUser["human-42"] != nil
	harness.conductor.reg.mu.Unlock()
	if sessionPresent {
		t.Error("session not reaped after successful retry")
	}
	if byUserAfterRetry {
		t.Error("byUser not freed after successful retry")
	}
	snapshot := harness.broker.snapshot()
	if snapshot.endSessionCalls != 2 {
		t.Errorf("endSessionCalls = %d, want 2 (first failed, retry succeeded)", snapshot.endSessionCalls)
	}
}

// --- compile-time: errors.Is dispatch locked --------------------------
//
// A sanity check that errors.Is still cuts through the join flow's %w
// wrap on ErrMarketNotLive — the rejoin-after-close scenario depends on
// it. Not strictly end-to-end but captures the error-category contract.
func TestLifecycle_RejoinAfterCloseWrapsErrMarketNotLive(t *testing.T) {
	harness := newLifecycleHarness(t)
	marketID := harness.installLiveMarket(t, "IN")
	decodeJoinResponse(t, harness.postJoin(t, "human-42", "IN"))
	harness.conductor.closeCanonicalMarket(context.Background(), marketID)

	_, err := harness.conductor.JoinCanonical(context.Background(), UserID("human-42"), "IN")
	if !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("JoinCanonical after close err = %v, want errors.Is ErrMarketNotLive", err)
	}
}
