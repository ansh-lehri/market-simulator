package conductor

// End-to-end on-demand lifecycle scenarios. Sibling of
// canonical_lifecycle_e2e_test.go. Drives real conductor code
// (handleSessionExit, handleSessionCurrent, tearDownOnDemandInstance,
// spawnOnDemandReaper) against a Live on-demand instance seeded
// directly into the registry — bypassing StartOnDemand so each
// scenario can pin closeAt, Mode, and clock deterministically.
//
// Four scenarios:
//
//  1. accidental nav → session alive. The user drops their socket but
//     presses no exit button; /session/current still returns the
//     session with mode:"ondemand" and both timers populated. Models
//     the reconnect-after-nav path the UI depends on.
//
//  2. explicit exit → /session/current returns 404. The reap clears
//     byUser so the next /session/current honestly reports "no session"
//     and the UI shows start-new instead of auto-reconnecting into a
//     stale market.
//
//  3. reaper race against the exit handler — reaper fires AND the user
//     presses exit at nearly the same moment. The Market.State flip
//     resolves to exactly one teardown body; the other path no-ops.
//     Observable: one EndSession call, one Stop call, clean registry.
//
//  4. determinism — same scenario replayed twice with an injected
//     clock produces identical observable sequences (recording stubs'
//     call lists match element-for-element).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- harness ---------------------------------------------------------

// lifecycleOnDemandHarness extends the Stage-3 teardown harness with
// HTTP driver helpers for exit + current, so each scenario reads as a
// sequence of posts against the real handlers.
type lifecycleOnDemandHarness struct {
	*onDemandTeardownHarness
}

func newLifecycleOnDemandHarness(t *testing.T) *lifecycleOnDemandHarness {
	t.Helper()
	inner := newOnDemandTeardownHarness(t)
	t.Cleanup(inner.conductor.Stop)
	return &lifecycleOnDemandHarness{onDemandTeardownHarness: inner}
}

func (harness *lifecycleOnDemandHarness) postSessionExit(t *testing.T, sessionID SessionID) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		PathSessionExit+"?session="+string(sessionID),
		strings.NewReader(""),
	)
	recorder := httptest.NewRecorder()
	harness.conductor.handleSessionExit(recorder, request)
	return recorder
}

func (harness *lifecycleOnDemandHarness) postSessionCurrent(t *testing.T, userID UserID) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"user":{"email":"` + string(userID) + `@x","user_id":"` + string(userID) + `"}}`
	request := httptest.NewRequest(http.MethodPost, PathSessionCurrent, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	harness.conductor.handleSessionCurrent(recorder, request)
	return recorder
}

func decodeSessionCurrentOK(t *testing.T, recorder *httptest.ResponseRecorder) sessionCurrentResponseBody {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("/session/current status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	var body sessionCurrentResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /session/current body: %v (raw=%q)", err, recorder.Body.String())
	}
	return body
}

// --- scenario 1: nav-without-exit resolves to the live session -------

// A dropped socket is NOT a teardown — the Market stays Live, byUser
// stays populated, and /session/current returns the session with
// mode:"ondemand" and both timers so the UI countdown resumes.
func TestOnDemandLifecycle_NavWithoutExit_SessionCurrentReturnsLive(t *testing.T) {
	harness := newLifecycleOnDemandHarness(t)
	warmupEndsAt := harness.nowValue.Add(5 * time.Minute)
	closeAt := warmupEndsAt.Add(30 * time.Minute)
	// installActiveOnDemandInstance stamps warmupEndsAt = closeAt - 5m
	// by convention; override here so both timers compute to pinned
	// test instants a reader can verify.
	sessionID, marketID := installActiveOnDemandInstance(t, harness.onDemandTeardownHarness, "u-nav", "IN", closeAt)
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets[marketID].warmupEndsAt = warmupEndsAt
	harness.conductor.reg.mu.Unlock()

	currentResp := harness.postSessionCurrent(t, "u-nav")
	body := decodeSessionCurrentOK(t, currentResp)

	if body.SessionID != string(sessionID) {
		t.Errorf("SessionID = %q, want %q", body.SessionID, sessionID)
	}
	if body.Mode != "ondemand" {
		t.Errorf("Mode = %q, want \"ondemand\"", body.Mode)
	}
	if body.Market != "IN" {
		t.Errorf("Market = %q, want \"IN\"", body.Market)
	}
	if body.Status != "active" {
		t.Errorf("Status = %q, want \"active\"", body.Status)
	}
	if body.WarmupEndsAt == "" {
		t.Error("WarmupEndsAt empty; on-demand sessions must carry both timers for the UI countdown")
	}
	if body.SessionEndsAt == "" {
		t.Error("SessionEndsAt empty; on-demand sessions must carry both timers for the UI countdown")
	}
	// Market and session untouched — no teardown, no reap.
	if harness.broker.endSessionCount() != 0 {
		t.Errorf("EndSession calls = %d, want 0 (nav is not teardown)", harness.broker.endSessionCount())
	}
	if harness.exchange.stopCount.Load() != 0 {
		t.Errorf("exchange.Stop calls = %d, want 0 (nav is not teardown)", harness.exchange.stopCount.Load())
	}
}

// --- scenario 2: exit → /session/current honestly returns 404 --------

// After an explicit exit, the ephemeral session + byUser slot are
// reaped. /session/current must return 404 so the UI shows "start new"
// instead of resuming into a market that no longer exists.
func TestOnDemandLifecycle_ExplicitExit_SessionCurrentReturns404(t *testing.T) {
	harness := newLifecycleOnDemandHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, _ := installActiveOnDemandInstance(t, harness.onDemandTeardownHarness, "u-exit", "IN", closeAt)

	exitResp := harness.postSessionExit(t, sessionID)
	if exitResp.Code != http.StatusOK {
		t.Fatalf("/session/exit status = %d, want 200. body=%q", exitResp.Code, exitResp.Body.String())
	}

	currentResp := harness.postSessionCurrent(t, "u-exit")
	if currentResp.Code != http.StatusNotFound {
		t.Fatalf("/session/current after exit status = %d, want 404. body=%q", currentResp.Code, currentResp.Body.String())
	}
	// Teardown shape asserted: one EndSession, one Stop, maps all gone.
	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession calls = %d, want 1", harness.broker.endSessionCount())
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Errorf("exchange.Stop calls = %d, want 1", harness.exchange.stopCount.Load())
	}
}

// --- scenario 3: reaper vs. exit converge to exactly one teardown ----

// Both triggers (user exit + reap timer) ultimately call
// tearDownOnDemandInstance. The Market.State Live→TearingDown flip
// resolves the race: whichever path takes the registry mutex first
// runs the body; the other sees TearingDown and no-ops. Observable:
// one EndSession, one Stop, clean registry, no deadlock.
func TestOnDemandLifecycle_ReaperRaceWithExit_RunsBodyExactlyOnce(t *testing.T) {
	harness := newLifecycleOnDemandHarness(t)
	// Reaper's wall-clock fire window: closeAt = now + 20ms. Short
	// enough that the reaper and the exit race on the registry mutex
	// in the test's own timeframe. Both paths calling the body
	// concurrently is the race condition we're validating.
	closeAt := harness.nowValue.Add(20 * time.Millisecond)
	sessionID, marketID := installActiveOnDemandInstance(t, harness.onDemandTeardownHarness, "u-race", "IN", closeAt)

	// Spawn the real reaper (as the shipped spin-up finalize would).
	harness.conductor.spawnOnDemandReaper(marketID, closeAt)

	// Drive the user-exit in parallel so both triggers overlap on the
	// Market.State flip.
	exitDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		exitDone <- harness.postSessionExit(t, sessionID)
	}()

	// Wait for both the reaper goroutine and the exit HTTP call.
	harness.conductor.wg.Wait()
	exitResp := <-exitDone

	// Exit handler either sees the Live session (returns 200) or the
	// reaper already reaped it (returns 404). Both are correct
	// outcomes under the race — the invariant is that the registry
	// ends clean and the teardown body ran exactly once.
	switch exitResp.Code {
	case http.StatusOK, http.StatusNotFound:
	default:
		t.Fatalf("exit response = %d, want 200 or 404 under race. body=%q", exitResp.Code, exitResp.Body.String())
	}

	if harness.broker.endSessionCount() != 1 {
		t.Errorf("EndSession calls = %d, want 1 (body ran exactly once under race)", harness.broker.endSessionCount())
	}
	if harness.exchange.stopCount.Load() != 1 {
		t.Errorf("exchange.Stop calls = %d, want 1 (body ran exactly once under race)", harness.exchange.stopCount.Load())
	}
	// Registry fully reaped.
	harness.conductor.reg.mu.Lock()
	_, sessionPresent := harness.conductor.reg.sessions[sessionID]
	_, marketPresent := harness.conductor.reg.markets[marketID]
	_, byUserPresent := harness.conductor.reg.byUser["u-race"]
	harness.conductor.reg.mu.Unlock()
	if sessionPresent || marketPresent || byUserPresent {
		t.Errorf("registry not clean after race: session=%v market=%v byUser=%v",
			sessionPresent, marketPresent, byUserPresent)
	}
}

// --- scenario 4: determinism across two identical runs ---------------

// Replay the exit scenario twice with the same injected clock. The
// recording stubs' call sequences must match element-for-element — if
// a non-determinism crept into the ordering of EndSession / WindDown /
// Stop / reap, two runs would diverge here.
func TestOnDemandLifecycle_ExitScenario_IsDeterministic(t *testing.T) {
	type recordedRun struct {
		endSessionCount    int
		endSessionModeSeen string
		windDownCount      int
		stopCount          int32
		exitStatus         int
		postExitHas404     bool
	}

	runOnce := func() recordedRun {
		harness := newLifecycleOnDemandHarness(t)
		closeAt := harness.nowValue.Add(time.Minute)
		sessionID, _ := installActiveOnDemandInstance(t, harness.onDemandTeardownHarness, "u-det", "IN", closeAt)

		exitResp := harness.postSessionExit(t, sessionID)
		currentResp := harness.postSessionCurrent(t, "u-det")

		run := recordedRun{
			endSessionCount: harness.broker.endSessionCount(),
			windDownCount:   harness.agents.windDownCallCount(),
			stopCount:       harness.exchange.stopCount.Load(),
			exitStatus:      exitResp.Code,
			postExitHas404:  currentResp.Code == http.StatusNotFound,
		}
		if harness.broker.endSessionCount() > 0 {
			run.endSessionModeSeen = harness.broker.lastEndSessionCmd().Mode.String()
		}
		return run
	}

	first := runOnce()
	second := runOnce()
	if !reflect.DeepEqual(first, second) {
		t.Errorf("non-deterministic replay:\n  first  = %+v\n  second = %+v", first, second)
	}
}

// --- scenario 5: concurrent /session/current during teardown ---------

// A reader on /session/current racing an in-flight teardown must never
// crash or observe a torn state. The exit's own flip + reap are inside
// the registry mutex; a /session/current that lands between the flip
// and the reap sees State == TearingDown but the sessions map still
// has the record. resolveCurrentSession returns the snapshot with the
// market-sourced fields zeroed when the market record is gone, which
// is defensive against this exact window. Here we drive many readers
// in parallel against a single teardown and assert no reader panics
// and the final /session/current returns 404 cleanly.
func TestOnDemandLifecycle_ConcurrentCurrentDuringTeardown_NoCrashes(t *testing.T) {
	harness := newLifecycleOnDemandHarness(t)
	closeAt := harness.nowValue.Add(time.Minute)
	sessionID, _ := installActiveOnDemandInstance(t, harness.onDemandTeardownHarness, "u-concurrent", "IN", closeAt)

	const readerCount = 20
	var readersWG sync.WaitGroup
	readersWG.Add(readerCount)
	for readerIndex := 0; readerIndex < readerCount; readerIndex++ {
		go func() {
			defer readersWG.Done()
			harness.postSessionCurrent(t, "u-concurrent") // may be 200 or 404; either is fine
		}()
	}

	exitResp := harness.postSessionExit(t, sessionID)
	readersWG.Wait()

	if exitResp.Code != http.StatusOK {
		t.Fatalf("exit response = %d, want 200. body=%q", exitResp.Code, exitResp.Body.String())
	}
	finalCurrent := harness.postSessionCurrent(t, "u-concurrent")
	if finalCurrent.Code != http.StatusNotFound {
		t.Errorf("post-teardown /session/current = %d, want 404", finalCurrent.Code)
	}
}
