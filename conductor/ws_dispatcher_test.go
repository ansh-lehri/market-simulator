package conductor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simulator/broker/marketdata"
)

// --- fake WS handler + factory --------------------------------------

// recordingWSHandler is the test seam that intercepts the dispatcher's
// delegation. It records every ServeHTTP invocation under a lock
// (concurrent dispatcher tests exercise this) and writes a sentinel
// body so the test can prove the delegate actually ran. The factory
// method also records which *marketdata.FanOut the dispatcher resolved
// to — that is the one piece of state the dispatcher's correctness
// hinges on.
type recordingWSHandler struct {
	callLock    sync.Mutex
	gotFanouts  []*marketdata.FanOut
	gotRequests []*http.Request

	// probeOnServe is called inside ServeHTTP after recording. Tests use
	// it to run assertions on registry state WHILE the delegate is
	// executing — notably the J.7 lock-released-before-delegate probe.
	probeOnServe func(w http.ResponseWriter, r *http.Request)
}

func (handler *recordingWSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.callLock.Lock()
	handler.gotRequests = append(handler.gotRequests, r)
	handler.callLock.Unlock()
	if handler.probeOnServe != nil {
		handler.probeOnServe(w, r)
	}
	w.Header().Set("X-Fake-WS", "delegated")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"delegate":"fake-ws"}`))
}

func (handler *recordingWSHandler) callCount() int {
	handler.callLock.Lock()
	defer handler.callLock.Unlock()
	return len(handler.gotRequests)
}

// factory returns a wsHandlerFactoryFn that records the fanout the
// dispatcher resolved to and returns this handler. One factory per
// dispatcher invocation; the fanout is recorded then.
func (handler *recordingWSHandler) factory() wsHandlerFactoryFn {
	return func(resolvedFanout *marketdata.FanOut, _ marketdata.WSConfig) http.Handler {
		handler.callLock.Lock()
		handler.gotFanouts = append(handler.gotFanouts, resolvedFanout)
		handler.callLock.Unlock()
		return handler
	}
}

// --- harness: a conductor with a real fanout on its live market -----

// wsHarness carries all the handles a WS test needs. The live Market
// record has a REAL *marketdata.FanOut (built via NewMarketSnapshot +
// NewFanOut) so the dispatcher resolves to a concrete pointer the fake
// delegate can match on.
type wsHarness struct {
	conductor  *Conductor
	fakeWS     *recordingWSHandler
	marketID   MarketID
	sessionID  SessionID
	userID     UserID
	liveFanout *marketdata.FanOut
}

// newWSHarness builds a Conductor whose WS factory is the recording
// fake, installs a Live Canonical market with a real FanOut, and parks
// one Active Session referencing it. Returns everything the test might
// want to assert against.
func newWSHarness(t *testing.T) *wsHarness {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	fakeWS := &recordingWSHandler{}
	broker := &fakeJoinBroker{}
	conductor, err := NewConductor(Config{
		CfgDir:           t.TempDir(),
		OHLCVPool:        pool,
		Broker:           broker,
		Market:           stubMarketDataPort{},
		WSHandlerFactory: fakeWS.factory(),
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}

	marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")
	snapshot := marketdata.NewMarketSnapshot(marketdata.ExchangeID(marketID))
	location, locErr := time.LoadLocation("Asia/Kolkata")
	if locErr != nil {
		t.Fatalf("LoadLocation Asia/Kolkata: %v", locErr)
	}
	liveFanout := marketdata.NewFanOut(snapshot, "IN", location)

	sessionID := SessionID(newUUIDv4())
	userID := UserID("human-ws-" + newUUIDv4()[:8])
	session := &Session{
		ID:          sessionID,
		User:        userID,
		Market:      marketID,
		Account:     "fake-account",
		Participant: ParticipantID(newUUIDv4()),
		AccountType: Durable,
		State:       Active,
	}
	conductor.reg.mu.Lock()
	conductor.reg.markets[marketID].fanout = liveFanout
	conductor.reg.sessions[sessionID] = session
	conductor.reg.byUser[userID] = session
	conductor.reg.markets[marketID].members[sessionID] = struct{}{}
	conductor.reg.mu.Unlock()

	return &wsHarness{
		conductor:  conductor,
		fakeWS:     fakeWS,
		marketID:   marketID,
		sessionID:  sessionID,
		userID:     userID,
		liveFanout: liveFanout,
	}
}

// doWS issues a request to handleWS directly (bypasses the mux so a
// test can run without MountRoutes) and returns the recorder.
func doWS(t *testing.T, conductor *Conductor, method string, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	target := marketdata.PathWS
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	request := httptest.NewRequest(method, target, nil)
	recorder := httptest.NewRecorder()
	conductor.handleWS(recorder, request)
	return recorder
}

// --- authFromRequest unit test --------------------------------------

func TestAuthFromRequest(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantSID SessionID
	}{
		{"empty query", "/v1/ws", ""},
		{"empty session value", "/v1/ws?session=", ""},
		{"present session", "/v1/ws?session=abc-123", "abc-123"},
		{"session alongside other params", "/v1/ws?foo=bar&session=xyz", "xyz"},
		{"other param only", "/v1/ws?foo=bar", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, tc.url, nil)
			if got := authFromRequest(request); got != tc.wantSID {
				t.Errorf("authFromRequest(%q) = %q, want %q", tc.url, got, tc.wantSID)
			}
		})
	}
}

// --- 401: empty session carrier (must precede Upgrade) --------------

func TestHandleWS_EmptySessionReturns401(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{"no session query at all", ""},
		{"empty session value", "session="},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newWSHarness(t)
			recorder := doWS(t, harness.conductor, http.MethodGet, tc.query)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401. body=%q", recorder.Code, recorder.Body.String())
			}
			if harness.fakeWS.callCount() != 0 {
				t.Errorf("fake WS handler called %d times, must be 0 (reject must precede Upgrade)",
					harness.fakeWS.callCount())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- 404: various unresolvable sessions (each must precede Upgrade) -

func TestHandleWS_UnresolvableSessionReturns404(t *testing.T) {
	// Each sub-case programs a different "unresolvable" state and
	// asserts the dispatcher writes 404 without touching the fake WS
	// handler. The matrix covers every nil-return branch of
	// fanoutForSession.
	cases := []struct {
		name     string
		mutate   func(*wsHarness)
		queryFor func(*wsHarness) string
	}{
		{
			name:     "unknown session id",
			mutate:   func(*wsHarness) {},
			queryFor: func(*wsHarness) string { return "session=totally-unknown" },
		},
		{
			name: "session is Starting (not Active)",
			mutate: func(harness *wsHarness) {
				harness.conductor.reg.mu.Lock()
				harness.conductor.reg.sessions[harness.sessionID].State = Starting
				harness.conductor.reg.mu.Unlock()
			},
			queryFor: func(h *wsHarness) string { return "session=" + string(h.sessionID) },
		},
		{
			name: "session is Stopping",
			mutate: func(harness *wsHarness) {
				harness.conductor.reg.mu.Lock()
				harness.conductor.reg.sessions[harness.sessionID].State = Stopping
				harness.conductor.reg.mu.Unlock()
			},
			queryFor: func(h *wsHarness) string { return "session=" + string(h.sessionID) },
		},
		{
			name: "market is TearingDown",
			mutate: func(harness *wsHarness) {
				harness.conductor.reg.mu.Lock()
				harness.conductor.reg.markets[harness.marketID].State = TearingDown
				harness.conductor.reg.mu.Unlock()
			},
			queryFor: func(h *wsHarness) string { return "session=" + string(h.sessionID) },
		},
		{
			name: "market record deleted",
			mutate: func(harness *wsHarness) {
				harness.conductor.reg.mu.Lock()
				delete(harness.conductor.reg.markets, harness.marketID)
				harness.conductor.reg.mu.Unlock()
			},
			queryFor: func(h *wsHarness) string { return "session=" + string(h.sessionID) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newWSHarness(t)
			tc.mutate(harness)
			recorder := doWS(t, harness.conductor, http.MethodGet, tc.queryFor(harness))

			if recorder.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404. body=%q", recorder.Code, recorder.Body.String())
			}
			if harness.fakeWS.callCount() != 0 {
				t.Errorf("fake WS handler called %d times, must be 0 (reject must precede Upgrade)",
					harness.fakeWS.callCount())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- 405 non-GET -----------------------------------------------------

func TestHandleWS_NonGETReturns405(t *testing.T) {
	harness := newWSHarness(t)
	recorder := doWS(t, harness.conductor, http.MethodPost, "session="+string(harness.sessionID))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405. body=%q", recorder.Code, recorder.Body.String())
	}
	if harness.fakeWS.callCount() != 0 {
		t.Error("fake WS handler must not be invoked on non-GET")
	}
}

// --- Happy path: delegate invoked with the right fanout --------------

func TestHandleWS_HappyPathDelegates(t *testing.T) {
	harness := newWSHarness(t)
	recorder := doWS(t, harness.conductor, http.MethodGet, "session="+string(harness.sessionID))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if marker := recorder.Header().Get("X-Fake-WS"); marker != "delegated" {
		t.Errorf("X-Fake-WS header = %q, want %q — delegation did not run", marker, "delegated")
	}
	var body struct {
		Delegate string `json:"delegate"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode delegate body: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.Delegate != "fake-ws" {
		t.Errorf("delegate body marker = %q, want %q", body.Delegate, "fake-ws")
	}

	if harness.fakeWS.callCount() != 1 {
		t.Fatalf("fake WS handler called %d times, want 1", harness.fakeWS.callCount())
	}
	if len(harness.fakeWS.gotFanouts) != 1 {
		t.Fatalf("factory recorded %d fanouts, want 1", len(harness.fakeWS.gotFanouts))
	}
	if harness.fakeWS.gotFanouts[0] != harness.liveFanout {
		t.Errorf("delegate got fanout %p, want %p — Model-A resolve pointed at wrong market",
			harness.fakeWS.gotFanouts[0], harness.liveFanout)
	}
}

// --- J.7 structural probe: lock released BEFORE delegate -----------

// TestHandleWS_LockReleasedBeforeDelegate is THE load-bearing invariant
// of the whole dispatcher. The fake WS handler, inside its ServeHTTP,
// does a TryLock on the registry mutex. If the lock is held → dispatcher
// delegated with reg.mu held → every join and every other dispatcher
// resolve is frozen for the whole connection life → bug.
//
// Running under -race makes the TryLock race-safe.
func TestHandleWS_LockReleasedBeforeDelegate(t *testing.T) {
	harness := newWSHarness(t)

	var lockFreeDuringDelegate bool
	harness.fakeWS.probeOnServe = func(http.ResponseWriter, *http.Request) {
		acquired := harness.conductor.reg.mu.TryLock()
		lockFreeDuringDelegate = acquired
		if acquired {
			harness.conductor.reg.mu.Unlock()
		}
	}

	recorder := doWS(t, harness.conductor, http.MethodGet, "session="+string(harness.sessionID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("delegate did not run; status = %d body = %q", recorder.Code, recorder.Body.String())
	}
	if !lockFreeDuringDelegate {
		t.Fatal("registry mutex was HELD during fake WS delegation — fanoutForSession must release the lock before the caller delegates (otherwise a connection freezes the whole registry for its life)")
	}
}

// --- MountRoutes registers /v1/ws ------------------------------------

func TestMountRoutes_RegistersWS(t *testing.T) {
	harness := newWSHarness(t)
	mux := http.NewServeMux()
	harness.conductor.MountRoutes(mux)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := http.Get(server.URL + marketdata.PathWS + "?session=" + string(harness.sessionID))
	if err != nil {
		t.Fatalf("GET /v1/ws through mux: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mux /v1/ws status = %d, want 200 (delegated)", response.StatusCode)
	}
	if harness.fakeWS.callCount() != 1 {
		t.Errorf("fake WS called %d times via mux, want 1", harness.fakeWS.callCount())
	}

	// No regression on the pre-existing routes.
	livenessResp, livenessErr := http.Get(server.URL + LivenessPath)
	if livenessErr != nil {
		t.Fatalf("liveness GET: %v", livenessErr)
	}
	defer livenessResp.Body.Close()
	if livenessResp.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d, want 200", livenessResp.StatusCode)
	}
}

// --- -race: concurrent dispatcher resolves ---------------------------

// TestHandleWS_ConcurrentDispatcherResolves exercises the race surface
// between many simultaneous dispatcher invocations and the registry
// mutex. The fake delegate is designed to spend non-trivial time inside
// ServeHTTP (short sleep) so overlapping fanoutForSession resolves must
// actually serialize through the mutex. All should succeed.
func TestHandleWS_ConcurrentDispatcherResolves(t *testing.T) {
	harness := newWSHarness(t)
	harness.fakeWS.probeOnServe = func(http.ResponseWriter, *http.Request) {
		time.Sleep(2 * time.Millisecond)
	}

	const numDispatches = 32
	var successCount atomic.Int32
	var startBarrier sync.WaitGroup
	var done sync.WaitGroup
	startBarrier.Add(1)
	done.Add(numDispatches)

	for workerIndex := range numDispatches {
		go func() {
			defer done.Done()
			startBarrier.Wait()
			recorder := doWS(t, harness.conductor, http.MethodGet,
				fmt.Sprintf("session=%s&worker=%d", harness.sessionID, workerIndex))
			if recorder.Code == http.StatusOK {
				successCount.Add(1)
			}
		}()
	}
	startBarrier.Done()
	done.Wait()

	if got := successCount.Load(); got != numDispatches {
		t.Errorf("successful dispatches = %d, want %d", got, numDispatches)
	}
	if harness.fakeWS.callCount() != numDispatches {
		t.Errorf("fake WS call count = %d, want %d", harness.fakeWS.callCount(), numDispatches)
	}
}
