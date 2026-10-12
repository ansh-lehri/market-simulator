package conductor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// --- harness --------------------------------------------------------

// newHandlerHarness wraps newJoinHarness so the test fixture (fake
// broker + live canonical market record for "IN") is one call away.
// Returns the handler path and the raw conductor so individual tests
// can poke the registry or program broker behavior.
func newHandlerHarness(t *testing.T) (*Conductor, *fakeJoinBroker) {
	t.Helper()
	conductor, broker := newJoinHarness(t)
	return conductor, broker
}

// postJoin issues a POST to handleJoinCanonical directly (bypasses the
// mux so each test can run without needing to mount routes) and returns
// the recorder for assertion.
func postJoin(t *testing.T, conductor *Conductor, method string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var requestBody *strings.Reader
	if body == "" {
		requestBody = strings.NewReader("")
	} else {
		requestBody = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, PathSessionCanonical, requestBody)
	recorder := httptest.NewRecorder()
	conductor.handleJoinCanonical(recorder, request)
	return recorder
}

// --- Happy path ------------------------------------------------------

func TestHandleJoinCanonical_HappyPath(t *testing.T) {
	conductor, _ := newHandlerHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")

	recorder := postJoin(t, conductor, http.MethodPost,
		`{"user":{"email":"user@example.com","user_id":"human-42"},"market":"IN"}`)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var responseBody joinResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &responseBody); err != nil {
		t.Fatalf("decode response: %v (body=%q)", err, recorder.Body.String())
	}
	if !isUUIDv4(responseBody.SessionID) {
		t.Errorf("session_id %q is not a UUID v4", responseBody.SessionID)
	}
	if !isUUIDv4(responseBody.ParticipantID) {
		t.Errorf("participant_id %q is not a UUID v4", responseBody.ParticipantID)
	}
	if responseBody.Status != "active" {
		t.Errorf("status = %q, want \"active\"", responseBody.Status)
	}
}

// --- Request validation (400 / 405) ---------------------------------

func TestHandleJoinCanonical_RequestValidation(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		body       string
		wantStatus int
	}{
		{"GET method -> 405", http.MethodGet, "", http.StatusMethodNotAllowed},
		{"PUT method -> 405", http.MethodPut, `{}`, http.StatusMethodNotAllowed},
		{"empty body -> 400", http.MethodPost, "", http.StatusBadRequest},
		{"malformed JSON -> 400", http.MethodPost, `not json`, http.StatusBadRequest},
		{"missing user_id -> 400", http.MethodPost, `{"user":{"email":"u@x"},"market":"IN"}`, http.StatusBadRequest},
		{"missing market -> 400", http.MethodPost, `{"user":{"email":"u@x","user_id":"human-42"}}`, http.StatusBadRequest},
		{"empty user_id -> 400", http.MethodPost, `{"user":{"email":"u@x","user_id":""},"market":"IN"}`, http.StatusBadRequest},
		{"empty market -> 400", http.MethodPost, `{"user":{"email":"u@x","user_id":"human-42"},"market":""}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, broker := newHandlerHarness(t)
			installLiveCanonicalMarket(t, conductor, "IN")

			recorder := postJoin(t, conductor, tc.method, tc.body)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q",
					recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			// Validation rejects must short-circuit before any broker I/O.
			if broker.ensureAccountCount() != 0 || broker.startSessionCount() != 0 {
				t.Errorf("broker was called despite request-validation reject: ensure=%d start=%d",
					broker.ensureAccountCount(), broker.startSessionCount())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- Conductor-side 409s ---------------------------------------------

func TestHandleJoinCanonical_NoLiveMarketReturns409(t *testing.T) {
	conductor, _ := newHandlerHarness(t)
	// No market installed.

	recorder := postJoin(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"},"market":"IN"}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
}

func TestHandleJoinCanonical_AlreadyInSessionReturns409(t *testing.T) {
	conductor, broker := newHandlerHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")

	// First join succeeds, second one hits the per-user XOR front-gate.
	firstRecorder := postJoin(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"},"market":"IN"}`)
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("first join status = %d, want 200", firstRecorder.Code)
	}

	secondRecorder := postJoin(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"},"market":"IN"}`)
	if secondRecorder.Code != http.StatusConflict {
		t.Fatalf("second join status = %d, want 409. body=%q",
			secondRecorder.Code, secondRecorder.Body.String())
	}
	// The front-gate must have refused the second attempt before any new
	// broker I/O.
	if broker.ensureAccountCount() != 1 || broker.startSessionCount() != 1 {
		t.Errorf("broker call counts: ensure=%d start=%d, want 1 each", broker.ensureAccountCount(), broker.startSessionCount())
	}
	assertErrorBody(t, secondRecorder)
}

// --- Broker-error → HTTP status (the §6.3 table) --------------------

// TestHandleJoinCanonical_BrokerErrorStatus walks the broker-error
// branches end-to-end so every row of the status table is proven
// through the handler, not just the mapper.
func TestHandleJoinCanonical_BrokerErrorStatus(t *testing.T) {
	type brokerError struct {
		name     string
		stage    string // "ensure" or "start"
		err      error
		wantCode int
	}
	cases := []brokerError{
		// EnsureAccount paths — all 500.
		{"ensure lifecycle.ErrBadOwnerType", "ensure", lifecycle.ErrBadOwnerType, http.StatusInternalServerError},
		{"ensure lifecycle.ErrBadOwnerID", "ensure", lifecycle.ErrBadOwnerID, http.StatusInternalServerError},
		{"ensure lifecycle.ErrCreateFailed", "ensure", lifecycle.ErrCreateFailed, http.StatusInternalServerError},
		{"ensure store.ErrNoAccount", "ensure", store.ErrNoAccount, http.StatusInternalServerError},
		{"ensure store.ErrLoadFailed", "ensure", store.ErrLoadFailed, http.StatusInternalServerError},

		// StartSession 409 — broker XOR backstop.
		{"start funnel.ErrSessionAlreadyLive", "start", funnel.ErrSessionAlreadyLive, http.StatusConflict},

		// StartSession 504 — shard reply didn't arrive.
		{"start funnel.ErrBrokerTimeout", "start", funnel.ErrBrokerTimeout, http.StatusGatewayTimeout},

		// StartSession 503 — durable load / snapshot failures.
		{"start lifecycle.ErrNoAccount", "start", lifecycle.ErrNoAccount, http.StatusServiceUnavailable},
		{"start lifecycle.ErrLoadFailed", "start", lifecycle.ErrLoadFailed, http.StatusServiceUnavailable},
		{"start lifecycle.ErrSnapshotWriteFailed", "start", lifecycle.ErrSnapshotWriteFailed, http.StatusServiceUnavailable},

		// StartSession 500 — conductor-bug seams.
		{"start funnel.ErrBadBinding", "start", funnel.ErrBadBinding, http.StatusInternalServerError},
		{"start funnel.ErrBadParticipant", "start", funnel.ErrBadParticipant, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, broker := newHandlerHarness(t)
			installLiveCanonicalMarket(t, conductor, "IN")

			switch tc.stage {
			case "ensure":
				broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
					return "", tc.err
				}
			case "start":
				broker.startSessionFunc = func(context.Context, funnel.SessionStartCmd) error {
					return tc.err
				}
			}

			recorder := postJoin(t, conductor, http.MethodPost,
				`{"user":{"email":"u@x","user_id":"human-42"},"market":"IN"}`)
			if recorder.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d. body=%q",
					recorder.Code, tc.wantCode, recorder.Body.String())
			}
			assertErrorBody(t, recorder)

			// Registry residue must be zero on every error path.
			conductor.reg.mu.Lock()
			sessionsCount := len(conductor.reg.sessions)
			byUserCount := len(conductor.reg.byUser)
			conductor.reg.mu.Unlock()
			if sessionsCount != 0 || byUserCount != 0 {
				t.Errorf("rollback incomplete: sessions=%d byUser=%d", sessionsCount, byUserCount)
			}
		})
	}
}

// --- Handler-owned deadline surfaces as 504 ---------------------------

// TestHandleJoinCanonical_HandlerTimeoutYields504 wires a slow fake
// StartSession and a tiny JoinTimeout; the handler-owned context must
// fire before StartSession returns, StartSession must observe the ctx
// error and surface funnel.ErrBrokerTimeout, and the mapper must land
// on 504 — proving the handler → flow → broker deadline plumbing is
// correct end-to-end.
func TestHandleJoinCanonical_HandlerTimeoutYields504(t *testing.T) {
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := &fakeJoinBroker{}
	broker.startSessionFunc = func(ctx context.Context, _ funnel.SessionStartCmd) error {
		select {
		case <-ctx.Done():
			return funnel.ErrBrokerTimeout
		case <-time.After(5 * time.Second):
			return nil
		}
	}
	conductor, err := NewConductor(Config{
		CfgDir:      t.TempDir(),
		OHLCVPool:   pool,
		Broker:      broker,
		Market:      stubMarketDataPort{},
		JoinTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	installLiveCanonicalMarket(t, conductor, "IN")

	start := time.Now()
	recorder := postJoin(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"},"market":"IN"}`)
	elapsed := time.Since(start)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504. body=%q", recorder.Code, recorder.Body.String())
	}
	if elapsed > 2*time.Second {
		t.Errorf("handler took %v, far longer than the 50ms join timeout — context not plumbed", elapsed)
	}
}

// --- Panic recovery returns 500 --------------------------------------

func TestHandleJoinCanonical_PanicReturns500(t *testing.T) {
	conductor, broker := newHandlerHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")

	// Force a panic inside the flow — recoverPanic must catch it and
	// write a clean 500, not take the server down.
	broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
		panic("induced panic for recovery test")
	}

	recorder := postJoin(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"},"market":"IN"}`)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
	// The panic landed after reserve — the handler's rollback never ran,
	// so registry residue is expected (the Starting session persists until
	// a reap / teardown passes over it). The point of this test is that
	// the SERVER stayed up and the CLIENT got a status; residue is out
	// of scope here.
}

// --- MountRoutes registers /session/canonical -----------------------

func TestMountRoutes_RegistersSessionCanonical(t *testing.T) {
	conductor, _ := newHandlerHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")

	mux := http.NewServeMux()
	conductor.MountRoutes(mux)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Happy POST through the mux.
	response, err := http.Post(
		server.URL+PathSessionCanonical,
		"application/json",
		strings.NewReader(`{"user":{"email":"u@x","user_id":"human-via-mux"},"market":"IN"}`),
	)
	if err != nil {
		t.Fatalf("POST through mux: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mux POST status = %d, want 200", response.StatusCode)
	}

	// Liveness probe must still work — no regression on the pre-existing
	// route.
	livenessResponse, err := http.Get(server.URL + LivenessPath)
	if err != nil {
		t.Fatalf("GET liveness: %v", err)
	}
	defer livenessResponse.Body.Close()
	if livenessResponse.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d, want 200", livenessResponse.StatusCode)
	}
}

// --- shared helpers --------------------------------------------------

// assertErrorBody checks that a non-2xx response carries the {"error":
// "..."} envelope with a non-empty message.
func assertErrorBody(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
		return
	}
	var body errorBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.Error == "" {
		t.Error("error body has empty .error field")
	}
}

