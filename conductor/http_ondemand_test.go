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
	"simulator/economy/lifecycle"
)

// --- harness helpers --------------------------------------------------

// postOnDemand issues a POST to handleStartOnDemand directly (bypasses
// the mux so each test can run without MountRoutes) with the given
// method, raw query string (no leading '?'), and request body.
func postOnDemand(t *testing.T, conductor *Conductor, method string, rawQuery string, body string) *httptest.ResponseRecorder {
	t.Helper()
	target := PathSessionOnDemand
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	conductor.handleStartOnDemand(recorder, request)
	return recorder
}

// standardOnDemandBody is the body shape every test that reaches the
// flow uses. Takes the userID so XOR tests can distinguish callers.
func standardOnDemandBody(userID string) string {
	return `{"user":{"email":"u@x","user_id":"` + userID + `"}}`
}

// --- parseOnDemandDuration unit table ---------------------------------

func TestParseOnDemandDuration(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		want    int
		wantErr bool
	}{
		{"absent uses default 60", "", 60, false},
		{"minimum 5 accepted", "5", 5, false},
		{"maximum 120 accepted", "120", 120, false},
		{"middle 60 accepted", "60", 60, false},
		{"four is below min", "4", 0, true},
		{"121 is above max", "121", 0, true},
		{"zero fails as below min", "0", 0, true},
		{"negative fails", "-5", 0, true},
		{"non-integer with unit fails", "5m", 0, true},
		{"fractional fails", "1.5", 0, true},
		{"non-numeric fails", "abc", 0, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parseOnDemandDuration(testCase.query)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("parseOnDemandDuration(%q) = %d, nil — want error", testCase.query, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOnDemandDuration(%q) = _, %v — want nil", testCase.query, err)
			}
			if got != testCase.want {
				t.Errorf("parseOnDemandDuration(%q) = %d, want %d", testCase.query, got, testCase.want)
			}
		})
	}
}

// --- Happy path -------------------------------------------------------

func TestHandleStartOnDemand_HappyPath(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=35", standardOnDemandBody("human-happy"))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var body onDemandWarmingUpBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode warming_up body: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.Status != "warming_up" {
		t.Errorf("status = %q, want \"warming_up\"", body.Status)
	}
	if !isUUIDv4(body.SessionID) {
		t.Errorf("session_id %q is not a UUID v4", body.SessionID)
	}

	warmupEndsAt, warmupParseErr := time.Parse(time.RFC3339Nano, body.WarmupEndsAt)
	if warmupParseErr != nil {
		t.Fatalf("warmup_ends_at parse: %v (raw=%q)", warmupParseErr, body.WarmupEndsAt)
	}
	sessionEndsAt, sessionParseErr := time.Parse(time.RFC3339Nano, body.SessionEndsAt)
	if sessionParseErr != nil {
		t.Fatalf("session_ends_at parse: %v (raw=%q)", sessionParseErr, body.SessionEndsAt)
	}
	wantWarmupEnds := harness.fixedNow.Add(configuredWarmupLength)
	wantSessionEnds := wantWarmupEnds.Add(35 * time.Minute)
	if !warmupEndsAt.Equal(wantWarmupEnds) {
		t.Errorf("warmup_ends_at = %s, want %s", warmupEndsAt, wantWarmupEnds)
	}
	if !sessionEndsAt.Equal(wantSessionEnds) {
		t.Errorf("session_ends_at = %s, want %s", sessionEndsAt, wantSessionEnds)
	}
}

// --- Insufficient funds (gate NO) → 200 insufficient_funds body ------

func TestHandleStartOnDemand_InsufficientFunds(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.broker.qualifyOnDemandFunc = func(context.Context, funnel.AccountID) (bool, error) {
		return false, nil
	}
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-poor"))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (gate NO is a soft reject). body=%q",
			recorder.Code, recorder.Body.String())
	}
	var body onDemandInsufficientFundsBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode insufficient_funds body: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.Status != "insufficient_funds" {
		t.Errorf("status = %q, want \"insufficient_funds\"", body.Status)
	}
	if body.Message == "" {
		t.Error("message empty on insufficient_funds")
	}
}

// --- Request validation table (405 / 400) ----------------------------

func TestHandleStartOnDemand_RequestValidation(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		rawQuery   string
		body       string
		wantStatus int
	}{
		{"GET method -> 405", http.MethodGet, "market=IN&duration=30", "", http.StatusMethodNotAllowed},
		{"PUT method -> 405", http.MethodPut, "market=IN&duration=30", `{}`, http.StatusMethodNotAllowed},
		{"missing market -> 400", http.MethodPost, "duration=30", standardOnDemandBody("human-x"), http.StatusBadRequest},
		{"empty market value -> 400", http.MethodPost, "market=&duration=30", standardOnDemandBody("human-x"), http.StatusBadRequest},
		{"duration=4 (below min) -> 400", http.MethodPost, "market=IN&duration=4", standardOnDemandBody("human-x"), http.StatusBadRequest},
		{"duration=121 (above max) -> 400", http.MethodPost, "market=IN&duration=121", standardOnDemandBody("human-x"), http.StatusBadRequest},
		{"duration=0 -> 400", http.MethodPost, "market=IN&duration=0", standardOnDemandBody("human-x"), http.StatusBadRequest},
		{"duration=abc -> 400", http.MethodPost, "market=IN&duration=abc", standardOnDemandBody("human-x"), http.StatusBadRequest},
		{"malformed JSON body -> 400", http.MethodPost, "market=IN&duration=30", `not json`, http.StatusBadRequest},
		{"empty body -> 400", http.MethodPost, "market=IN&duration=30", "", http.StatusBadRequest},
		{"missing user_id -> 400", http.MethodPost, "market=IN&duration=30", `{"user":{"email":"u@x"}}`, http.StatusBadRequest},
		{"empty user_id -> 400", http.MethodPost, "market=IN&duration=30", `{"user":{"user_id":""}}`, http.StatusBadRequest},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newOnDemandHarness(t, "IN")
			recorder := postOnDemand(t, harness.conductor, testCase.method, testCase.rawQuery, testCase.body)
			if recorder.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q",
					recorder.Code, testCase.wantStatus, recorder.Body.String())
			}
			// Pre-flow rejects must never reach the broker.
			if harness.broker.ensureAccountCount() != 0 || harness.broker.qualifyOnDemandCount() != 0 ||
				harness.broker.startSessionCount() != 0 {
				t.Errorf("broker called despite validation reject: ensure=%d qualify=%d start=%d",
					harness.broker.ensureAccountCount(),
					harness.broker.qualifyOnDemandCount(),
					harness.broker.startSessionCount())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- Duration edge values (5 and 120 inclusive) ---------------------

func TestHandleStartOnDemand_DurationEdgesAccepted(t *testing.T) {
	cases := []struct {
		name            string
		rawQuery        string
		expectedMinutes int
	}{
		{"minimum 5 minutes", "market=IN&duration=5", 5},
		{"maximum 120 minutes", "market=IN&duration=120", 120},
		{"duration absent defaults to 60", "market=IN", 60},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newOnDemandHarness(t, "IN")
			recorder := postOnDemand(t, harness.conductor, http.MethodPost,
				testCase.rawQuery, standardOnDemandBody("human-edge"))
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
			}
			var body onDemandWarmingUpBody
			if decodeErr := json.Unmarshal(recorder.Body.Bytes(), &body); decodeErr != nil {
				t.Fatalf("decode: %v", decodeErr)
			}
			warmupEndsAt, _ := time.Parse(time.RFC3339Nano, body.WarmupEndsAt)
			sessionEndsAt, _ := time.Parse(time.RFC3339Nano, body.SessionEndsAt)
			delta := sessionEndsAt.Sub(warmupEndsAt)
			wantDelta := time.Duration(testCase.expectedMinutes) * time.Minute
			if delta != wantDelta {
				t.Errorf("session span = %v, want %v", delta, wantDelta)
			}
		})
	}
}

// --- XOR conflict → 409 ---------------------------------------------

func TestHandleStartOnDemand_XORReturns409(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	first := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-xor"))
	if first.Code != http.StatusOK {
		t.Fatalf("first POST status = %d, want 200. body=%q", first.Code, first.Body.String())
	}

	second := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-xor"))
	if second.Code != http.StatusConflict {
		t.Fatalf("second POST status = %d, want 409. body=%q", second.Code, second.Body.String())
	}
	assertErrorBody(t, second)
	// Front-gate short-circuited before any new broker I/O: the second
	// call did NOT reach the broker.
	if harness.broker.ensureAccountCount() != 1 {
		t.Errorf("ensure_account calls = %d, want 1 (second call must not reach broker)",
			harness.broker.ensureAccountCount())
	}
}

// --- Install failure after Start → 503 failed body ------------------

func TestHandleStartOnDemand_InstallFailedReturns503(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.broker.startSessionFunc = func(context.Context, funnel.SessionStartCmd) error {
		return funnel.ErrBrokerTimeout
	}
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-install-fail"))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. body=%q", recorder.Code, recorder.Body.String())
	}
	var body onDemandFailedBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode failed body: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.Status != "failed" {
		t.Errorf("status = %q, want \"failed\"", body.Status)
	}
	if body.Reason == "" {
		t.Error("reason empty on failed body")
	}
}

// --- Populate failure before construct → 503 failed body ------------

func TestHandleStartOnDemand_PopulateFailedReturns503(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.agents.returnErr = ErrOrchestratorUnavailable
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-populate-fail"))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. body=%q", recorder.Code, recorder.Body.String())
	}
	var body onDemandFailedBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode failed body: %v", err)
	}
	if body.Status != "failed" {
		t.Errorf("status = %q, want \"failed\"", body.Status)
	}
	if !strings.Contains(body.Reason, "orchestrator unavailable") {
		t.Errorf("reason = %q, want to include orchestrator-unavailable detail", body.Reason)
	}
}

// --- Handler timeout: setupTimeout fires → 503 ----------------------

// TestHandleStartOnDemand_TimeoutYields503 proves the context plumbing
// handler → flow → broker is end-to-end correct. A deliberately tight
// SetupTimeout (50ms) is set on the Conductor; the fake StartSession
// observes ctx.Done and returns funnel.ErrBrokerTimeout wrapped by
// the ctx error path. If the ctx didn't thread all the way through,
// the fake would block indefinitely and the test would hit its own
// 2s guard — timing out, not asserting.
func TestHandleStartOnDemand_TimeoutYields503(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.conductor.setupTimeout = 50 * time.Millisecond
	harness.broker.startSessionFunc = func(ctx context.Context, _ funnel.SessionStartCmd) error {
		select {
		case <-ctx.Done():
			return funnel.ErrBrokerTimeout
		case <-time.After(2 * time.Second):
			t.Error("StartSession ctx did not fire within setupTimeout window")
			return nil
		}
	}

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- postOnDemand(t, harness.conductor, http.MethodPost,
			"market=IN&duration=30", standardOnDemandBody("human-timeout"))
	}()

	select {
	case recorder := <-done:
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503. body=%q", recorder.Code, recorder.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return within 2s — ctx not threaded through to StartSession")
	}
}

// --- Conductor-side bug: ErrBadBinding / ErrBadParticipant → 500 ----

// TestHandleStartOnDemand_ConductorBugReturns500 pins the mapper's
// classification of broker sentinels that can only fire if the
// conductor filled SessionStartCmd incorrectly. StartOnDemand wraps
// every post-reserve failure with ErrOnDemandSetupFailed (which maps
// to 503), so the mapper has to inspect for these programming-error
// sentinels BEFORE the setup bucket catches them — errors.Is on the
// double-wrap matches both. Response shape is the plain errorBody
// (not the failed/reason envelope) because 500 is a programmer-fault
// class, not an infra class.
func TestHandleStartOnDemand_ConductorBugReturns500(t *testing.T) {
	cases := []error{
		funnel.ErrBadBinding,
		funnel.ErrBadParticipant,
	}
	for _, forcedErr := range cases {
		t.Run(forcedErr.Error(), func(t *testing.T) {
			harness := newOnDemandHarness(t, "IN")
			harness.broker.startSessionFunc = func(context.Context, funnel.SessionStartCmd) error {
				return forcedErr
			}
			recorder := postOnDemand(t, harness.conductor, http.MethodPost,
				"market=IN&duration=30", standardOnDemandBody("human-bug"))
			if recorder.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (conductor-bug sentinel). body=%q",
					recorder.Code, recorder.Body.String())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- Panic recovery → 500 clean --------------------------------------

func TestHandleStartOnDemand_PanicReturns500(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
		panic("deliberate test panic inside EnsureAccount")
	}
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-panic"))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500. body=%q", recorder.Code, recorder.Body.String())
	}
	var body errorBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body after panic: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.Error != "internal error" {
		t.Errorf("error body = %q, want \"internal error\" (recoverPanic default)", body.Error)
	}
}

// --- Unknown market tag → 400 ---------------------------------------

// TestHandleStartOnDemand_UnknownTagReturns400 verifies loadConfig's
// file-not-found error descends through fmt.Errorf's %w wrap and
// surfaces to the mapper as fs.ErrNotExist, which routes to 400 (the
// client sent a tag that doesn't exist in config).
func TestHandleStartOnDemand_UnknownTagReturns400(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=ZZZ&duration=30", standardOnDemandBody("human-badtag"))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (unknown tag). body=%q",
			recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
	// Registry must be untouched — load-config fails before reserve.
	harness.conductor.reg.mu.Lock()
	defer harness.conductor.reg.mu.Unlock()
	if len(harness.conductor.reg.sessions) != 0 || len(harness.conductor.reg.markets) != 0 {
		t.Errorf("registry mutated on unknown-tag path: sessions=%d markets=%d",
			len(harness.conductor.reg.sessions), len(harness.conductor.reg.markets))
	}
}

// --- MountRoutes registers /session/ondemand ------------------------

func TestMountRoutes_RegistersOnDemand(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	mux := http.NewServeMux()
	harness.conductor.MountRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := http.Post(
		server.URL+PathSessionOnDemand+"?market=IN&duration=30",
		"application/json",
		strings.NewReader(standardOnDemandBody("human-mux")),
	)
	if err != nil {
		t.Fatalf("POST through mux: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mux POST status = %d, want 200", response.StatusCode)
	}

	// No regression on the pre-existing routes.
	livenessResp, livenessErr := http.Get(server.URL + LivenessPath)
	if livenessErr != nil {
		t.Fatalf("liveness GET: %v", livenessErr)
	}
	defer livenessResp.Body.Close()
	if livenessResp.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d, want 200 (route regression)", livenessResp.StatusCode)
	}
}
