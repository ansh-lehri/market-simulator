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

// --- parsePracticeCondition unit table ---------------------------------

func TestParsePracticeCondition(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{"empty is legal (no condition requested)", "", "", false},
		{"normal accepted", "normal", "normal", false},
		{"calm accepted", "calm", "calm", false},
		{"busy accepted", "busy", "busy", false},
		{"thin accepted", "thin", "thin", false},
		{"dispersion accepted", "dispersion", "dispersion", false},
		{"unknown value rejected", "volcano", "", true},
		{"wrong case rejected", "Busy", "", true},
	}
	allowed := buildPracticeConditionSet(defaultPracticeConditions)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parsePracticeCondition(testCase.raw, allowed)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("parsePracticeCondition(%q) = %q, nil — want error", testCase.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePracticeCondition(%q) = _, %v — want nil", testCase.raw, err)
			}
			if got != testCase.want {
				t.Errorf("parsePracticeCondition(%q) = %q, want %q", testCase.raw, got, testCase.want)
			}
		})
	}
}

// --- condition field end-to-end through the HTTP handler ---------------

func TestHandleStartOnDemand_UnknownConditionReturns400(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	body := `{"user":{"user_id":"human-cond-bad"},"condition":"volcano"}`
	recorder := postOnDemand(t, harness.conductor, http.MethodPost, "market=IN&duration=30", body)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
	if harness.agents.callCount != 0 {
		t.Errorf("agents.BringUp calls = %d, want 0 (bad condition must reject before setup)", harness.agents.callCount)
	}
}

func TestHandleStartOnDemand_ConditionReachesAgentBringUpRequest(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	body := `{"user":{"user_id":"human-cond-good"},"condition":"dispersion"}`
	recorder := postOnDemand(t, harness.conductor, http.MethodPost, "market=IN&duration=30", body)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	gotRequest := harness.agents.lastRequestSnapshot()
	if gotRequest.Scenario != "dispersion" {
		t.Errorf("AgentBringUpRequest.Scenario = %q, want %q", gotRequest.Scenario, "dispersion")
	}
}

// TestHandleStartOnDemand_PracticeConditionAllowlistIsConfigurable
// proves Config.PracticeConditions genuinely overrides the default set
// — the whole reason this isn't a fixed package-level list — by
// building a Conductor with a custom, single-value allowlist and
// checking that: (a) a value outside it is rejected even though it's
// one of the contract's five default values, and (b) the one value
// the override DOES allow is accepted even though it is not in
// defaultPracticeConditions at all.
func TestHandleStartOnDemand_PracticeConditionAllowlistIsConfigurable(t *testing.T) {
	marketTag := MarketTag("IN")
	cfgDir := t.TempDir()
	writeTestMarketConfig(t, cfgDir, marketTag)

	sequencer := &callSequencer{}
	broker := &fakeOnDemandBroker{fakeJoinBroker: &fakeJoinBroker{}}
	market := &fakeOnDemandMarket{fanoutMarketTag: marketTag}
	agents := &recordingAgents{sequencer: sequencer}
	liquidity := &recordingLiquidity{sequencer: sequencer}
	exchFactory := &recordingExchangeFactory{sequencer: sequencer}
	pool := newUnconnectedPool(t)

	customCondition := "beta_test_only"
	conductor, err := NewConductor(Config{
		CfgDir:             cfgDir,
		OHLCVPool:          pool,
		Broker:             broker,
		Market:             market,
		Exchange:           exchFactory,
		Agents:             agents,
		Liquidity:          liquidity,
		Now:                func() time.Time { return onDemandFixedNow },
		PracticeConditions: []string{customCondition},
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	conductor.referenceReader = readerReturningClose(251575)
	t.Cleanup(func() {
		exchFactory.stopAll()
		broker.wireDrainWG.Wait()
		market.wireDrainWG.Wait()
		conductor.Stop()
	})

	// A default-list value ("busy") is rejected because the override
	// replaced the whole list, not appended to it.
	rejected := postOnDemand(t, conductor, http.MethodPost,
		"market=IN&duration=30", `{"user":{"user_id":"human-override-1"},"condition":"busy"}`)
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("busy against custom allowlist: status = %d, want 400. body=%q",
			rejected.Code, rejected.Body.String())
	}

	// The override's own value is accepted and still reaches Scenario.
	accepted := postOnDemand(t, conductor, http.MethodPost,
		"market=IN&duration=30", `{"user":{"user_id":"human-override-2"},"condition":"beta_test_only"}`)
	if accepted.Code != http.StatusOK {
		t.Fatalf("custom condition: status = %d, want 200. body=%q", accepted.Code, accepted.Body.String())
	}
	if got := agents.lastRequestSnapshot().Scenario; got != customCondition {
		t.Errorf("AgentBringUpRequest.Scenario = %q, want %q", got, customCondition)
	}
}

func TestHandleStartOnDemand_OmittedConditionLeavesScenarioEmpty(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	recorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=30", standardOnDemandBody("human-cond-none"))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	gotRequest := harness.agents.lastRequestSnapshot()
	if gotRequest.Scenario != "" {
		t.Errorf("AgentBringUpRequest.Scenario = %q, want empty (condition omitted)", gotRequest.Scenario)
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

// --- GET /session/status helpers --------------------------------------

// getSessionStatus issues a GET to handleSessionStatus directly with
// the given query string (no leading '?').
func getSessionStatus(t *testing.T, conductor *Conductor, method string, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	target := PathSessionStatus
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	request := httptest.NewRequest(method, target, nil)
	recorder := httptest.NewRecorder()
	conductor.handleSessionStatus(recorder, request)
	return recorder
}

// installSessionDirect places a Session (and optional Market) in the
// registry without walking the real flow — so each status test can
// program its exact pre-condition (Starting with no Market, Starting
// with Market, Active, Failed, etc.) without time-dependent setup.
func installSessionDirect(
	t *testing.T,
	conductor *Conductor,
	sessionState SessionState,
	failReason string,
	withMarket bool,
	warmupEndsAt time.Time,
	closeAt time.Time,
) SessionID {
	t.Helper()
	sessionID := SessionID(newUUIDv4())
	userID := UserID("human-" + newUUIDv4()[:8])
	marketID := MarketID("od:" + string(sessionID))
	session := &Session{
		ID:          sessionID,
		User:        userID,
		Market:      marketID,
		AccountType: Ephemeral,
		State:       sessionState,
		FailReason:  failReason,
	}
	conductor.reg.mu.Lock()
	conductor.reg.sessions[sessionID] = session
	conductor.reg.byUser[userID] = session
	if withMarket {
		conductor.reg.markets[marketID] = &Market{
			ID:           marketID,
			Mode:         OnDemand,
			Tag:          "IN",
			State:        Live,
			warmupEndsAt: warmupEndsAt,
			closeAt:      closeAt,
			members:      map[SessionID]struct{}{sessionID: {}},
		}
	}
	conductor.reg.mu.Unlock()
	return sessionID
}

// decodeSessionStatus parses the JSON response body into a
// sessionStatusBody. Fatals with a readable message on decode error.
func decodeSessionStatus(t *testing.T, recorder *httptest.ResponseRecorder) sessionStatusBody {
	t.Helper()
	var body sessionStatusBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode session status body: %v (raw=%q)", err, recorder.Body.String())
	}
	return body
}

// --- Starting session with no Market record --------------------------

// TestHandleSessionStatus_StartingWithoutMarket covers the brief
// window between the per-user reservation and the Market-create lock
// take: the Session exists in Starting state, but no Market record
// has been reserved yet. The handler must return warming_up with no
// times (omitempty hides the two zero fields).
func TestHandleSessionStatus_StartingWithoutMarket(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	sessionID := installSessionDirect(t, harness.conductor, Starting, "", false, time.Time{}, time.Time{})

	recorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session="+string(sessionID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	body := decodeSessionStatus(t, recorder)
	if body.Status != sessionStatusLabelWarmingUp {
		t.Errorf("status = %q, want %q", body.Status, sessionStatusLabelWarmingUp)
	}
	if body.WarmupEndsAt != "" || body.SessionEndsAt != "" {
		t.Errorf("times must be omitted when Market record is absent: warmup=%q session=%q",
			body.WarmupEndsAt, body.SessionEndsAt)
	}
	if body.Reason != "" {
		t.Errorf("reason must be empty on non-failed status; got %q", body.Reason)
	}
}

// --- Starting session with Market times already stamped --------------

func TestHandleSessionStatus_StartingWithMarket(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	warmupEndsAt := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	closeAt := warmupEndsAt.Add(30 * time.Minute)
	sessionID := installSessionDirect(t, harness.conductor, Starting, "", true, warmupEndsAt, closeAt)

	recorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session="+string(sessionID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	body := decodeSessionStatus(t, recorder)
	if body.Status != sessionStatusLabelWarmingUp {
		t.Errorf("status = %q, want %q", body.Status, sessionStatusLabelWarmingUp)
	}
	gotWarmupEnds, _ := time.Parse(time.RFC3339Nano, body.WarmupEndsAt)
	gotSessionEnds, _ := time.Parse(time.RFC3339Nano, body.SessionEndsAt)
	if !gotWarmupEnds.Equal(warmupEndsAt) {
		t.Errorf("warmup_ends_at = %s, want %s", gotWarmupEnds, warmupEndsAt)
	}
	if !gotSessionEnds.Equal(closeAt) {
		t.Errorf("session_ends_at = %s, want %s", gotSessionEnds, closeAt)
	}
}

// --- Active session (post-finalize) → ready + times -----------------

func TestHandleSessionStatus_Active(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	warmupEndsAt := time.Date(2026, 10, 12, 11, 38, 47, 500_000_000, time.UTC)
	closeAt := warmupEndsAt.Add(45 * time.Minute)
	sessionID := installSessionDirect(t, harness.conductor, Active, "", true, warmupEndsAt, closeAt)

	recorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session="+string(sessionID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	body := decodeSessionStatus(t, recorder)
	if body.Status != sessionStatusLabelReady {
		t.Errorf("status = %q, want %q", body.Status, sessionStatusLabelReady)
	}
	gotWarmupEnds, _ := time.Parse(time.RFC3339Nano, body.WarmupEndsAt)
	gotSessionEnds, _ := time.Parse(time.RFC3339Nano, body.SessionEndsAt)
	if !gotWarmupEnds.Equal(warmupEndsAt) {
		t.Errorf("warmup_ends_at = %s, want %s", gotWarmupEnds, warmupEndsAt)
	}
	if !gotSessionEnds.Equal(closeAt) {
		t.Errorf("session_ends_at = %s, want %s", gotSessionEnds, closeAt)
	}
	if body.Reason != "" {
		t.Errorf("reason must be empty on ready status; got %q", body.Reason)
	}
}

// --- Failed session → failed + reason, no times ---------------------

func TestHandleSessionStatus_Failed(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	// Failed sessions have no Market record (failOnDemand deletes it);
	// install without the market so the response omits the two time
	// fields even though the FailReason is populated.
	failReason := "install_failed: funnel: request timed out"
	sessionID := installSessionDirect(t, harness.conductor, Failed, failReason, false, time.Time{}, time.Time{})

	recorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session="+string(sessionID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	body := decodeSessionStatus(t, recorder)
	if body.Status != sessionStatusLabelFailed {
		t.Errorf("status = %q, want %q", body.Status, sessionStatusLabelFailed)
	}
	if body.Reason != failReason {
		t.Errorf("reason = %q, want %q", body.Reason, failReason)
	}
	if body.WarmupEndsAt != "" || body.SessionEndsAt != "" {
		t.Errorf("times must be omitted on failed status: warmup=%q session=%q",
			body.WarmupEndsAt, body.SessionEndsAt)
	}
}

// --- Unknown session → 404 ------------------------------------------

func TestHandleSessionStatus_UnknownSession(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	recorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session=never-existed-anywhere")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
}

// --- Missing session carrier → 400 ----------------------------------

func TestHandleSessionStatus_MissingSessionParam(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{"no query at all", ""},
		{"empty session value", "session="},
		{"other param only", "foo=bar"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newOnDemandHarness(t, "IN")
			recorder := getSessionStatus(t, harness.conductor, http.MethodGet, testCase.query)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400. body=%q", recorder.Code, recorder.Body.String())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- Non-GET → 405 ---------------------------------------------------

func TestHandleSessionStatus_NonGETReturns405(t *testing.T) {
	cases := []string{http.MethodPost, http.MethodPut, http.MethodDelete}
	for _, method := range cases {
		t.Run(method, func(t *testing.T) {
			harness := newOnDemandHarness(t, "IN")
			recorder := getSessionStatus(t, harness.conductor, method, "session=anything")
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405. body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

// --- Pure read: registry unchanged across the call -------------------

// TestHandleSessionStatus_PureRead proves the status endpoint has
// zero side effects on the registry — the map lengths and the stored
// Session record are byte-identical before and after. If a future
// edit accidentally slips a mutation into the handler (e.g. "touch
// the session with a last-read timestamp"), this test fails loudly.
func TestHandleSessionStatus_PureRead(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	warmupEndsAt := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	closeAt := warmupEndsAt.Add(60 * time.Minute)
	sessionID := installSessionDirect(t, harness.conductor, Active, "", true, warmupEndsAt, closeAt)
	marketID := MarketID("od:" + string(sessionID))

	// Snapshot the registry before the call.
	harness.conductor.reg.mu.Lock()
	preSessionsCount := len(harness.conductor.reg.sessions)
	preMarketsCount := len(harness.conductor.reg.markets)
	preByUserCount := len(harness.conductor.reg.byUser)
	preSessionCopy := *harness.conductor.reg.sessions[sessionID]
	preMarketWarmup := harness.conductor.reg.markets[marketID].warmupEndsAt
	preMarketClose := harness.conductor.reg.markets[marketID].closeAt
	preMarketState := harness.conductor.reg.markets[marketID].State
	harness.conductor.reg.mu.Unlock()

	// Issue the status read.
	recorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session="+string(sessionID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status read: status = %d, body=%q", recorder.Code, recorder.Body.String())
	}

	// Snapshot after and compare.
	harness.conductor.reg.mu.Lock()
	defer harness.conductor.reg.mu.Unlock()
	if len(harness.conductor.reg.sessions) != preSessionsCount {
		t.Errorf("sessions count mutated: before=%d after=%d",
			preSessionsCount, len(harness.conductor.reg.sessions))
	}
	if len(harness.conductor.reg.markets) != preMarketsCount {
		t.Errorf("markets count mutated: before=%d after=%d",
			preMarketsCount, len(harness.conductor.reg.markets))
	}
	if len(harness.conductor.reg.byUser) != preByUserCount {
		t.Errorf("byUser count mutated: before=%d after=%d",
			preByUserCount, len(harness.conductor.reg.byUser))
	}
	postSession := *harness.conductor.reg.sessions[sessionID]
	if postSession != preSessionCopy {
		t.Errorf("session record mutated by status read: before=%+v after=%+v",
			preSessionCopy, postSession)
	}
	postMarket := harness.conductor.reg.markets[marketID]
	if !postMarket.warmupEndsAt.Equal(preMarketWarmup) ||
		!postMarket.closeAt.Equal(preMarketClose) ||
		postMarket.State != preMarketState {
		t.Error("market record mutated by status read")
	}
}

// --- End-to-end: trigger → status chain ------------------------------

// TestHandleSessionStatus_EndToEndAfterStartOnDemand exercises the
// full integration: a real happy-path POST to /session/ondemand
// produces an Active session, then GET /session/status?session=<sid>
// returns "ready" with the same times the trigger returned. Proves
// the two endpoints agree on the shape of a live session.
func TestHandleSessionStatus_EndToEndAfterStartOnDemand(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	triggerRecorder := postOnDemand(t, harness.conductor, http.MethodPost,
		"market=IN&duration=42", standardOnDemandBody("human-e2e"))
	if triggerRecorder.Code != http.StatusOK {
		t.Fatalf("trigger status = %d, body=%q", triggerRecorder.Code, triggerRecorder.Body.String())
	}
	var triggerBody onDemandWarmingUpBody
	if err := json.Unmarshal(triggerRecorder.Body.Bytes(), &triggerBody); err != nil {
		t.Fatalf("decode trigger body: %v", err)
	}

	statusRecorder := getSessionStatus(t, harness.conductor, http.MethodGet, "session="+triggerBody.SessionID)
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("status read: status = %d, body=%q", statusRecorder.Code, statusRecorder.Body.String())
	}
	statusBody := decodeSessionStatus(t, statusRecorder)
	if statusBody.Status != sessionStatusLabelReady {
		t.Errorf("status = %q, want %q (post-finalize active session)",
			statusBody.Status, sessionStatusLabelReady)
	}
	if statusBody.WarmupEndsAt != triggerBody.WarmupEndsAt {
		t.Errorf("warmup_ends_at from status (%q) != from trigger (%q)",
			statusBody.WarmupEndsAt, triggerBody.WarmupEndsAt)
	}
	if statusBody.SessionEndsAt != triggerBody.SessionEndsAt {
		t.Errorf("session_ends_at from status (%q) != from trigger (%q)",
			statusBody.SessionEndsAt, triggerBody.SessionEndsAt)
	}
}

// --- MountRoutes registers /session/status --------------------------

func TestMountRoutes_RegistersSessionStatus(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	sessionID := installSessionDirect(t, harness.conductor, Active, "", true,
		time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 12, 13, 0, 0, 0, time.UTC),
	)
	mux := http.NewServeMux()
	harness.conductor.MountRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := http.Get(server.URL + PathSessionStatus + "?session=" + string(sessionID))
	if err != nil {
		t.Fatalf("GET through mux: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mux GET status = %d, want 200", response.StatusCode)
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
