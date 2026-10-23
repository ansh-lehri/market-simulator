package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// --- status mapper table --------------------------------------------

// TestAccountOpenStatusFor walks every row of the status table bare
// and %w-wrapped so the mapper's errors.Is dispatch is pinned against
// future regressions.
func TestAccountOpenStatusFor(t *testing.T) {
	type statusCase struct {
		name string
		err  error
		want int
	}
	cases := []statusCase{
		{"nil to 200", nil, http.StatusOK},

		// 400 — user_id validation that the handler's empty check missed.
		{"lifecycle.ErrBadOwnerID to 400", lifecycle.ErrBadOwnerID, http.StatusBadRequest},

		// 504 — handler-owned deadline fired.
		{"context.DeadlineExceeded to 504", context.DeadlineExceeded, http.StatusGatewayTimeout},

		// 503 — infra (retryable).
		{"store.ErrNoAccount to 503", store.ErrNoAccount, http.StatusServiceUnavailable},
		{"lifecycle.ErrCreateFailed to 503", lifecycle.ErrCreateFailed, http.StatusServiceUnavailable},

		// 500 — conductor bug / server misconfig.
		{"lifecycle.ErrBadOwnerType to 500", lifecycle.ErrBadOwnerType, http.StatusInternalServerError},
		{"lifecycle.ErrNegativeStartingCapital to 500", lifecycle.ErrNegativeStartingCapital, http.StatusInternalServerError},
		{"lifecycle.ErrNegativeStartingBank to 500", lifecycle.ErrNegativeStartingBank, http.StatusInternalServerError},

		// 500 — unknown error must never leak as client-fixable.
		{"unknown error to 500", errors.New("something unexpected"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name+" bare", func(t *testing.T) {
			if got := accountOpenStatusFor(tc.err); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
		if tc.err == nil {
			continue
		}
		t.Run(tc.name+" wrapped", func(t *testing.T) {
			wrapped := fmt.Errorf("conductor wrap: %w", tc.err)
			if got := accountOpenStatusFor(wrapped); got != tc.want {
				t.Errorf("wrapped got %d, want %d", got, tc.want)
			}
		})
	}
}

// --- harness --------------------------------------------------------

// postAccountOpen calls handleAccountOpen directly (bypasses the mux).
func postAccountOpen(t *testing.T, conductor *Conductor, method string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, PathAccountOpen, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	conductor.handleAccountOpen(recorder, request)
	return recorder
}

// assertRegistryEmpty verifies the handler left the registry untouched
// — the account-open endpoint is session-less, so sessions / byUser /
// markets must all be unchanged.
func assertRegistryEmpty(t *testing.T, conductor *Conductor) {
	t.Helper()
	conductor.reg.mu.Lock()
	defer conductor.reg.mu.Unlock()
	if len(conductor.reg.sessions) != 0 {
		t.Errorf("registry.sessions = %d, want 0 (account-open is session-less)", len(conductor.reg.sessions))
	}
	if len(conductor.reg.byUser) != 0 {
		t.Errorf("registry.byUser = %d, want 0 (account-open never touches per-user state)", len(conductor.reg.byUser))
	}
	if len(conductor.reg.markets) != 0 {
		t.Errorf("registry.markets = %d, want 0 (account-open never touches markets)", len(conductor.reg.markets))
	}
}

// --- happy path + idempotence (signup then login) --------------------

func TestHandleAccountOpen_HappyPathIdempotent(t *testing.T) {
	conductor, broker := newJoinHarness(t)

	firstRecorder := postAccountOpen(t, conductor, http.MethodPost,
		`{"user":{"email":"alice@example.com","user_id":"user-7f3c"}}`)
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("first POST status = %d, want 200. body=%q", firstRecorder.Code, firstRecorder.Body.String())
	}
	if contentType := firstRecorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	var firstResponse accountOpenResponseBody
	if err := json.Unmarshal(firstRecorder.Body.Bytes(), &firstResponse); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if firstResponse.AccountID == "" {
		t.Error("first response: account_id empty")
	}

	// Second call — login path. 200 again, same account_id (idempotent).
	secondRecorder := postAccountOpen(t, conductor, http.MethodPost,
		`{"user":{"email":"alice@example.com","user_id":"user-7f3c"}}`)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("second POST status = %d, want 200 (login path must also be 200)", secondRecorder.Code)
	}
	var secondResponse accountOpenResponseBody
	_ = json.Unmarshal(secondRecorder.Body.Bytes(), &secondResponse)
	if secondResponse.AccountID != firstResponse.AccountID {
		t.Errorf("account_id drift: first=%q second=%q (idempotent EnsureAccount must return same handle)",
			firstResponse.AccountID, secondResponse.AccountID)
	}

	// Broker must have been called twice — fake EnsureAccount is
	// deterministic per owner_id, so the frontend's "signup then login"
	// pattern materializes as two calls that resolve to the same id.
	if broker.ensureAccountCount() != 2 {
		t.Errorf("ensure_account count = %d, want 2", broker.ensureAccountCount())
	}
	// Owner type is always OwnerHuman — admission-trusted identity.
	for i, inv := range broker.ensureAccountInvocations {
		if inv.OwnerType != lifecycle.OwnerHuman {
			t.Errorf("call[%d] owner_type = %v, want OwnerHuman", i, inv.OwnerType)
		}
		if inv.OwnerID != "user-7f3c" {
			t.Errorf("call[%d] owner_id = %q, want %q", i, inv.OwnerID, "user-7f3c")
		}
	}
	assertRegistryEmpty(t, conductor)
}

// --- owner_type resolution -------------------------------------------

func TestHandleAccountOpen_OwnerTypeAgent(t *testing.T) {
	conductor, broker := newJoinHarness(t)

	recorder := postAccountOpen(t, conductor, http.MethodPost,
		`{"user":{"user_id":"agent-7f3c","owner_type":"agent"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if broker.ensureAccountCount() != 1 {
		t.Fatalf("ensure_account count = %d, want 1", broker.ensureAccountCount())
	}
	if inv := broker.ensureAccountInvocations[0]; inv.OwnerType != lifecycle.OwnerAgent {
		t.Errorf("owner_type = %v, want OwnerAgent", inv.OwnerType)
	}
}

// --- request validation --------------------------------------------

func TestHandleAccountOpen_RequestValidation(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		body       string
		wantStatus int
	}{
		{"GET -> 405", http.MethodGet, "", http.StatusMethodNotAllowed},
		{"PUT -> 405", http.MethodPut, "{}", http.StatusMethodNotAllowed},
		{"empty body -> 400", http.MethodPost, "", http.StatusBadRequest},
		{"malformed JSON -> 400", http.MethodPost, "not json", http.StatusBadRequest},
		{"missing user_id -> 400", http.MethodPost, `{"user":{"email":"u@x"}}`, http.StatusBadRequest},
		{"empty user_id -> 400", http.MethodPost, `{"user":{"email":"u@x","user_id":""}}`, http.StatusBadRequest},
		{"bad owner_type -> 400", http.MethodPost, `{"user":{"email":"u@x","user_id":"u1","owner_type":"robot"}}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, broker := newJoinHarness(t)

			recorder := postAccountOpen(t, conductor, tc.method, tc.body)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			// Validation must short-circuit BEFORE any broker I/O.
			if broker.ensureAccountCount() != 0 {
				t.Errorf("broker called %d time(s) despite validation reject", broker.ensureAccountCount())
			}
			assertErrorBody(t, recorder)
			assertRegistryEmpty(t, conductor)
		})
	}
}

// --- broker error -> HTTP status table (end-to-end) -----------------

func TestHandleAccountOpen_BrokerErrorStatus(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"lifecycle.ErrBadOwnerID -> 400", lifecycle.ErrBadOwnerID, http.StatusBadRequest},
		{"store.ErrNoAccount -> 503", store.ErrNoAccount, http.StatusServiceUnavailable},
		{"lifecycle.ErrCreateFailed -> 503", lifecycle.ErrCreateFailed, http.StatusServiceUnavailable},
		{"lifecycle.ErrBadOwnerType -> 500", lifecycle.ErrBadOwnerType, http.StatusInternalServerError},
		{"lifecycle.ErrNegativeStartingCapital -> 500", lifecycle.ErrNegativeStartingCapital, http.StatusInternalServerError},
		{"lifecycle.ErrNegativeStartingBank -> 500", lifecycle.ErrNegativeStartingBank, http.StatusInternalServerError},
		{"unknown error -> 500", errors.New("something unexpected"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, broker := newJoinHarness(t)
			broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
				return "", tc.err
			}

			recorder := postAccountOpen(t, conductor, http.MethodPost,
				`{"user":{"email":"u@x","user_id":"user-7f3c"}}`)
			if recorder.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d. body=%q", recorder.Code, tc.wantCode, recorder.Body.String())
			}
			assertErrorBody(t, recorder)
			assertRegistryEmpty(t, conductor)
		})
	}
}

// --- wrapped broker error dispatches the same way -------------------

func TestHandleAccountOpen_WrappedErrorsClassifyCorrectly(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
		return "", fmt.Errorf("broker wrap: %w", lifecycle.ErrCreateFailed)
	}

	recorder := postAccountOpen(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"user-7f3c"}}`)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (wrapped ErrCreateFailed)", recorder.Code)
	}
}

// --- handler-owned deadline surfaces as 504 -------------------------

// TestHandleAccountOpen_HandlerTimeoutYields504 wires a slow fake
// EnsureAccount that blocks on ctx.Done(), pairs it with a tiny
// AccountOpenTimeout, and asserts the handler fires 504 inside the
// expected envelope. Proves the handler → broker ctx threading is
// correct (if ctx were not threaded through the broker, the fake would
// never see Done and the test would wedge).
func TestHandleAccountOpen_HandlerTimeoutYields504(t *testing.T) {
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := &fakeJoinBroker{}
	broker.ensureAccountFunc = func(ctx context.Context, _ lifecycle.OwnerType, _ string) (funnel.AccountID, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(5 * time.Second):
			return "", errors.New("test watchdog fired — ctx never cancelled")
		}
	}
	conductor, err := NewConductor(Config{
		CfgDir:             t.TempDir(),
		OHLCVPool:          pool,
		Broker:             broker,
		Market:             stubMarketDataPort{},
		AccountOpenTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}

	start := time.Now()
	recorder := postAccountOpen(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"user-7f3c"}}`)
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("handler took %v (ctx deadline plumbing likely broken)", elapsed)
	}
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
	assertRegistryEmpty(t, conductor)
}

// --- panic path recovers cleanly -----------------------------------

func TestHandleAccountOpen_PanicReturns500(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
		panic("simulated invariant violation inside EnsureAccount")
	}

	recorder := postAccountOpen(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"user-7f3c"}}`)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (recoverPanic clean-500 path)", recorder.Code)
	}
	assertErrorBody(t, recorder)
}

// --- MountRoutes end-to-end through the mux -------------------------

// TestMountRoutes_RegistersAccountOpen builds a real *http.ServeMux,
// mounts via Conductor.MountRoutes, POSTs through an httptest.Server,
// and asserts both the new route and the pre-existing liveness probe
// return 200 — the one combined test proves the new route landed and
// didn't shadow anything already mounted.
func TestMountRoutes_RegistersAccountOpen(t *testing.T) {
	conductor, _ := newJoinHarness(t)

	mux := http.NewServeMux()
	conductor.MountRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Post(server.URL+PathAccountOpen, "application/json",
		strings.NewReader(`{"user":{"email":"u@x","user_id":"user-7f3c"}}`))
	if err != nil {
		t.Fatalf("POST %s: %v", PathAccountOpen, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200. body=%s", resp.StatusCode, string(body))
	}

	livenessResp, err := http.Get(server.URL + LivenessPath)
	if err != nil {
		t.Fatalf("GET liveness: %v", err)
	}
	defer livenessResp.Body.Close()
	if livenessResp.StatusCode != http.StatusOK {
		t.Errorf("liveness status = %d, want 200 (pre-existing route regression)", livenessResp.StatusCode)
	}
}

// --- MountRoutes registers all three new reads ---------------------

func TestMountRoutes_RegistersMarketsAndInstruments(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	_ = broker // not used here
	// Write one config so markets returns a populated response and
	// instruments can look it up.
	writeMarketConfig(t, conductor.cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	conductor.now = func() time.Time { return time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata) }

	mux := http.NewServeMux()
	conductor.MountRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	marketsResp, err := http.Get(server.URL + PathMarkets)
	if err != nil {
		t.Fatalf("GET markets: %v", err)
	}
	defer marketsResp.Body.Close()
	if marketsResp.StatusCode != http.StatusOK {
		t.Errorf("markets status = %d, want 200", marketsResp.StatusCode)
	}

	instrumentsResp, err := http.Get(server.URL + PathInstruments + "?market=IN")
	if err != nil {
		t.Fatalf("GET instruments: %v", err)
	}
	defer instrumentsResp.Body.Close()
	if instrumentsResp.StatusCode != http.StatusOK {
		t.Errorf("instruments status = %d, want 200", instrumentsResp.StatusCode)
	}
}
