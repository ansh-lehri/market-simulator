package conductor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- helpers ---------------------------------------------------------

// postSessionCurrent issues a POST to handleSessionCurrent directly
// (bypasses the mux so each test runs without mounting routes).
func postSessionCurrent(t *testing.T, conductor *Conductor, method string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, PathSessionCurrent, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	conductor.handleSessionCurrent(recorder, request)
	return recorder
}

// injectActiveSession wires a user + market record directly into the
// registry so the resolver sees the state the test needs, without
// routing through the fresh-join flow. onDemandWindow stamps the two
// end timers for on-demand markets; the zero value leaves them unset.
type injectedSessionWindow struct {
	warmupEndsAt time.Time
	closeAt      time.Time
}

func injectActiveSession(t *testing.T, conductor *Conductor, userID UserID, tag MarketTag, mode Mode, window injectedSessionWindow) (SessionID, MarketID) {
	t.Helper()
	marketID := MarketID(string(tag) + ":injected")
	sessionID := SessionID("sid-injected-" + string(userID))
	market := &Market{
		ID:           marketID,
		Mode:         mode,
		Tag:          tag,
		State:        Live,
		warmupEndsAt: window.warmupEndsAt,
		closeAt:      window.closeAt,
		members:      map[SessionID]struct{}{sessionID: {}},
	}
	session := &Session{
		ID:          sessionID,
		User:        userID,
		Market:      marketID,
		AccountType: Durable,
		Participant: ParticipantID("pid-injected"),
		State:       Active,
	}
	conductor.reg.mu.Lock()
	conductor.reg.markets[marketID] = market
	conductor.reg.sessions[sessionID] = session
	conductor.reg.byUser[userID] = session
	conductor.reg.mu.Unlock()
	return sessionID, marketID
}

// --- 200 hit (canonical) --------------------------------------------

// A canonical hit returns the session identity + state; the two
// end-at fields must be omitted from the JSON (canonical markets carry
// no user-visible end timer at this door).
func TestHandleSessionCurrent_CanonicalHit_OmitsEndTimers(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	userID := UserID("human-42")
	sessionID, _ := injectActiveSession(t, conductor, userID, "IN", Canonical, injectedSessionWindow{})

	recorder := postSessionCurrent(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"}}`)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var body sessionCurrentResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.SessionID != string(sessionID) {
		t.Errorf("session_id = %q, want %q", body.SessionID, sessionID)
	}
	if body.Mode != "canonical" {
		t.Errorf("mode = %q, want \"canonical\"", body.Mode)
	}
	if body.Market != "IN" {
		t.Errorf("market = %q, want \"IN\"", body.Market)
	}
	if body.Status != "active" {
		t.Errorf("status = %q, want \"active\"", body.Status)
	}
	// Canonical: the end-at fields must not be set on the struct and
	// must not appear in the raw JSON (omitempty).
	if body.WarmupEndsAt != "" {
		t.Errorf("warmup_ends_at = %q, want empty for canonical", body.WarmupEndsAt)
	}
	if body.SessionEndsAt != "" {
		t.Errorf("session_ends_at = %q, want empty for canonical", body.SessionEndsAt)
	}
	rawJSON := recorder.Body.String()
	if strings.Contains(rawJSON, "warmup_ends_at") {
		t.Errorf("raw JSON contains warmup_ends_at key for canonical hit: %s", rawJSON)
	}
	if strings.Contains(rawJSON, "session_ends_at") {
		t.Errorf("raw JSON contains session_ends_at key for canonical hit: %s", rawJSON)
	}

	// Pure read — the broker must not have been touched.
	if broker.ensureAccountCount() != 0 || broker.startSessionCount() != 0 {
		t.Errorf("broker was called by a pure-read handler: ensure=%d start=%d",
			broker.ensureAccountCount(), broker.startSessionCount())
	}
}

// --- 200 hit (on-demand) --------------------------------------------

// An on-demand hit returns the session identity + state AND both
// end-at timers (RFC3339Nano UTC). The UI counts down against them.
func TestHandleSessionCurrent_OnDemandHit_PopulatesEndTimers(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	userID := UserID("human-42")
	warmupEnd := time.Date(2026, 10, 15, 9, 5, 0, 0, time.UTC)
	sessionEnd := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	sessionID, _ := injectActiveSession(t, conductor, userID, "US", OnDemand, injectedSessionWindow{
		warmupEndsAt: warmupEnd,
		closeAt:      sessionEnd,
	})

	recorder := postSessionCurrent(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"}}`)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	var body sessionCurrentResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (raw=%q)", err, recorder.Body.String())
	}
	if body.SessionID != string(sessionID) {
		t.Errorf("session_id = %q, want %q", body.SessionID, sessionID)
	}
	if body.Mode != "ondemand" {
		t.Errorf("mode = %q, want \"ondemand\"", body.Mode)
	}
	if body.Market != "US" {
		t.Errorf("market = %q, want \"US\"", body.Market)
	}
	if body.Status != "active" {
		t.Errorf("status = %q, want \"active\"", body.Status)
	}
	wantWarmup := warmupEnd.UTC().Format(time.RFC3339Nano)
	if body.WarmupEndsAt != wantWarmup {
		t.Errorf("warmup_ends_at = %q, want %q", body.WarmupEndsAt, wantWarmup)
	}
	wantSessionEnd := sessionEnd.UTC().Format(time.RFC3339Nano)
	if body.SessionEndsAt != wantSessionEnd {
		t.Errorf("session_ends_at = %q, want %q", body.SessionEndsAt, wantSessionEnd)
	}
	if broker.ensureAccountCount() != 0 || broker.startSessionCount() != 0 {
		t.Errorf("broker was called by a pure-read handler: ensure=%d start=%d",
			broker.ensureAccountCount(), broker.startSessionCount())
	}
}

// --- 404 miss --------------------------------------------------------

// No byUser[user] → 404. No broker I/O.
func TestHandleSessionCurrent_Miss_Returns404(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	// No session injected.

	recorder := postSessionCurrent(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"ghost-user"}}`)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404. body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorBody(t, recorder)
	if broker.ensureAccountCount() != 0 || broker.startSessionCount() != 0 {
		t.Errorf("broker was called on a miss: ensure=%d start=%d",
			broker.ensureAccountCount(), broker.startSessionCount())
	}
}

// --- request validation ----------------------------------------------

func TestHandleSessionCurrent_RequestValidation(t *testing.T) {
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
		{"missing user_id -> 400", http.MethodPost, `{"user":{"email":"u@x"}}`, http.StatusBadRequest},
		{"empty user_id -> 400", http.MethodPost, `{"user":{"email":"u@x","user_id":""}}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, broker := newJoinHarness(t)

			recorder := postSessionCurrent(t, conductor, tc.method, tc.body)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q",
					recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if broker.ensureAccountCount() != 0 || broker.startSessionCount() != 0 {
				t.Errorf("broker was called despite request-validation reject: ensure=%d start=%d",
					broker.ensureAccountCount(), broker.startSessionCount())
			}
			assertErrorBody(t, recorder)
		})
	}
}

// --- defensive: market record vanished while byUser slot survives ----

// A teardown may delete the market record before the reap finishes
// freeing the byUser slot. The resolver must still return 200 for the
// session the user holds, with the market-sourced fields left zeroed
// (end timers omitted; mode and market tag reflect the Zero Mode which
// serializes to "canonical" — acceptable: the frontend uses status +
// 404 from a later poll to detect the vanished market).
func TestHandleSessionCurrent_MarketRecordGone_Returns200(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	userID := UserID("human-42")
	sessionID, marketID := injectActiveSession(t, conductor, userID, "IN", OnDemand, injectedSessionWindow{
		warmupEndsAt: time.Now().UTC(),
		closeAt:      time.Now().UTC().Add(time.Hour),
	})
	// Delete the market record directly; session + byUser slot remain.
	conductor.reg.mu.Lock()
	delete(conductor.reg.markets, marketID)
	conductor.reg.mu.Unlock()

	recorder := postSessionCurrent(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (market record gone, session still present). body=%q",
			recorder.Code, recorder.Body.String())
	}
	var body sessionCurrentResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.SessionID != string(sessionID) {
		t.Errorf("session_id = %q, want %q", body.SessionID, sessionID)
	}
	// With no market record, mode is the zero Mode (Canonical) → the
	// handler suppresses the end-at timers.
	if body.WarmupEndsAt != "" {
		t.Errorf("warmup_ends_at = %q, want empty when market record is gone", body.WarmupEndsAt)
	}
	if body.SessionEndsAt != "" {
		t.Errorf("session_ends_at = %q, want empty when market record is gone", body.SessionEndsAt)
	}
}

// --- lock discipline -------------------------------------------------

// The resolver releases reg.mu before returning. A probe goroutine that
// tries to acquire reg.mu immediately after the handler returns must
// succeed without waiting — proves no stray lock across the response
// write.
func TestHandleSessionCurrent_ReleasesLockBeforeReturn(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	injectActiveSession(t, conductor, "human-42", "IN", Canonical, injectedSessionWindow{})

	recorder := postSessionCurrent(t, conductor, http.MethodPost,
		`{"user":{"email":"u@x","user_id":"human-42"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("precondition: handler returned %d, want 200", recorder.Code)
	}
	// A fresh lock take must proceed without blocking.
	acquired := make(chan struct{})
	go func() {
		conductor.reg.mu.Lock()
		conductor.reg.mu.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("reg.mu still held after handleSessionCurrent returned")
	}
}

// --- MountRoutes registers /session/current -------------------------

func TestMountRoutes_RegistersSessionCurrent(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	injectActiveSession(t, conductor, "human-via-mux", "IN", Canonical, injectedSessionWindow{})

	mux := http.NewServeMux()
	conductor.MountRoutes(mux)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := http.Post(
		server.URL+PathSessionCurrent,
		"application/json",
		strings.NewReader(`{"user":{"email":"u@x","user_id":"human-via-mux"}}`),
	)
	if err != nil {
		t.Fatalf("POST through mux: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mux POST status = %d, want 200", response.StatusCode)
	}
}
