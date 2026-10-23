package conductor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// postOnDemandJoin issues a POST to handleJoinOnDemand directly
// (bypasses the mux) and returns the recorder for assertion.
func postOnDemandJoin(t *testing.T, conductor *Conductor, method string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, PathSessionOnDemandJoin, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	conductor.handleJoinOnDemand(recorder, request)
	return recorder
}

func TestHandleJoinOnDemand_HappyPath(t *testing.T) {
	conductor, broker := newHandlerHarness(t)
	marketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")

	recorder := postOnDemandJoin(t, conductor, http.MethodPost,
		`{"user":{"user_id":"agent-42"},"exchange_id":"`+string(marketID)+`"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}

	var responseBody joinResponseBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &responseBody); err != nil {
		t.Fatalf("decode response: %v (body=%q)", err, recorder.Body.String())
	}
	if !isUUIDv4(responseBody.SessionID) || !isUUIDv4(responseBody.ParticipantID) {
		t.Errorf("response = %+v, want UUID v4 identifiers", responseBody)
	}
	if responseBody.Status != "active" {
		t.Errorf("status = %q, want \"active\"", responseBody.Status)
	}
	if broker.ensureAccountCount() != 1 {
		t.Fatalf("EnsureAccount calls = %d, want 1", broker.ensureAccountCount())
	}
}

func TestHandleJoinOnDemand_RequestValidation(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		body       string
		wantStatus int
	}{
		{"GET method -> 405", http.MethodGet, "", http.StatusMethodNotAllowed},
		{"empty body -> 400", http.MethodPost, "", http.StatusBadRequest},
		{"malformed JSON -> 400", http.MethodPost, `not json`, http.StatusBadRequest},
		{"missing user_id -> 400", http.MethodPost, `{"exchange_id":"od:1"}`, http.StatusBadRequest},
		{"missing exchange_id -> 400", http.MethodPost, `{"user":{"user_id":"agent-1"}}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, broker := newHandlerHarness(t)
			recorder := postOnDemandJoin(t, conductor, tc.method, tc.body)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. body=%q", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if broker.ensureAccountCount() != 0 {
				t.Errorf("broker called despite validation reject")
			}
			assertErrorBody(t, recorder)
		})
	}
}

func TestHandleJoinOnDemand_NoLiveMarketReturns409(t *testing.T) {
	conductor, _ := newHandlerHarness(t)
	recorder := postOnDemandJoin(t, conductor, http.MethodPost,
		`{"user":{"user_id":"agent-1"},"exchange_id":"od:nonexistent"}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409. body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestMountRoutes_RegistersSessionOnDemandJoin(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	mux := http.NewServeMux()
	conductor.MountRoutes(mux)

	marketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, PathSessionOnDemandJoin,
		strings.NewReader(`{"user":{"user_id":"agent-1"},"exchange_id":"`+string(marketID)+`"}`))
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
}
