package reads

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// writeJSON / writeError
// -----------------------------------------------------------------------------

func TestWriteJSON_StatusAndContentType(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusOK, map[string]int{"balance": 1_000_000}, "/v1/account/balance")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if gotCT := recorder.Header().Get("Content-Type"); gotCT != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", gotCT)
	}
	var decoded map[string]int
	if err := json.NewDecoder(recorder.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if decoded["balance"] != 1_000_000 {
		t.Fatalf("decoded body = %+v", decoded)
	}
}

func TestWriteError_StatusAndEnvelope(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeError(recorder, http.StatusBadRequest, "account_id required", "/v1/account/balance")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var body errorBody
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error != "account_id required" {
		t.Fatalf("body.Error = %q", body.Error)
	}
}

// -----------------------------------------------------------------------------
// requireGET
// -----------------------------------------------------------------------------

func TestRequireGET(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		wantPass   bool
		wantStatus int
	}{
		{"GET passes", http.MethodGet, true, http.StatusOK /* unused */},
		{"POST → 405", http.MethodPost, false, http.StatusMethodNotAllowed},
		{"PUT → 405", http.MethodPut, false, http.StatusMethodNotAllowed},
		{"DELETE → 405", http.MethodDelete, false, http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "/v1/account/balance", nil)
			recorder := httptest.NewRecorder()

			got := requireGET(recorder, request, "/v1/account/balance")
			if got != tc.wantPass {
				t.Fatalf("requireGET returned %v, want %v", got, tc.wantPass)
			}
			if !tc.wantPass {
				if recorder.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d", recorder.Code, tc.wantStatus)
				}
				if allow := recorder.Header().Get("Allow"); allow != http.MethodGet {
					t.Fatalf("Allow header = %q, want GET", allow)
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// parseAccountID
// -----------------------------------------------------------------------------

func TestParseAccountID(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantID  string
		wantOK  bool
	}{
		{"present", "/x?account_id=acct-1", "acct-1", true},
		{"missing", "/x", "", false},
		{"empty value", "/x?account_id=", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, tc.url, nil)
			gotID, gotOK := parseAccountID(request)
			if gotID != tc.wantID || gotOK != tc.wantOK {
				t.Fatalf("parseAccountID = (%q, %v), want (%q, %v)", gotID, gotOK, tc.wantID, tc.wantOK)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// parseBefore
// -----------------------------------------------------------------------------

func TestParseBefore(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		wantOK bool
		// wantNil indicates the pointer should be nil (absent/empty path).
		wantNil bool
	}{
		{"absent → (nil, true)", "", true, true},
		{"valid RFC3339 → (&t, true)", "2026-10-08T12:00:00Z", true, false},
		{"malformed → (nil, false)", "not-a-timestamp", false, true},
		{"non-RFC3339 layout rejected", "2026-10-08", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := "/x"
			if tc.raw != "" {
				url = "/x?before=" + tc.raw
			}
			request := httptest.NewRequest(http.MethodGet, url, nil)
			got, gotOK := parseBefore(request)
			if gotOK != tc.wantOK {
				t.Fatalf("parseBefore ok = %v, want %v", gotOK, tc.wantOK)
			}
			if tc.wantNil && got != nil {
				t.Fatalf("parseBefore ptr = %v, want nil", got)
			}
			if !tc.wantNil && got == nil {
				t.Fatalf("parseBefore ptr = nil, want non-nil for %q", tc.raw)
			}
		})
	}
}

// Round-trip check: parsing then formatting the sample should land on
// the same RFC3339 string we started with — guards against a drift
// where parseBefore accepts a layout the Store cannot query by.
func TestParseBefore_RoundTripFormatMatches(t *testing.T) {
	const sample = "2026-10-08T12:00:00Z"
	request := httptest.NewRequest(http.MethodGet, "/x?before="+sample, nil)
	got, ok := parseBefore(request)
	if !ok || got == nil {
		t.Fatalf("expected (non-nil, true), got (%v, %v)", got, ok)
	}
	if got.Format(time.RFC3339) != sample {
		t.Fatalf("round-trip = %q, want %q", got.Format(time.RFC3339), sample)
	}
}

// -----------------------------------------------------------------------------
// parseLimit
// -----------------------------------------------------------------------------

func TestParseLimit(t *testing.T) {
	cases := []struct {
		name        string
		raw         string
		defaultLim  int
		wantValue   int
		wantOK      bool
	}{
		{"absent → default", "", 50, 50, true},
		{"valid positive", "25", 50, 25, true},
		{"zero rejected (ambiguous with 'default')", "0", 50, 0, false},
		{"negative rejected", "-1", 50, 0, false},
		{"non-numeric rejected", "many", 50, 0, false},
		{"huge value passes (Store caps)", "10000", 50, 10000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := "/x"
			if tc.raw != "" {
				url = "/x?limit=" + tc.raw
			}
			request := httptest.NewRequest(http.MethodGet, url, nil)
			got, gotOK := parseLimit(request, tc.defaultLim)
			if got != tc.wantValue || gotOK != tc.wantOK {
				t.Fatalf("parseLimit = (%d, %v), want (%d, %v)", got, gotOK, tc.wantValue, tc.wantOK)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// recoverPanic
// -----------------------------------------------------------------------------

// handler that defers recoverPanic then panics — the recover must turn
// the panic into a 500 with the standard errorBody. Guards the HTTP-
// boundary panic story every Phase-6 handler relies on.
func TestRecoverPanic_WritesInternalError(t *testing.T) {
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer recoverPanic(w, "/v1/account/balance")
		panic("boom")
	})
	recorder := httptest.NewRecorder()
	testHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/account/balance", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	bodyText := recorder.Body.String()
	if !strings.Contains(bodyText, "internal error") {
		t.Fatalf("body missing 'internal error': %q", bodyText)
	}
}

// A handler that returns normally must NOT trip the recover (nothing to
// recover from, nothing written) — the guard is passive on the happy
// path.
func TestRecoverPanic_NoOpOnHappyPath(t *testing.T) {
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer recoverPanic(w, "/v1/account/balance")
		writeJSON(w, http.StatusOK, map[string]int{"ok": 1}, "/v1/account/balance")
	})
	recorder := httptest.NewRecorder()
	testHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/account/balance", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (recover must not clobber a successful write)", recorder.Code)
	}
}
