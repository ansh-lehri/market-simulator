package conductor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"simulator/contract"
)

// newInstrumentsHarness is the same shape as newMarketsHarness — the
// handler only reads cfgDir, so broker and market-data stubs are enough.
func newInstrumentsHarness(t *testing.T) (*Conductor, string) {
	t.Helper()
	return newMarketsHarness(t)
}

// getInstruments calls handleInstruments directly with the given query
// string. Empty query string yields a request with no "market" param,
// so the 400 path can be exercised.
func getInstruments(t *testing.T, conductor *Conductor, method string, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	targetURL := PathInstruments
	if rawQuery != "" {
		targetURL += "?" + rawQuery
	}
	request := httptest.NewRequest(method, targetURL, nil)
	recorder := httptest.NewRecorder()
	conductor.handleInstruments(recorder, request)
	return recorder
}

// --- happy path ------------------------------------------------------

func TestHandleInstruments_HappyPath(t *testing.T) {
	conductor, cfgDir := newInstrumentsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")

	recorder := getInstruments(t, conductor, http.MethodGet, "market=IN")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var response instrumentListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v (raw=%q)", err, recorder.Body.String())
	}
	if response.Market != "IN" {
		t.Errorf("market = %q, want %q", response.Market, "IN")
	}
	if len(response.Instruments) != 2 {
		t.Fatalf("got %d instruments, want 2 (ACME + BETA from fixture)", len(response.Instruments))
	}
	// Order matches the config file (writeMarketConfig emits ACME then BETA).
	if response.Instruments[0].ID != contract.InstrumentID("ACME") || response.Instruments[0].TickSize != 5 {
		t.Errorf("instruments[0] = %+v, want {ACME, 5}", response.Instruments[0])
	}
	if response.Instruments[1].ID != contract.InstrumentID("BETA") || response.Instruments[1].TickSize != 10 {
		t.Errorf("instruments[1] = %+v, want {BETA, 10}", response.Instruments[1])
	}
}

// --- works even when no Live market exists ---------------------------

func TestHandleInstruments_WorksWithNoLiveMarket(t *testing.T) {
	conductor, cfgDir := newInstrumentsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	// No registry entry installed on purpose.

	recorder := getInstruments(t, conductor, http.MethodGet, "market=IN")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (list is static config, not live state)", recorder.Code)
	}
	var response instrumentListResponse
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Instruments) == 0 {
		t.Error("expected non-empty instruments even with no Live market")
	}
}

// --- request validation ---------------------------------------------

func TestHandleInstruments_MissingMarketParamReturns400(t *testing.T) {
	conductor, _ := newInstrumentsHarness(t)

	for _, query := range []string{"", "market=", "foo=bar"} {
		t.Run("query="+query, func(t *testing.T) {
			recorder := getInstruments(t, conductor, http.MethodGet, query)
			if recorder.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", recorder.Code)
			}
			assertErrorBody(t, recorder)
		})
	}
}

func TestHandleInstruments_UnknownMarketReturns404(t *testing.T) {
	conductor, _ := newInstrumentsHarness(t)
	// No configs at all.

	recorder := getInstruments(t, conductor, http.MethodGet, "market=NOPE")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	assertErrorBody(t, recorder)
}

// Malformed config file for a known tag — not 404 (the file does
// exist), not a happy 200 (it is broken) — must land on 500.
func TestHandleInstruments_MalformedConfigReturns500(t *testing.T) {
	conductor, cfgDir := newInstrumentsHarness(t)
	if err := os.WriteFile(filepath.Join(cfgDir, "IN.json"), []byte("{ not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	recorder := getInstruments(t, conductor, http.MethodGet, "market=IN")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (malformed config is server-side, not 404)", recorder.Code)
	}
	assertErrorBody(t, recorder)
}

func TestHandleInstruments_MethodNotGET(t *testing.T) {
	conductor, cfgDir := newInstrumentsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			recorder := getInstruments(t, conductor, method, "market=IN")
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s status = %d, want 405", method, recorder.Code)
			}
			assertErrorBody(t, recorder)
		})
	}
}
