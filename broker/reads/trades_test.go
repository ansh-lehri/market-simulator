package reads

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/store"
)

// Tests for the trades + pnl endpoints — pure durable reads, no funnel.
// Covers:
//
//   - status map (400 for bad params, 503 on ErrLoadFailed, 500 for
//     unexpected errors, 200 on empty + 200 on populated);
//   - pagination cursor semantics: a full page advertises next_before
//     set to the oldest row's exit_at; a short or empty page returns
//     next_before:null;
//   - construction-time panics (nil store, bad config).

func serveTrades(t *testing.T, durableStore *stubStore, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewTradesHandler(durableStore, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func servePnL(t *testing.T, durableStore *stubStore, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewPnLHandler(durableStore, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestTrades_StatusMap(t *testing.T) {
	okStore := &stubStore{tradePageReply: nil}
	loadFailStore := &stubStore{tradePageErr: fmt.Errorf("db died: %w", store.ErrLoadFailed)}
	unexpectedStore := &stubStore{tradePageErr: errors.New("boom")}
	cases := []struct {
		name       string
		url        string
		method     string
		store      *stubStore
		wantStatus int
	}{
		{"400 missing aid", PathTrades, http.MethodGet, okStore, http.StatusBadRequest},
		{"400 bad limit", PathTrades + "?account_id=acct-1&limit=zero", http.MethodGet, okStore, http.StatusBadRequest},
		{"400 bad before", PathTrades + "?account_id=acct-1&before=not-a-time", http.MethodGet, okStore, http.StatusBadRequest},
		{"405 non-GET", PathTrades + "?account_id=acct-1", http.MethodPost, okStore, http.StatusMethodNotAllowed},
		{"503 store unavailable", PathTrades + "?account_id=acct-1", http.MethodGet, loadFailStore, http.StatusServiceUnavailable},
		{"500 unexpected", PathTrades + "?account_id=acct-1", http.MethodGet, unexpectedStore, http.StatusInternalServerError},
		{"200 empty", PathTrades + "?account_id=acct-1", http.MethodGet, okStore, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveTrades(t, tc.store, defaultTestConfig, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

func TestTrades_200_EmptyBody(t *testing.T) {
	recorder := serveTrades(t, &stubStore{}, defaultTestConfig, http.MethodGet, PathTrades+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Trades     []map[string]any `json:"trades"`
		NextBefore *time.Time       `json:"next_before"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Trades == nil {
		t.Errorf("trades must serialize as [], not null")
	}
	if len(body.Trades) != 0 {
		t.Errorf("trades len = %d, want 0", len(body.Trades))
	}
	if body.NextBefore != nil {
		t.Errorf("next_before must be null on an empty page, got %v", body.NextBefore)
	}
}

// A full page (len(rows) == limit) must advertise next_before set to
// the oldest row's exit_at — otherwise the client has no way to walk
// to the next-older page.
func TestTrades_200_FullPageAdvertisesNextBefore(t *testing.T) {
	oldestExitAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	middleExitAt := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	newestExitAt := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	durableStore := &stubStore{tradePageReply: []store.TradeRow{
		{TradeID: "t-newest", InstrumentID: "ACME", ExitAt: newestExitAt},
		{TradeID: "t-middle", InstrumentID: "ACME", ExitAt: middleExitAt},
		{TradeID: "t-oldest", InstrumentID: "ACME", ExitAt: oldestExitAt},
	}}
	recorder := serveTrades(t, durableStore, defaultTestConfig, http.MethodGet, PathTrades+"?account_id=acct-1&limit=3")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Trades     []map[string]any `json:"trades"`
		NextBefore *time.Time       `json:"next_before"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Trades) != 3 {
		t.Fatalf("trades len = %d, want 3", len(body.Trades))
	}
	if body.NextBefore == nil {
		t.Fatalf("full page must advertise next_before, got nil")
	}
	if !body.NextBefore.Equal(oldestExitAt) {
		t.Fatalf("next_before = %v, want oldest row's exit_at %v", body.NextBefore, oldestExitAt)
	}
}

// A short page (fewer rows than requested limit) means we've reached
// the end of the walk — next_before must be null to signal "no more".
func TestTrades_200_ShortPageHasNoNextBefore(t *testing.T) {
	exitAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	durableStore := &stubStore{tradePageReply: []store.TradeRow{
		{TradeID: "t-only", InstrumentID: "ACME", ExitAt: exitAt},
	}}
	recorder := serveTrades(t, durableStore, defaultTestConfig, http.MethodGet, PathTrades+"?account_id=acct-1&limit=50")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Trades     []map[string]any `json:"trades"`
		NextBefore *time.Time       `json:"next_before"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.NextBefore != nil {
		t.Fatalf("short page must have next_before=null, got %v", body.NextBefore)
	}
}

// The handler must thread limit and before through verbatim; a
// regression that drops limit would silently return the Store's cap
// instead of the requested page.
func TestTrades_ArgsThreadedToStore(t *testing.T) {
	durableStore := &stubStore{}
	beforeTime := "2026-10-05T12:00:00Z"
	url := PathTrades + "?account_id=acct-1&limit=17&before=" + beforeTime
	serveTrades(t, durableStore, defaultTestConfig, http.MethodGet, url)
	if durableStore.tradePageLimit != 17 {
		t.Errorf("limit threaded = %d, want 17", durableStore.tradePageLimit)
	}
	if durableStore.tradePageBefore == nil {
		t.Fatalf("before threaded = nil, want %q", beforeTime)
	}
	wantBefore, _ := time.Parse(time.RFC3339, beforeTime)
	if !durableStore.tradePageBefore.Equal(wantBefore) {
		t.Errorf("before threaded = %v, want %v", durableStore.tradePageBefore, wantBefore)
	}
}

func TestPnL_StatusMap(t *testing.T) {
	okStore := &stubStore{}
	loadFailStore := &stubStore{pnlErr: fmt.Errorf("db died: %w", store.ErrLoadFailed)}
	unexpectedStore := &stubStore{pnlErr: errors.New("boom")}
	cases := []struct {
		name       string
		url        string
		method     string
		store      *stubStore
		wantStatus int
	}{
		{"400 missing aid", PathPnL, http.MethodGet, okStore, http.StatusBadRequest},
		{"405 non-GET", PathPnL + "?account_id=acct-1", http.MethodPost, okStore, http.StatusMethodNotAllowed},
		{"503 store unavailable", PathPnL + "?account_id=acct-1", http.MethodGet, loadFailStore, http.StatusServiceUnavailable},
		{"500 unexpected", PathPnL + "?account_id=acct-1", http.MethodGet, unexpectedStore, http.StatusInternalServerError},
		{"200 zero-filled summary", PathPnL + "?account_id=acct-1", http.MethodGet, okStore, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := servePnL(t, tc.store, defaultTestConfig, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

// Breakevens (net_pnl == 0) are counted in neither win nor loss, so
// win_count + loss_count can be less than trade_count. Response must
// surface the Store numbers verbatim without "fixing" that gap.
func TestPnL_200_BreakevensSurfaceAsThreeDistinctCounts(t *testing.T) {
	durableStore := &stubStore{pnlReply: store.PnLSummary{
		TradeCount: 5,
		GrossPnl:   1_000,
		TotalCosts: 100,
		NetPnl:     900,
		WinCount:   2,
		LossCount:  2,
		// Implicit: 1 breakeven trade (5 - 2 - 2).
	}}
	recorder := servePnL(t, durableStore, defaultTestConfig, http.MethodGet, PathPnL+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for key, want := range map[string]float64{
		"trade_count": 5,
		"gross_pnl":   1_000,
		"total_costs": 100,
		"net_pnl":     900,
		"win_count":   2,
		"loss_count":  2,
	} {
		if got, ok := body[key].(float64); !ok || got != want {
			t.Errorf("body[%q] = %v, want %v", key, body[key], want)
		}
	}
	if _, present := body["unit"]; present {
		t.Error("response must not include a 'unit' field — all money is bare int64 here")
	}
}

func TestNewTradesHandler_PanicsOnBadDeps(t *testing.T) {
	cases := []struct {
		name  string
		store storePort
		cfg   Config
	}{
		{"nil store", nil, defaultTestConfig},
		{"bad config", &stubStore{}, Config{ReadTimeout: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			_ = NewTradesHandler(tc.store, tc.cfg)
		})
	}
}

func TestNewPnLHandler_PanicsOnBadDeps(t *testing.T) {
	cases := []struct {
		name  string
		store storePort
		cfg   Config
	}{
		{"nil store", nil, defaultTestConfig},
		{"bad config", &stubStore{}, Config{ReadTimeout: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			_ = NewPnLHandler(tc.store, tc.cfg)
		})
	}
}
