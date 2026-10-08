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

// Tests for the costs + costs/summary endpoints — the country-
// dispatched durable reads. Covers:
//
//   - country gating: required param, "IN" routes, "US" returns 501,
//     anything else returns 400 before any Store call fires;
//   - the store methods are called with the expected args
//     (confirming the handler stays country-agnostic — it talks only
//     to the IN methods via dispatch);
//   - keyset pagination on exit_at via the newly-added
//     TradeCostBreakdown.ExitAt field (full page sets next_before,
//     short page returns null);
//   - 503 on ErrLoadFailed; 200 on empty / zero-filled;
//   - response shape (lines are objects with name+amount; total is a
//     bare int64; no unit field anywhere).

func serveCosts(t *testing.T, durableStore *stubStore, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewCostsHandler(durableStore, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func serveCostsSummary(t *testing.T, durableStore *stubStore, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewCostsSummaryHandler(durableStore, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestCosts_CountryDispatch(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantStatus int
		wantHit    bool // true iff the IN store method should fire
	}{
		{"missing country → 400 before any store call", "&limit=10", http.StatusBadRequest, false},
		{"IN → routes to CostsPerTradeIN", "&country=IN", http.StatusOK, true},
		{"US → 501, no store call", "&country=US", http.StatusNotImplemented, false},
		{"unknown country → 400, no store call", "&country=ZZ", http.StatusBadRequest, false},
		{"lowercase in rejected (case-sensitive)", "&country=in", http.StatusBadRequest, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			durableStore := &stubStore{}
			recorder := serveCosts(t, durableStore, defaultTestConfig, http.MethodGet, PathCosts+"?account_id=acct-1"+tc.query)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			firedIN := durableStore.costsPerTradeCalls.Load() > 0
			if firedIN != tc.wantHit {
				t.Fatalf("CostsPerTradeIN fired=%v, want %v", firedIN, tc.wantHit)
			}
		})
	}
}

func TestCosts_StatusMap(t *testing.T) {
	okStore := &stubStore{costsPerTradeReply: nil}
	loadFailStore := &stubStore{costsPerTradeErr: fmt.Errorf("db died: %w", store.ErrLoadFailed)}
	unexpectedStore := &stubStore{costsPerTradeErr: errors.New("boom")}
	cases := []struct {
		name       string
		url        string
		method     string
		store      *stubStore
		wantStatus int
	}{
		{"400 missing aid", PathCosts + "?country=IN", http.MethodGet, okStore, http.StatusBadRequest},
		{"400 bad limit", PathCosts + "?account_id=acct-1&country=IN&limit=nope", http.MethodGet, okStore, http.StatusBadRequest},
		{"400 bad before", PathCosts + "?account_id=acct-1&country=IN&before=yesterday", http.MethodGet, okStore, http.StatusBadRequest},
		{"405 non-GET", PathCosts + "?account_id=acct-1&country=IN", http.MethodPost, okStore, http.StatusMethodNotAllowed},
		{"503 store unavailable", PathCosts + "?account_id=acct-1&country=IN", http.MethodGet, loadFailStore, http.StatusServiceUnavailable},
		{"500 unexpected", PathCosts + "?account_id=acct-1&country=IN", http.MethodGet, unexpectedStore, http.StatusInternalServerError},
		{"200 empty", PathCosts + "?account_id=acct-1&country=IN", http.MethodGet, okStore, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveCosts(t, tc.store, defaultTestConfig, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

// A full page must advertise next_before set to the oldest row's
// exit_at. This is the exact point that drove the Store change —
// without ExitAt on TradeCostBreakdown, this cursor cannot form.
func TestCosts_200_FullPageAdvertisesNextBefore(t *testing.T) {
	newestExitAt := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	middleExitAt := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	oldestExitAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	durableStore := &stubStore{costsPerTradeReply: []store.TradeCostBreakdown{
		{TradeID: "t-newest", Lines: []store.Line{{Name: "stt", Amount: 25}}, Total: 25, ExitAt: newestExitAt},
		{TradeID: "t-middle", Lines: []store.Line{{Name: "stt", Amount: 25}}, Total: 25, ExitAt: middleExitAt},
		{TradeID: "t-oldest", Lines: []store.Line{{Name: "stt", Amount: 25}}, Total: 25, ExitAt: oldestExitAt},
	}}
	recorder := serveCosts(t, durableStore, defaultTestConfig, http.MethodGet, PathCosts+"?account_id=acct-1&country=IN&limit=3")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Trades     []map[string]any `json:"trades"`
		NextBefore *time.Time       `json:"next_before"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.NextBefore == nil {
		t.Fatalf("full page must advertise next_before, got nil")
	}
	if !body.NextBefore.Equal(oldestExitAt) {
		t.Fatalf("next_before = %v, want oldest row's exit_at %v", body.NextBefore, oldestExitAt)
	}
	// exit_at is a pagination mechanism, not a client-visible field —
	// a leak would couple clients to our cursor shape.
	if _, present := body.Trades[0]["exit_at"]; present {
		t.Error("exit_at must not appear in the per-row response body — it's a cursor key, not row data")
	}
}

func TestCosts_200_ShortPageHasNoNextBefore(t *testing.T) {
	durableStore := &stubStore{costsPerTradeReply: []store.TradeCostBreakdown{
		{TradeID: "t-only", Lines: []store.Line{{Name: "stt", Amount: 25}}, Total: 25, ExitAt: time.Now()},
	}}
	recorder := serveCosts(t, durableStore, defaultTestConfig, http.MethodGet, PathCosts+"?account_id=acct-1&country=IN&limit=50")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		NextBefore *time.Time `json:"next_before"`
	}
	_ = json.NewDecoder(recorder.Body).Decode(&body)
	if body.NextBefore != nil {
		t.Fatalf("short page must have next_before=null, got %v", body.NextBefore)
	}
}

func TestCosts_200_BodyShape(t *testing.T) {
	durableStore := &stubStore{costsPerTradeReply: []store.TradeCostBreakdown{
		{
			TradeID: "t-1",
			Lines: []store.Line{
				{Name: "stt", Amount: 25},
				{Name: "exchange", Amount: 3},
				{Name: "gst", Amount: 5},
			},
			Total:  33,
			ExitAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		},
	}}
	recorder := serveCosts(t, durableStore, defaultTestConfig, http.MethodGet, PathCosts+"?account_id=acct-1&country=IN")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Trades []map[string]any `json:"trades"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Trades) != 1 {
		t.Fatalf("want 1 row, got %d", len(body.Trades))
	}
	row := body.Trades[0]
	if row["trade_id"] != "t-1" || row["total"].(float64) != 33 {
		t.Errorf("row header fields wrong: %+v", row)
	}
	lines, ok := row["lines"].([]any)
	if !ok || len(lines) != 3 {
		t.Fatalf("lines = %v (want slice of 3)", row["lines"])
	}
	firstLine := lines[0].(map[string]any)
	if firstLine["name"] != "stt" || firstLine["amount"].(float64) != 25 {
		t.Errorf("first line wrong: %+v", firstLine)
	}
	if _, present := row["unit"]; present {
		t.Error("response must not include a 'unit' field — all money is bare int64 here")
	}
}

// Captures that the handler threads pagination args through to the IN
// method verbatim. A regression dropping limit would silently return
// the Store's cap instead of the client's requested page.
func TestCosts_ArgsThreadedToStore(t *testing.T) {
	durableStore := &stubStore{}
	beforeTime := "2026-10-05T12:00:00Z"
	url := PathCosts + "?account_id=acct-1&country=IN&limit=17&before=" + beforeTime
	serveCosts(t, durableStore, defaultTestConfig, http.MethodGet, url)
	if durableStore.costsPerTradeLimit != 17 {
		t.Errorf("limit threaded = %d, want 17", durableStore.costsPerTradeLimit)
	}
	if durableStore.costsPerTradeBefore == nil {
		t.Fatalf("before threaded = nil, want %q", beforeTime)
	}
	wantBefore, _ := time.Parse(time.RFC3339, beforeTime)
	if !durableStore.costsPerTradeBefore.Equal(wantBefore) {
		t.Errorf("before threaded = %v, want %v", durableStore.costsPerTradeBefore, wantBefore)
	}
}

func TestCostsSummary_CountryDispatch(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantStatus int
		wantHit    bool
	}{
		{"missing country → 400", "", http.StatusBadRequest, false},
		{"IN → routes", "&country=IN", http.StatusOK, true},
		{"US → 501", "&country=US", http.StatusNotImplemented, false},
		{"unknown → 400", "&country=ZZ", http.StatusBadRequest, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			durableStore := &stubStore{}
			recorder := serveCostsSummary(t, durableStore, defaultTestConfig, http.MethodGet, PathCostsSummary+"?account_id=acct-1"+tc.query)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			firedIN := durableStore.costsAggregateCalls.Load() > 0
			if firedIN != tc.wantHit {
				t.Fatalf("CostsAggregateIN fired=%v, want %v", firedIN, tc.wantHit)
			}
		})
	}
}

func TestCostsSummary_StatusMap(t *testing.T) {
	okStore := &stubStore{}
	loadFailStore := &stubStore{costsAggregateErr: fmt.Errorf("db died: %w", store.ErrLoadFailed)}
	unexpectedStore := &stubStore{costsAggregateErr: errors.New("boom")}
	cases := []struct {
		name       string
		store      *stubStore
		wantStatus int
	}{
		{"503 store unavailable", loadFailStore, http.StatusServiceUnavailable},
		{"500 unexpected", unexpectedStore, http.StatusInternalServerError},
		{"200 zero-filled aggregate", okStore, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveCostsSummary(t, tc.store, defaultTestConfig, http.MethodGet, PathCostsSummary+"?account_id=acct-1&country=IN")
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

func TestCostsSummary_200_BodyShape(t *testing.T) {
	durableStore := &stubStore{costsAggregateReply: store.CostAggregate{
		TradeCount: 42,
		Lines: []store.Line{
			{Name: "stt", Amount: 1_000},
			{Name: "gst", Amount: 300},
		},
		Total: 1_300,
	}}
	recorder := serveCostsSummary(t, durableStore, defaultTestConfig, http.MethodGet, PathCostsSummary+"?account_id=acct-1&country=IN")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["trade_count"].(float64) != 42 || body["total"].(float64) != 1_300 {
		t.Errorf("header fields wrong: %+v", body)
	}
	lines := body["lines"].([]any)
	if len(lines) != 2 {
		t.Errorf("lines len = %d, want 2", len(lines))
	}
	if _, present := body["unit"]; present {
		t.Error("response must not include a 'unit' field — all money is bare int64 here")
	}
}

func TestNewCostsHandler_PanicsOnBadDeps(t *testing.T) {
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
			_ = NewCostsHandler(tc.store, tc.cfg)
		})
	}
}

func TestNewCostsSummaryHandler_PanicsOnBadDeps(t *testing.T) {
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
			_ = NewCostsSummaryHandler(tc.store, tc.cfg)
		})
	}
}
