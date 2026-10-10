package marketdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// stubQueryEngine lets handler tests control QueryCandles's output
// without wiring a real engine + stub store. Captures the inbound
// CandleQuery so tz-parsing assertions can run against it.
type stubQueryEngine struct {
	page CandlePage
	err  error

	gotQuery  CandleQuery
	callCount int
}

func (stub *stubQueryEngine) QueryCandles(ctx context.Context, query CandleQuery) (CandlePage, error) {
	stub.callCount++
	stub.gotQuery = query
	return stub.page, stub.err
}

const testHandlerReadTimeout = 2 * time.Second

// mustBuildHandler constructs the handler; tiny helper to keep tests
// terse (NewCandleHandler panics on bad input, so this is really just
// "no-arg named constructor").
func mustBuildHandler(t *testing.T, engine queryEnginePort, cfg Config) http.Handler {
	t.Helper()
	return NewCandleHandler(engine, cfg)
}

// validParams builds a valid query-string set. Tests mutate one param
// to drive a specific failure branch (one 400 case per test). Keeps the
// "what's being probed" obvious at each call site.
func validParams() url.Values {
	params := url.Values{}
	params.Set("market", "IN")
	params.Set("instrument", "ACME")
	params.Set("timeframe", "5m")
	params.Set("from", "2026-10-10T09:00:00")
	params.Set("to", "2026-10-10T10:00:00")
	params.Set("limit", "10")
	return params
}

func doRequest(t *testing.T, handler http.Handler, params url.Values) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, PathCandles+"?"+params.Encode(), nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// ----- construction panics --------------------------------------------

func TestNewCandleHandler_PanicsOnNilEngine(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic on nil engine")
		}
	}()
	NewCandleHandler(nil, Config{ReadTimeout: testHandlerReadTimeout})
}

func TestNewCandleHandler_PanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic on invalid Config (zero ReadTimeout)")
		}
	}()
	NewCandleHandler(&stubQueryEngine{}, Config{ReadTimeout: 0})
}

// ----- method guard ---------------------------------------------------

func TestServeHTTP_NonGET_405WithAllowHeader(t *testing.T) {
	handler := mustBuildHandler(t, &stubQueryEngine{}, Config{ReadTimeout: testHandlerReadTimeout})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			request := httptest.NewRequest(method, PathCandles, nil)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Errorf("status: got %d, want 405", recorder.Code)
			}
			if got := recorder.Header().Get("Allow"); got != http.MethodGet {
				t.Errorf("Allow header: got %q, want GET", got)
			}
		})
	}
}

// ----- 400 bad params (table-driven) ---------------------------------

// Each case mutates validParams() to drive one 400 branch. Status is
// 400, body contains an "error" field; the exact message isn't pinned
// (it's a human-readable hint, not a client-dispatched tag).
func TestServeHTTP_BadParams_400(t *testing.T) {
	engine := &stubQueryEngine{page: CandlePage{Candles: []Candle{}}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})

	cases := []struct {
		name   string
		mutate func(url.Values)
	}{
		{"missing_market", func(params url.Values) { params.Del("market") }},
		{"unknown_market", func(params url.Values) { params.Set("market", "ZZ") }},
		{"missing_instrument", func(params url.Values) { params.Del("instrument") }},
		{"missing_timeframe", func(params url.Values) { params.Del("timeframe") }},
		{"off_allowlist_timeframe", func(params url.Values) { params.Set("timeframe", "15m") }},
		{"off_allowlist_timeframe_1h", func(params url.Values) { params.Set("timeframe", "1h") }},
		{"timeframe_case_sensitive", func(params url.Values) { params.Set("timeframe", "5M") }},
		{"missing_from", func(params url.Values) { params.Del("from") }},
		{"missing_to", func(params url.Values) { params.Del("to") }},
		{"unparseable_from", func(params url.Values) { params.Set("from", "not-a-time") }},
		{"unparseable_to", func(params url.Values) { params.Set("to", "yesterday") }},
		{"from_equals_to", func(params url.Values) { params.Set("to", params.Get("from")) }},
		{"from_after_to", func(params url.Values) { params.Set("from", "2026-10-10T11:00:00") }},
		{"bad_cursor", func(params url.Values) { params.Set("cursor", "garbage") }},
		{"limit_zero", func(params url.Values) { params.Set("limit", "0") }},
		{"limit_negative", func(params url.Values) { params.Set("limit", "-5") }},
		{"limit_non_integer", func(params url.Values) { params.Set("limit", "ten") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := validParams()
			tc.mutate(params)
			recorder := doRequest(t, handler, params)
			if recorder.Code != http.StatusBadRequest {
				t.Errorf("status: got %d, want 400 (body=%s)", recorder.Code, recorder.Body.String())
			}
			var body errorBody
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Errorf("decode body: %v", err)
			}
			if body.Error == "" {
				t.Error("error body: missing 'error' field")
			}
			// Engine must not be called when the request fails validation.
			if engine.callCount != 0 {
				t.Errorf("engine called despite 400: callCount=%d", engine.callCount)
				engine.callCount = 0
			}
		})
	}
}

func TestServeHTTP_LimitAbsent_FallsBackToDefault(t *testing.T) {
	engine := &stubQueryEngine{page: CandlePage{Candles: []Candle{}}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	params := validParams()
	params.Del("limit")
	recorder := doRequest(t, handler, params)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", recorder.Code)
	}
	if engine.gotQuery.Limit != defaultCandleLimit {
		t.Errorf("Limit: got %d, want %d (default)", engine.gotQuery.Limit, defaultCandleLimit)
	}
}

// ----- 200 happy path -------------------------------------------------

// Empty candles[] is 200 — the durable-read family rule: empty ≠ error
// (DESIGN §4.5). A client walking the end of a window gets an empty
// array, never 404/409.
func TestServeHTTP_EmptyPage_200EmptyArrayNullCursor(t *testing.T) {
	engine := &stubQueryEngine{page: CandlePage{Candles: []Candle{}, NextCursor: nil}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handler, validParams())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", contentType)
	}
	var body CandleResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Candles == nil {
		t.Error("Candles: got null, want empty array")
	}
	if len(body.Candles) != 0 {
		t.Errorf("len: got %d, want 0", len(body.Candles))
	}
	if body.NextCursor != nil {
		t.Errorf("NextCursor: got %q, want null", *body.NextCursor)
	}
}

// ----- 503 durable-fail -----------------------------------------------

func TestServeHTTP_ErrLoadFailed_503(t *testing.T) {
	engine := &stubQueryEngine{err: fmt.Errorf("sim store fault: %w", ErrLoadFailed)}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handler, validParams())
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", recorder.Code)
	}
}

// A ctx timeout surfaced by the Store as a wrapped ErrLoadFailed must
// map to 503, NEVER 504. Durable reads don't emit 504 (DESIGN §4.5).
func TestServeHTTP_CtxTimeoutWrappedAsErrLoadFailed_503_NotFiveZeroFour(t *testing.T) {
	engine := &stubQueryEngine{
		err: fmt.Errorf("timeout: %w (%v)", ErrLoadFailed, context.DeadlineExceeded),
	}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handler, validParams())
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503 (durable reads never 504)", recorder.Code)
	}
}

// Defensive 500: a non-ErrLoadFailed error becomes 500. Should not fire
// in production (engine's only error source is the Store, which always
// wraps ErrLoadFailed) — test proves the fallback.
func TestServeHTTP_NonSentinelError_500(t *testing.T) {
	engine := &stubQueryEngine{err: errors.New("engine broke for unrelated reasons")}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handler, validParams())
	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500", recorder.Code)
	}
}

// ----- tz symmetry ----------------------------------------------------

// Request: naive-local IST time attaches market tz and converts to UTC
// instant. 09:00 IST = 03:30 UTC. The engine must receive the UTC
// instant.
func TestServeHTTP_RequestTime_ParsedAsMarketLocal_ConvertedToUTCInstant(t *testing.T) {
	engine := &stubQueryEngine{page: CandlePage{Candles: []Candle{}}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	params := validParams()
	params.Set("from", "2026-10-10T09:00:00") // IST 09:00
	params.Set("to", "2026-10-10T10:00:00")
	doRequest(t, handler, params)

	wantFromUTC := time.Date(2026, 10, 10, 3, 30, 0, 0, time.UTC)
	wantToUTC := time.Date(2026, 10, 10, 4, 30, 0, 0, time.UTC)
	if !engine.gotQuery.From.Equal(wantFromUTC) {
		t.Errorf("From instant: got %s, want %s (IST 09:00 == UTC 03:30)",
			engine.gotQuery.From, wantFromUTC)
	}
	if !engine.gotQuery.To.Equal(wantToUTC) {
		t.Errorf("To instant: got %s, want %s", engine.gotQuery.To, wantToUTC)
	}
}

// Response: a bar whose Start is UTC 03:30 renders as IST 09:00 on the
// wire. Verifies the full round trip (parse-in / format-out) through
// one handler call.
func TestServeHTTP_ResponseTime_FormattedAsMarketLocal(t *testing.T) {
	barStart := time.Date(2026, 10, 10, 3, 30, 0, 0, time.UTC)
	barEnd := barStart.Add(5 * time.Minute)
	engine := &stubQueryEngine{page: CandlePage{
		Candles: []Candle{{
			Market: "IN", Instrument: "ACME",
			Start: barStart, End: barEnd,
			Open: 100, High: 110, Low: 95, Close: 105, Volume: 500,
		}},
	}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handler, validParams())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body CandleResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Timezone != "Asia/Kolkata" {
		t.Errorf("Timezone: got %q, want Asia/Kolkata", body.Timezone)
	}
	if len(body.Candles) != 1 {
		t.Fatalf("len: got %d, want 1", len(body.Candles))
	}
	if body.Candles[0].Start != "2026-10-10T09:00:00" {
		t.Errorf("Start: got %q, want %q (IST render of UTC 03:30)",
			body.Candles[0].Start, "2026-10-10T09:00:00")
	}
	if body.Candles[0].End != "2026-10-10T09:05:00" {
		t.Errorf("End: got %q, want %q", body.Candles[0].End, "2026-10-10T09:05:00")
	}
}

// Request-echo: from / to come back on the response exactly as the
// client sent them (DESIGN §4.4). Simpler and less fragile than
// re-rendering from parsed instants.
func TestServeHTTP_FromToEchoedVerbatim(t *testing.T) {
	engine := &stubQueryEngine{page: CandlePage{Candles: []Candle{}}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	params := validParams()
	params.Set("from", "2026-10-10T09:00:00")
	params.Set("to", "2026-10-10T10:00:00")
	recorder := doRequest(t, handler, params)
	var body CandleResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.From != "2026-10-10T09:00:00" {
		t.Errorf("From: got %q, want echo of request", body.From)
	}
	if body.To != "2026-10-10T10:00:00" {
		t.Errorf("To: got %q, want echo of request", body.To)
	}
}

// Market / Instrument / Timeframe also echoed on the response for
// disambiguation across in-flight requests.
func TestServeHTTP_EchoesMarketInstrumentTimeframe(t *testing.T) {
	engine := &stubQueryEngine{page: CandlePage{Candles: []Candle{}}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handler, validParams())
	var body CandleResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Market != "IN" {
		t.Errorf("Market: got %q, want IN", body.Market)
	}
	if body.Instrument != "ACME" {
		t.Errorf("Instrument: got %q, want ACME", body.Instrument)
	}
	if body.Timeframe != "5m" {
		t.Errorf("Timeframe: got %q, want 5m", body.Timeframe)
	}
}

// ----- cursor round-trip ---------------------------------------------

// Engine emits a UTC NextCursor → handler formats as naive-local IST
// on the wire. Client sends that cursor back → handler parses as IST
// naive-local → engine receives the SAME UTC instant.
func TestServeHTTP_CursorRoundTrip_FormatOutParseIn(t *testing.T) {
	cursorUTC := time.Date(2026, 10, 10, 3, 50, 0, 0, time.UTC) // = IST 09:20
	engineOut := &stubQueryEngine{page: CandlePage{
		Candles:    []Candle{},
		NextCursor: &cursorUTC,
	}}
	handlerOut := mustBuildHandler(t, engineOut, Config{ReadTimeout: testHandlerReadTimeout})
	recorder := doRequest(t, handlerOut, validParams())
	var body CandleResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.NextCursor == nil {
		t.Fatal("NextCursor: got nil, want naive-local IST string")
	}
	const wantCursorString = "2026-10-10T09:20:00"
	if *body.NextCursor != wantCursorString {
		t.Errorf("NextCursor out: got %q, want %q (IST render of UTC 03:50)",
			*body.NextCursor, wantCursorString)
	}

	// Round-trip: send that cursor back as ?cursor=
	engineIn := &stubQueryEngine{page: CandlePage{Candles: []Candle{}}}
	handlerIn := mustBuildHandler(t, engineIn, Config{ReadTimeout: testHandlerReadTimeout})
	params := validParams()
	params.Set("cursor", wantCursorString)
	doRequest(t, handlerIn, params)
	if engineIn.gotQuery.AfterTS == nil {
		t.Fatal("AfterTS: got nil, want parsed UTC instant")
	}
	if !engineIn.gotQuery.AfterTS.Equal(cursorUTC) {
		t.Errorf("AfterTS parsed from cursor: got %s, want %s (same UTC instant)",
			*engineIn.gotQuery.AfterTS, cursorUTC)
	}
}

// ----- envelope shape ------------------------------------------------

// Keys on the wire are stable; no unit/currency field leaks (D-3 /
// P6-D33). A grep over the serialized body is simpler and more durable
// than asserting struct types.
func TestServeHTTP_ResponseEnvelope_StableKeys_NoUnitField(t *testing.T) {
	barStart := time.Date(2026, 10, 10, 3, 30, 0, 0, time.UTC)
	engine := &stubQueryEngine{page: CandlePage{
		Candles: []Candle{{
			Market: "IN", Instrument: "ACME",
			Start: barStart, End: barStart.Add(5 * time.Minute),
			Open: 100, High: 110, Low: 90, Close: 105, Volume: 1000,
		}},
	}}
	handler := mustBuildHandler(t, engine, Config{ReadTimeout: testHandlerReadTimeout})
	body := doRequest(t, handler, validParams()).Body.String()

	// Top-level keys.
	for _, key := range []string{
		`"market"`, `"instrument"`, `"timeframe"`, `"timezone"`,
		`"from"`, `"to"`, `"candles"`, `"next_cursor"`,
	} {
		if !strings.Contains(body, key) {
			t.Errorf("response missing expected key %s: body=%s", key, body)
		}
	}
	// Per-bar keys.
	for _, key := range []string{
		`"start"`, `"end"`, `"open"`, `"high"`, `"low"`, `"close"`, `"volume"`,
	} {
		if !strings.Contains(body, key) {
			t.Errorf("bar missing expected key %s: body=%s", key, body)
		}
	}
	// Forbidden fields (D-3 / P6-D33: no unit / currency anywhere).
	for _, forbidden := range []string{`"unit"`, `"currency"`} {
		if strings.Contains(body, forbidden) {
			t.Errorf("response leaked forbidden field %s: body=%s", forbidden, body)
		}
	}
}
