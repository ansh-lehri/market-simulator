package reads

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/contract"
)

// Tests for the positions endpoint — status map, the ?instrument=
// filter threading, body shape, and the "unheld instrument is still a
// live 200" rule.

func servePositions(t *testing.T, broker *stubBroker, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewPositionsHandler(broker, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestPositions_StatusMap(t *testing.T) {
	cases := []struct {
		name       string
		url        string
		method     string
		broker     *stubBroker
		cfg        Config
		wantStatus int
	}{
		{"400 missing aid", PathPositions, http.MethodGet, &stubBroker{}, defaultTestConfig, http.StatusBadRequest},
		{"405 non-GET", PathPositions + "?account_id=acct-1", http.MethodPost, &stubBroker{}, defaultTestConfig, http.StatusMethodNotAllowed},
		{"504 timeout", PathPositions + "?account_id=acct-1", http.MethodGet,
			&stubBroker{delay: 100 * time.Millisecond}, Config{ReadTimeout: 10 * time.Millisecond}, http.StatusGatewayTimeout},
		{"500 unexpected", PathPositions + "?account_id=acct-1", http.MethodGet,
			&stubBroker{positionsErr: http.ErrServerClosed}, defaultTestConfig, http.StatusInternalServerError},
		{"409 no live session", PathPositions + "?account_id=acct-1", http.MethodGet,
			&stubBroker{positionsReply: funnel.PositionsSnapshot{Present: false}}, defaultTestConfig, http.StatusConflict},
		{"200 live flat", PathPositions + "?account_id=acct-1", http.MethodGet,
			&stubBroker{positionsReply: funnel.PositionsSnapshot{Present: true, Positions: nil}}, defaultTestConfig, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := servePositions(t, tc.broker, tc.cfg, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

// Instrument filter must be threaded verbatim from ?instrument= to the
// broker facade (not stripped, not case-folded).
func TestPositions_InstrumentFilterThreaded(t *testing.T) {
	broker := &stubBroker{positionsReply: funnel.PositionsSnapshot{Present: true}}
	servePositions(t, broker, defaultTestConfig, http.MethodGet, PathPositions+"?account_id=acct-1&instrument=ACME")
	if broker.positionsInstrument != funnel.InstrumentID("ACME") {
		t.Fatalf("broker saw instrument=%q, want %q", broker.positionsInstrument, "ACME")
	}
}

func TestPositions_200_BodyShape(t *testing.T) {
	broker := &stubBroker{
		positionsReply: funnel.PositionsSnapshot{Present: true, Positions: []funnel.PositionView{
			{
				Instrument: contract.InstrumentID("ACME"), Qty: 10, ReservedQty: 2, WeightedAvgEntry: 100,
				MarkAvailable: true, LTP: 115, GrossUnrealized: 150,
			},
		}},
	}
	recorder := servePositions(t, broker, defaultTestConfig, http.MethodGet, PathPositions+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Positions []map[string]any `json:"positions"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Positions) != 1 {
		t.Fatalf("want 1 row, got %d", len(body.Positions))
	}
	row := body.Positions[0]
	if row["instrument"] != "ACME" || row["qty"].(float64) != 10 || row["gross_unrealized"].(float64) != 150 {
		t.Fatalf("shape mismatch: %+v", row)
	}
	if _, present := row["unit"]; present {
		t.Fatalf("response must not include a 'unit' field — all money is bare int64 here")
	}
}

// A specified instrument the account doesn't hold is still a live
// response — the account exists, the position on that instrument is
// just flat. Returns 200 with {positions:[]}, never 404.
func TestPositions_200_EmptyWhenInstrumentNotHeld(t *testing.T) {
	broker := &stubBroker{positionsReply: funnel.PositionsSnapshot{Present: true, Positions: []funnel.PositionView{}}}
	recorder := servePositions(t, broker, defaultTestConfig, http.MethodGet, PathPositions+"?account_id=acct-1&instrument=UNHELD")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Positions []map[string]any `json:"positions"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// shapePositions preallocates an empty slice so the encoder emits
	// [] rather than null — clients iterate the array, not type-check it.
	if body.Positions == nil {
		t.Fatalf("positions must serialize as [], not null")
	}
	if len(body.Positions) != 0 {
		t.Fatalf("want empty positions, got %d rows", len(body.Positions))
	}
}

func TestNewPositionsHandler_PanicsOnNilBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on nil broker")
		}
	}()
	_ = NewPositionsHandler(nil, defaultTestConfig)
}
