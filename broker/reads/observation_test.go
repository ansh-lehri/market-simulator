package reads

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/contract"
)

// Tests for the observation endpoint — the composite that fans a live
// shard read and a Postgres recent-trades read out concurrently, then
// merges them. Covers:
//
//   - 400 / 405 / 504 / 500 / 409 on the live leg
//   - 200 when both legs succeed, with the merged body shape and a
//     call-count witness that both legs actually fired
//   - 200 with trades_degraded:true when the durable leg fails (the
//     "degrade on durable failure, never fail the response" rule)
//   - 504 on live timeout even if the durable half returned fine
//     (live failure must not be masked by durable success)

func serveObservation(t *testing.T, broker *stubBroker, durableStore *stubStore, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewObservationHandler(broker, durableStore, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestObservation_StatusMap_LiveLeg(t *testing.T) {
	cases := []struct {
		name       string
		url        string
		method     string
		broker     *stubBroker
		cfg        Config
		wantStatus int
	}{
		{"400 missing aid", PathObservation, http.MethodGet, &stubBroker{}, defaultTestConfig, http.StatusBadRequest},
		{"405 non-GET", PathObservation + "?account_id=acct-1", http.MethodPost, &stubBroker{}, defaultTestConfig, http.StatusMethodNotAllowed},
		{"504 live timeout", PathObservation + "?account_id=acct-1", http.MethodGet,
			&stubBroker{delay: 100 * time.Millisecond}, Config{ReadTimeout: 10 * time.Millisecond}, http.StatusGatewayTimeout},
		{"500 live unexpected", PathObservation + "?account_id=acct-1", http.MethodGet,
			&stubBroker{observationErr: http.ErrServerClosed}, defaultTestConfig, http.StatusInternalServerError},
		{"409 live no-session", PathObservation + "?account_id=acct-1", http.MethodGet,
			&stubBroker{observationReply: funnel.ObservationLive{Present: false}}, defaultTestConfig, http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveObservation(t, tc.broker, &stubStore{}, tc.cfg, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

func TestObservation_200_BothLegsOk(t *testing.T) {
	broker := &stubBroker{observationReply: funnel.ObservationLive{
		Present: true,
		Balance: funnel.BalanceSnapshot{Present: true, Available: 100, Bank: 50},
		Positions: []funnel.PositionView{
			{Instrument: contract.InstrumentID("ACME"), Qty: 10, WeightedAvgEntry: 100},
		},
		OpenOrders: []funnel.OpenOrderView{
			{OrderID: "acct-1:1", Instrument: contract.InstrumentID("ACME"), Side: contract.Buy, OrigQty: 10, RemainingQty: 10, Status: funnel.OrderStatusWorking},
		},
	}}
	tradeAt := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	durableStore := &stubStore{recentReply: []store.TradeRow{
		{TradeID: "t-1", InstrumentID: "ACME", Direction: "long", Quantity: 5, EntryPrice: 100, ExitPrice: 110,
			EntryAt: tradeAt, ExitAt: tradeAt, GrossPnl: 50, TotalCosts: 2, NetPnl: 48, PnlPct: 0.1, ExitReason: "manual"},
	}}

	recorder := serveObservation(t, broker, durableStore, defaultTestConfig, http.MethodGet, PathObservation+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"balance", "positions", "orders", "recent_trades", "trades_degraded"} {
		if _, present := body[key]; !present {
			t.Errorf("missing key %q in response: %+v", key, body)
		}
	}
	if body["trades_degraded"].(bool) != false {
		t.Errorf("trades_degraded = %v, want false", body["trades_degraded"])
	}
	trades := body["recent_trades"].([]any)
	if len(trades) != 1 {
		t.Errorf("recent_trades len = %d, want 1", len(trades))
	}
	if broker.observationCalls.Load() != 1 || durableStore.recentCalls.Load() != 1 {
		t.Errorf("both legs must fire exactly once: observationCalls=%d, recentCalls=%d, want 1 each",
			broker.observationCalls.Load(), durableStore.recentCalls.Load())
	}
}

// A durable failure must not escalate — the live snapshot is the whole
// value of this endpoint, so the response is still 200 with an empty
// recent_trades slice and trades_degraded:true marking the gap.
func TestObservation_200_DegradeOnDurableFailure(t *testing.T) {
	broker := &stubBroker{observationReply: funnel.ObservationLive{Present: true}}
	durableStore := &stubStore{recentErr: http.ErrServerClosed}

	recorder := serveObservation(t, broker, durableStore, defaultTestConfig, http.MethodGet, PathObservation+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (degrade-don't-fail)", recorder.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["trades_degraded"].(bool) != true {
		t.Errorf("trades_degraded = %v, want true", body["trades_degraded"])
	}
	trades, _ := body["recent_trades"].([]any)
	if len(trades) != 0 {
		t.Errorf("recent_trades len = %d, want 0 (empty on degrade)", len(trades))
	}
}

// If the live leg hits the ctx deadline before replying, the handler
// returns 504 regardless of whether the durable leg completed — the
// client must not get a merged body with a stale/missing live snapshot.
func TestObservation_504_LiveTimeoutBeatsDurableSuccess(t *testing.T) {
	broker := &stubBroker{delay: 100 * time.Millisecond}
	durableStore := &stubStore{recentReply: []store.TradeRow{{TradeID: "t-1"}}}
	cfg := Config{ReadTimeout: 10 * time.Millisecond}

	recorder := serveObservation(t, broker, durableStore, cfg, http.MethodGet, PathObservation+"?account_id=acct-1")
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", recorder.Code)
	}
}

func TestNewObservationHandler_PanicsOnNilDeps(t *testing.T) {
	cases := []struct {
		name   string
		broker brokerPort
		store  storePort
	}{
		{"nil broker", nil, &stubStore{}},
		{"nil store", &stubBroker{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			_ = NewObservationHandler(tc.broker, tc.store, defaultTestConfig)
		})
	}
}
