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

// Tests for the open-orders endpoint — status map plus body shape:
// the enum stringification (side/status), the derived filled_qty, and
// the cancel-in-flight / is-market flags.

func serveOrders(t *testing.T, broker *stubBroker, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewOrdersHandler(broker, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestOrders_StatusMap(t *testing.T) {
	cases := []struct {
		name       string
		url        string
		method     string
		broker     *stubBroker
		cfg        Config
		wantStatus int
	}{
		{"400 missing aid", PathOrders, http.MethodGet, &stubBroker{}, defaultTestConfig, http.StatusBadRequest},
		{"405 non-GET", PathOrders + "?account_id=acct-1", http.MethodPost, &stubBroker{}, defaultTestConfig, http.StatusMethodNotAllowed},
		{"504 timeout", PathOrders + "?account_id=acct-1", http.MethodGet,
			&stubBroker{delay: 100 * time.Millisecond}, Config{ReadTimeout: 10 * time.Millisecond}, http.StatusGatewayTimeout},
		{"500 unexpected", PathOrders + "?account_id=acct-1", http.MethodGet,
			&stubBroker{ordersErr: http.ErrServerClosed}, defaultTestConfig, http.StatusInternalServerError},
		{"409 no live session", PathOrders + "?account_id=acct-1", http.MethodGet,
			&stubBroker{ordersReply: funnel.OpenOrdersSnapshot{Present: false}}, defaultTestConfig, http.StatusConflict},
		{"200 empty working set", PathOrders + "?account_id=acct-1", http.MethodGet,
			&stubBroker{ordersReply: funnel.OpenOrdersSnapshot{Present: true, Orders: nil}}, defaultTestConfig, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveOrders(t, tc.broker, tc.cfg, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

func TestOrders_200_BodyShape(t *testing.T) {
	broker := &stubBroker{
		ordersReply: funnel.OpenOrdersSnapshot{Present: true, Orders: []funnel.OpenOrderView{
			{
				OrderID: "acct-1:1", Instrument: contract.InstrumentID("ACME"),
				Side: contract.Buy, IsMarket: false, LimitPrice: 100,
				OrigQty: 10, RemainingQty: 4, ReservedCash: 400, ReservedUnits: 0,
				Status: funnel.OrderStatusPartiallyFilled, CancelInFlight: true,
			},
		}},
	}
	recorder := serveOrders(t, broker, defaultTestConfig, http.MethodGet, PathOrders+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Orders []map[string]any `json:"orders"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Orders) != 1 {
		t.Fatalf("want 1 row, got %d", len(body.Orders))
	}
	row := body.Orders[0]
	if row["side"] != "buy" {
		t.Errorf("side = %v, want \"buy\"", row["side"])
	}
	if row["status"] != "partially_filled" {
		t.Errorf("status = %v, want \"partially_filled\"", row["status"])
	}
	if row["filled_qty"].(float64) != 6 { // 10 - 4
		t.Errorf("filled_qty = %v, want 6", row["filled_qty"])
	}
	if row["cancel_in_flight"].(bool) != true {
		t.Errorf("cancel_in_flight = %v, want true", row["cancel_in_flight"])
	}
	if row["is_market"].(bool) != false {
		t.Errorf("is_market = %v, want false", row["is_market"])
	}
	if _, present := row["unit"]; present {
		t.Fatalf("response must not include a 'unit' field — all money is bare int64 here")
	}
}

func TestNewOrdersHandler_PanicsOnNilBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on nil broker")
		}
	}()
	_ = NewOrdersHandler(nil, defaultTestConfig)
}
