package reads

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/funnel"
)

// Tests for the balance endpoint — covers every status the handler can
// return (400 / 405 / 504 / 500 / 409 / 200), the 200 body shape, and
// the construction-time panic guards.

func serveBalance(t *testing.T, broker *stubBroker, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewBalanceHandler(broker, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestBalance_400_MissingAccountID(t *testing.T) {
	recorder := serveBalance(t, &stubBroker{}, defaultTestConfig, http.MethodGet, PathBalance)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestBalance_405_NonGET(t *testing.T) {
	recorder := serveBalance(t, &stubBroker{}, defaultTestConfig, http.MethodPost, PathBalance+"?account_id=acct-1")
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
	if allow := recorder.Header().Get("Allow"); allow != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", allow)
	}
}

func TestBalance_504_BrokerTimeout(t *testing.T) {
	broker := &stubBroker{delay: 100 * time.Millisecond}
	cfg := Config{ReadTimeout: 10 * time.Millisecond}
	recorder := serveBalance(t, broker, cfg, http.MethodGet, PathBalance+"?account_id=acct-1")
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", recorder.Code)
	}
}

func TestBalance_500_UnexpectedError(t *testing.T) {
	broker := &stubBroker{balanceErr: http.ErrServerClosed}
	recorder := serveBalance(t, broker, defaultTestConfig, http.MethodGet, PathBalance+"?account_id=acct-1")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
}

func TestBalance_409_NoLiveSession(t *testing.T) {
	broker := &stubBroker{balanceReply: funnel.BalanceSnapshot{Present: false}}
	recorder := serveBalance(t, broker, defaultTestConfig, http.MethodGet, PathBalance+"?account_id=acct-1")
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
}

func TestBalance_200_FourBucketBody(t *testing.T) {
	broker := &stubBroker{
		balanceReply: funnel.BalanceSnapshot{Present: true, Available: 10, Reserved: 20, Settled: 30, Bank: 40},
	}
	recorder := serveBalance(t, broker, defaultTestConfig, http.MethodGet, PathBalance+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body map[string]int64
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	want := map[string]int64{"available": 10, "reserved": 20, "settled": 30, "bank": 40}
	if len(body) != len(want) {
		t.Fatalf("body has %d keys, want %d (full body: %+v)", len(body), len(want), body)
	}
	for key, wantValue := range want {
		if body[key] != wantValue {
			t.Errorf("body[%q] = %d, want %d", key, body[key], wantValue)
		}
	}
}

// A nil broker at construction must panic — the factory's job is to
// catch bringup mistakes early instead of nil-dereffing on first request.
func TestNewBalanceHandler_PanicsOnNilBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on nil broker")
		}
	}()
	_ = NewBalanceHandler(nil, defaultTestConfig)
}

func TestNewBalanceHandler_PanicsOnBadConfig(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on bad config")
		}
	}()
	_ = NewBalanceHandler(&stubBroker{}, Config{ReadTimeout: 0})
}
