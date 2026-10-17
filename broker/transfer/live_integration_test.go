package transfer_test

// Live-runtime HTTP integration — against TEST_DATABASE_URL +
// TEST_REDIS_URL. Covers the real *funnel.Broker + *store.Store wiring
// behind the transfer handler. Skips when the test env is unset.
//
// This file lives in `package transfer_test` (external) so it can import
// broker/bringup. The internal http_test.go stays in `package transfer`
// because its stub-based tests need the unexported brokerPort / storePort
// interfaces. External test packages are compiled separately from the
// production package, which breaks the bringup→transfer→bringup cycle
// that would otherwise exist if these tests imported bringup from inside
// `package transfer`.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"simulator/broker/bringup"
	"simulator/broker/funnel"
	"simulator/broker/transfer"
	"simulator/connectivity/config"
	"simulator/contract"
	"simulator/economy/lifecycle"
)

const (
	envTestDBURL    = "TEST_DATABASE_URL"
	envTestRedisURL = "TEST_REDIS_URL"
)

// postJSON — local to this external test file. The internal http_test.go
// has its own identical copy; duplicating is lighter than exporting a
// prod-visible test helper.
func postJSON(t *testing.T, handler http.Handler, body any) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, transfer.Path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	responseRecorder := httptest.NewRecorder()
	handler.ServeHTTP(responseRecorder, request)

	responseBody := map[string]any{}
	if responseRecorder.Body.Len() > 0 {
		if err := json.Unmarshal(responseRecorder.Body.Bytes(), &responseBody); err != nil {
			t.Fatalf("decode response body: %v (raw=%q)", err, responseRecorder.Body.String())
		}
	}
	return responseRecorder.Code, responseBody
}

func requireLiveRuntime(t *testing.T, startingCapital, startingBank int64) *bringup.Runtime {
	t.Helper()
	dbDSN := os.Getenv(envTestDBURL)
	redisURL := os.Getenv(envTestRedisURL)
	if dbDSN == "" || redisURL == "" {
		t.Skipf("%s / %s not set — live-runtime test skipped", envTestDBURL, envTestRedisURL)
	}
	infra := config.Config{
		Postgres: config.PostgresConfig{DSN: dbDSN, PingTimeout: 5 * time.Second, MaxConns: 4},
		Redis:    config.RedisConfig{URL: redisURL, PingTimeout: 5 * time.Second},
	}
	runtime, err := bringup.BringUpForTest(context.Background(), bringup.BrokerConfig{
		BrokerID:        "B-transfer-http-integ",
		ShardCount:      1,
		InboxDepth:      4,
		Country:         "IN",
		StartingCapital: startingCapital,
		StartingBank:    startingBank,
	}, infra)
	if err != nil {
		t.Fatalf("bringUp: %v", err)
	}
	t.Cleanup(runtime.Shutdown)
	return runtime
}

func cleanupOwner(t *testing.T, runtime *bringup.Runtime, ownerPrefix string) {
	t.Helper()
	t.Cleanup(func() {
		pool := runtime.Store.Pool()
		if pool == nil {
			return
		}
		ctx := context.Background()
		likePattern := ownerPrefix + "%"
		_, _ = pool.Exec(ctx, `DELETE FROM trade_costs WHERE trade_id IN (
			SELECT pt.trade_id FROM participant_trades pt
			JOIN accounts acc ON acc.account_id = pt.account_id
			WHERE acc.owner_id LIKE $1)`, likePattern)
		_, _ = pool.Exec(ctx, `DELETE FROM participant_trades WHERE account_id IN (
			SELECT account_id FROM accounts WHERE owner_id LIKE $1)`, likePattern)
		_, _ = pool.Exec(ctx, `DELETE FROM accounts WHERE owner_id LIKE $1`, likePattern)
	})
}

func uniquePrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("httptr-%s-%d", t.Name(), time.Now().UnixNano())
}

func installLiveSession(t *testing.T, runtime *bringup.Runtime, ownerPrefix string) funnel.AccountID {
	t.Helper()
	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	cmd := funnel.SessionStartCmd{
		AccountID:       accountID,
		SessionID:       ownerPrefix + "-sess",
		Mode:            lifecycle.Canonical,
		ExchangeID:      "X-http-integ",
		ExchangeInbound: make(chan contract.InboundMessage, 1),
		Participant:     "P-http-integ",
	}
	if err := runtime.Broker.StartSession(context.Background(), cmd); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	return accountID
}

func liveHandler(runtime *bringup.Runtime) http.Handler {
	return transfer.NewHandler(runtime.Broker, runtime.Store, transfer.Config{TransferTimeout: 5 * time.Second})
}

// H1 live push via the full stack.
func TestHTTP_Live_PushSuccess(t *testing.T) {
	const capital, bank, amount int64 = 10_000, 5_000, 3_000
	ownerPrefix := uniquePrefix(t)
	runtime := requireLiveRuntime(t, capital, bank)
	cleanupOwner(t, runtime, ownerPrefix)
	accountID := installLiveSession(t, runtime, ownerPrefix)

	status, body := postJSON(t, liveHandler(runtime), map[string]any{
		"account_id": string(accountID), "direction": "to_bank", "amount": amount,
	})
	if status != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%v)", status, body)
	}
	if gotAvailable := int64(body["available"].(float64)); gotAvailable != capital-amount {
		t.Errorf("available: got %d, want %d", gotAvailable, capital-amount)
	}
	if gotBank := int64(body["bank"].(float64)); gotBank != bank+amount {
		t.Errorf("bank: got %d, want %d", gotBank, bank+amount)
	}
}

// H2 live pull via the full stack.
func TestHTTP_Live_PullSuccess(t *testing.T) {
	const capital, bank, amount int64 = 10_000, 5_000, 2_000
	ownerPrefix := uniquePrefix(t)
	runtime := requireLiveRuntime(t, capital, bank)
	cleanupOwner(t, runtime, ownerPrefix)
	accountID := installLiveSession(t, runtime, ownerPrefix)

	status, body := postJSON(t, liveHandler(runtime), map[string]any{
		"account_id": string(accountID), "direction": "to_trading", "amount": amount,
	})
	if status != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%v)", status, body)
	}
	if gotAvailable := int64(body["available"].(float64)); gotAvailable != capital+amount {
		t.Errorf("available: got %d, want %d", gotAvailable, capital+amount)
	}
	if gotBank := int64(body["bank"].(float64)); gotBank != bank-amount {
		t.Errorf("bank: got %d, want %d", gotBank, bank-amount)
	}
}

// H5 idle push via the full stack — EnsureAccount, NO StartSession, call
// the handler. Must route-first-miss and land on durable, returning the
// same mode-blind {available, bank} shape as H1.
func TestHTTP_Live_IdlePush(t *testing.T) {
	const capital, bank, amount int64 = 10_000, 5_000, 1_000
	ownerPrefix := uniquePrefix(t)
	runtime := requireLiveRuntime(t, capital, bank)
	cleanupOwner(t, runtime, ownerPrefix)
	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	// NO StartSession — the handler must fall to durable.

	status, body := postJSON(t, liveHandler(runtime), map[string]any{
		"account_id": string(accountID), "direction": "to_bank", "amount": amount,
	})
	if status != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%v)", status, body)
	}
	if _, ok := body["available"]; !ok {
		t.Errorf("idle 200 body missing 'available': %v", body)
	}
	if _, ok := body["bank"]; !ok {
		t.Errorf("idle 200 body missing 'bank': %v", body)
	}
}

// H8 idle unknown account → 404 (durable's ErrNoAccount).
func TestHTTP_Live_IdleUnknownAccount_404(t *testing.T) {
	runtime := requireLiveRuntime(t, 0, 0)
	// no cleanup needed — nothing to insert
	status, _ := postJSON(t, liveHandler(runtime), map[string]any{
		"account_id": "acct-does-not-exist",
		"direction":  "to_bank",
		"amount":     1,
	})
	if status != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", status)
	}
}

// H15 concurrent transfer + order via the handler — the shard loop
// serializes; -race is the oracle; final balances must reflect every
// successful Transfer.
func TestHTTP_Live_ConcurrentWithPlaceOrder(t *testing.T) {
	const capital, bank int64 = 100_000, 50_000
	const iterations = 30

	ownerPrefix := uniquePrefix(t)
	runtime := requireLiveRuntime(t, capital, bank)
	cleanupOwner(t, runtime, ownerPrefix)
	accountID := installLiveSession(t, runtime, ownerPrefix)
	handler := liveHandler(runtime)

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			status, _ := postJSON(t, handler, map[string]any{
				"account_id": string(accountID), "direction": "to_bank", "amount": 1,
			})
			if status != http.StatusOK {
				t.Errorf("transfer iteration %d: got %d, want 200", iteration, status)
				return
			}
		}
	}()
	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			if _, err := runtime.Broker.PlaceOrder(context.Background(), accountID, ownerPrefix+"-sess", funnel.OrderSpec{}); err != nil {
				t.Errorf("placeorder iteration %d: %v", iteration, err)
				return
			}
		}
	}()
	waitGroup.Wait()

	// A final 1-coin push verifies every prior Transfer landed on the
	// shard (no lost commands).
	status, body := postJSON(t, handler, map[string]any{
		"account_id": string(accountID), "direction": "to_bank", "amount": 1,
	})
	if status != http.StatusOK {
		t.Fatalf("final probe: got %d, want 200", status)
	}
	if gotAvailable := int64(body["available"].(float64)); gotAvailable != capital-iterations-1 {
		t.Errorf("final available: got %d, want %d (concurrent Transfer lost)", gotAvailable, capital-iterations-1)
	}
	if gotBank := int64(body["bank"].(float64)); gotBank != bank+iterations+1 {
		t.Errorf("final bank: got %d, want %d (concurrent Transfer lost)", gotBank, bank+iterations+1)
	}
}
