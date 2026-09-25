package persistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// buildDrainerComponentRedisOnly returns a Component wired to Redis only
// (nil Postgres). Only the drainer lifecycle tests should use this — as
// long as the buffer is EMPTY, drainOnce never reaches Postgres, so nil
// db is safe. Any test that enqueues a row before Start MUST switch to
// the full pool-based helper below.
func buildDrainerComponentRedisOnly(t *testing.T, tick time.Duration) *Component {
	t.Helper()
	client, namespace := requireRedis(t)
	cfg := Config{
		KeyNamespace: namespace,
		DrainerTick:  tick,
		DrainerBatch: 4,
	}.withDefaults()
	storeInstance := newStore(nil, client, IndiaCostAdapter{}, cfg)
	return &Component{
		store:   storeInstance,
		dirty:   make(map[dirtyKey]struct{}),
		cfg:     cfg,
		drainer: newDrainer(storeInstance, cfg),
	}
}

// buildDrainerStoreWithTx returns a store wired to (tx pool, real Redis).
// Used by drainOnce tests that need both sides — the outer tx keeps
// Postgres clean via rollback; Redis is cleaned via the namespaced helper.
func buildDrainerStoreWithTx(t *testing.T) (*store, string) {
	t.Helper()
	tx := beginTestTx(t)
	client, namespace := requireRedis(t)
	cfg := Config{KeyNamespace: namespace, DrainerBatch: 8}.withDefaults()
	return newStore(tx, client, IndiaCostAdapter{}, cfg), namespace
}

func TestBackoffDelayCap(t *testing.T) {
	base := 100 * time.Millisecond
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, base},                      // 1x
		{1, 2 * base},                  // 2x
		{2, 4 * base},                  // 4x
		{5, 32 * base},                 // 32x
		{10, 32 * base},                // capped at 32x
		{63, 32 * base},                // capped even at pathological input
	}
	for _, testCase := range cases {
		got := backoffDelay(base, testCase.failures)
		if got != testCase.want {
			t.Errorf("backoffDelay(base=%v, failures=%d): got %v, want %v",
				base, testCase.failures, got, testCase.want)
		}
	}
}

func TestDrainerDrainOnceEmptyBufferNoop(t *testing.T) {
	component := buildDrainerComponentRedisOnly(t, 10*time.Millisecond)
	failures, err := component.drainer.drainOnce(context.Background())
	if err != nil {
		t.Fatalf("drainOnce on empty buffer: %v", err)
	}
	if failures != 0 {
		t.Errorf("drainOnce empty buffer failures: got %d, want 0", failures)
	}
}

func TestDrainerDrainOnceHappyPathLandsAllRowsInPostgres(t *testing.T) {
	sto, _ := buildDrainerStoreWithTx(t)
	accountID := fmt.Sprintf("acct-drn-%d", time.Now().UnixNano())
	seedAccount(t, sto.db, accountID, 1_000_000, 0)

	tradeIDs := []string{
		fmt.Sprintf("trd-drn-a-%d", time.Now().UnixNano()),
		fmt.Sprintf("trd-drn-b-%d", time.Now().UnixNano()),
		fmt.Sprintf("trd-drn-c-%d", time.Now().UnixNano()),
	}
	ctx := context.Background()
	for _, tradeID := range tradeIDs {
		insert := baseTradeInsert(tradeID, accountID)
		if err := sto.pushClosedTrade(ctx, insert); err != nil {
			t.Fatalf("push %s: %v", tradeID, err)
		}
	}

	worker := newDrainer(sto, sto.cfg)
	failures, err := worker.drainOnce(ctx)
	if err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if failures != 0 {
		t.Errorf("failures: got %d, want 0", failures)
	}

	// Buffer must be empty.
	remaining, err := sto.drainClosedTrades(ctx, 10)
	if err != nil {
		t.Fatalf("drainClosedTrades post-check: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("buffer not empty after happy drain: %d rows left", len(remaining))
	}

	// Postgres must hold all three rows.
	for _, tradeID := range tradeIDs {
		var count int
		err := sto.db.QueryRow(ctx,
			`SELECT COUNT(*) FROM participant_trades WHERE trade_id = $1`, tradeID).Scan(&count)
		if err != nil {
			t.Fatalf("count %s: %v", tradeID, err)
		}
		if count != 1 {
			t.Errorf("participant_trades[%s]: got %d rows, want 1", tradeID, count)
		}
	}
}

func TestDrainerRequeuesRowOnInsertFailure(t *testing.T) {
	// D5: on Postgres error, the row stays in the buffer. We simulate the
	// error via an FK violation (account_id does not exist). After the
	// drainer's requeue, the row must appear on the next drainClosedTrades.
	sto, _ := buildDrainerStoreWithTx(t)
	tradeID := fmt.Sprintf("trd-fail-%d", time.Now().UnixNano())

	// Push a row whose account_id doesn't exist — fk_participant_trades_account
	// will reject the INSERT.
	badInsert := baseTradeInsert(tradeID, "acct-nonexistent")
	ctx := context.Background()
	if err := sto.pushClosedTrade(ctx, badInsert); err != nil {
		t.Fatalf("push bad row: %v", err)
	}

	worker := newDrainer(sto, sto.cfg)
	failures, err := worker.drainOnce(ctx)
	if err != nil {
		t.Fatalf("drainOnce error path: %v", err)
	}
	if failures != 1 {
		t.Errorf("failures: got %d, want 1", failures)
	}

	// The row must be back on the buffer for the next tick.
	remaining, err := sto.drainClosedTrades(ctx, 10)
	if err != nil {
		t.Fatalf("post-check drain: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("row lost from buffer after insert failure: got %d rows, want 1", len(remaining))
	}
	if remaining[0].TradeID != tradeID {
		t.Errorf("wrong row requeued: got %s, want %s", remaining[0].TradeID, tradeID)
	}

	// Postgres must NOT have the row (the savepoint rolled back).
	var count int
	err = sto.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM participant_trades WHERE trade_id = $1`, tradeID).Scan(&count)
	if err != nil {
		t.Fatalf("count %s: %v", tradeID, err)
	}
	if count != 0 {
		t.Errorf("participant_trades[%s]: got %d, want 0 (savepoint should have rolled back)", tradeID, count)
	}
}

func TestDrainerReplayIsIdempotent(t *testing.T) {
	// Enqueue the same trade_id twice; drainer must land ONE row in
	// Postgres and treat the second pop as a no-op via ON CONFLICT.
	sto, _ := buildDrainerStoreWithTx(t)
	accountID := fmt.Sprintf("acct-idem-drn-%d", time.Now().UnixNano())
	seedAccount(t, sto.db, accountID, 0, 0)
	tradeID := fmt.Sprintf("trd-idem-drn-%d", time.Now().UnixNano())

	insert := baseTradeInsert(tradeID, accountID)
	ctx := context.Background()
	if err := sto.pushClosedTrade(ctx, insert); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	if err := sto.pushClosedTrade(ctx, insert); err != nil {
		t.Fatalf("push 2: %v", err)
	}

	worker := newDrainer(sto, sto.cfg)
	failures, err := worker.drainOnce(ctx)
	if err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if failures != 0 {
		t.Errorf("failures: got %d, want 0 (ON CONFLICT is a silent no-op)", failures)
	}

	var count int
	err = sto.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM participant_trades WHERE trade_id = $1`, tradeID).Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("participant_trades[%s]: got %d, want 1 (idempotent replay)", tradeID, count)
	}
}

func TestDrainerBatchSizeCap(t *testing.T) {
	// DrainerBatch bounds one drainOnce call. Push (batch+extra) rows;
	// after one drainOnce, exactly `extra` remain.
	sto, _ := buildDrainerStoreWithTx(t)
	accountID := fmt.Sprintf("acct-cap-%d", time.Now().UnixNano())
	seedAccount(t, sto.db, accountID, 0, 0)

	sto.cfg.DrainerBatch = 3 // small so the test finishes fast
	const extra = 2
	total := sto.cfg.DrainerBatch + extra

	ctx := context.Background()
	for tradeIndex := 0; tradeIndex < total; tradeIndex++ {
		tradeID := fmt.Sprintf("trd-cap-%d-%d", time.Now().UnixNano(), tradeIndex)
		insert := baseTradeInsert(tradeID, accountID)
		if err := sto.pushClosedTrade(ctx, insert); err != nil {
			t.Fatalf("push %d: %v", tradeIndex, err)
		}
	}

	worker := newDrainer(sto, sto.cfg)
	failures, err := worker.drainOnce(ctx)
	if err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if failures != 0 {
		t.Errorf("failures: got %d, want 0", failures)
	}

	remaining, err := sto.drainClosedTrades(ctx, 10)
	if err != nil {
		t.Fatalf("post-check drain: %v", err)
	}
	if len(remaining) != extra {
		t.Errorf("batch cap not honored: %d rows left, want %d", len(remaining), extra)
	}
}

func TestDrainerStartStopLifecycleCleanShutdown(t *testing.T) {
	// Empty-buffer lifecycle test — no Postgres side effects. Verify Start
	// launches a goroutine that Stop can join cleanly.
	component := buildDrainerComponentRedisOnly(t, 20*time.Millisecond)

	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Give the loop a couple of ticks to actually run.
	time.Sleep(80 * time.Millisecond)

	// Stop must return within a bounded time — bounded by one drainOnce.
	stopReturned := make(chan struct{})
	go func() {
		component.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
		// clean
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s — drainer goroutine leaked or blocked")
	}
}

func TestDrainerDoubleStartReturnsError(t *testing.T) {
	component := buildDrainerComponentRedisOnly(t, 50*time.Millisecond)

	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	t.Cleanup(component.Stop)

	err := component.Start(context.Background())
	if err == nil {
		t.Fatal("second Start returned nil, expected error")
	}
	if !strings.Contains(err.Error(), "already started") {
		t.Errorf("second Start error: got %v, expected 'already started'", err)
	}
}

func TestDrainerStopIdempotent(t *testing.T) {
	// Stop before Start: no-op. Stop twice after Start: second call is
	// no-op. Both must be panic-free.
	component := buildDrainerComponentRedisOnly(t, 50*time.Millisecond)

	// Pre-start stop.
	component.Stop()

	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	component.Stop()

	// Second Stop: must not panic, must not block.
	stopReturned := make(chan struct{})
	go func() {
		component.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(1 * time.Second):
		t.Fatal("second Stop blocked — Stop is not idempotent")
	}
}

func TestDrainerContextCancellationExitsGoroutine(t *testing.T) {
	// ctx cancellation is an alternate exit path (same effect as Stop).
	component := buildDrainerComponentRedisOnly(t, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	if err := component.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cancel()

	// The goroutine should observe the cancellation and finish; Stop
	// after cancel should return promptly (doneCh closed by run's defer).
	stopReturned := make(chan struct{})
	go func() {
		component.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not exit on ctx.Cancel within 2s")
	}
}

func TestDrainerGoroutineActuallyDrains(t *testing.T) {
	// End-to-end lifecycle: enqueue rows BEFORE Start, run the drainer for
	// a few ticks, Stop, verify the buffer is empty and Postgres has the
	// rows. Uses the full pool (not a tx) so the drainer's goroutine can
	// safely share the pool with the test's main goroutine. Cleanup: DELETE
	// rows manually and DELETE the seeded account.
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)

	cfg := Config{
		KeyNamespace: namespace,
		DrainerTick:  20 * time.Millisecond,
		DrainerBatch: 4,
	}.withDefaults()
	component := NewComponent(pool, client, IndiaCostAdapter{}, cfg)

	// Seed one account and manual cleanup.
	accountID := fmt.Sprintf("acct-drn-e2e-%d", time.Now().UnixNano())
	ctx := context.Background()
	_, err := pool.Exec(ctx,
		`INSERT INTO accounts (account_id, owner_type, owner_id, trading_cash, bank_cash)
		 VALUES ($1, 'agent', $1, 0, 0)`, accountID)
	if err != nil {
		t.Fatalf("seed account: %v", err)
	}
	tradeIDs := []string{
		fmt.Sprintf("trd-drn-e2e-a-%d", time.Now().UnixNano()),
		fmt.Sprintf("trd-drn-e2e-b-%d", time.Now().UnixNano()),
	}
	t.Cleanup(func() {
		for _, tradeID := range tradeIDs {
			_, _ = pool.Exec(ctx, `DELETE FROM trade_costs WHERE trade_id = $1`, tradeID)
			_, _ = pool.Exec(ctx, `DELETE FROM participant_trades WHERE trade_id = $1`, tradeID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM accounts WHERE account_id = $1`, accountID)
	})

	for _, tradeID := range tradeIDs {
		insert := baseTradeInsert(tradeID, accountID)
		if err := component.store.pushClosedTrade(ctx, insert); err != nil {
			t.Fatalf("push %s: %v", tradeID, err)
		}
	}

	if err := component.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait until the buffer is drained (poll — bounded).
	drained := waitForBufferDrained(t, client, cfg.KeyNamespace, 2*time.Second)
	component.Stop()

	if !drained {
		t.Fatal("drainer did not drain the buffer within 2s")
	}

	for _, tradeID := range tradeIDs {
		var count int
		err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM participant_trades WHERE trade_id = $1`, tradeID).Scan(&count)
		if err != nil {
			t.Fatalf("count %s: %v", tradeID, err)
		}
		if count != 1 {
			t.Errorf("participant_trades[%s]: got %d, want 1", tradeID, count)
		}
	}
}

// waitForBufferDrained polls LLEN on the buffer key until it hits zero or
// deadline. Returns true if drained within the timeout.
func waitForBufferDrained(t *testing.T, client *redis.Client, namespace string, timeout time.Duration) bool {
	t.Helper()
	bufferKey := bufKey(namespace)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		length, err := client.LLen(context.Background(), bufferKey).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			t.Fatalf("LLEN: %v", err)
		}
		if length == 0 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
