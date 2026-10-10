package marketdata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/connectivity/config"
	"simulator/connectivity/db"
)

// Integration tests here run against a live TEST database with migration
// 0004 (candles) applied in addition to 0001–0003. Mirrors the harness
// shape in broker/store/integration_helpers_test.go: TEST_DATABASE_URL
// envelope, per-test instrument prefix for isolation, cleanup via a
// prefix-filtered DELETE. If TEST_DATABASE_URL is unset the tests skip
// cleanly — the same contract the broker/store suite advertises.

const envTestDBURL = "TEST_DATABASE_URL"

// candlesBaseTime is a fixed anchor so test rows have deterministic
// ordering. UTC-anchored because the column is TIMESTAMPTZ and we want
// the stored instant to be unambiguous regardless of the test machine's
// clock. (The ingestor stamps the exchange's clock time; the store
// doesn't care about the location tag — only the instant.)
var candlesBaseTime = time.Date(2026, 3, 15, 9, 30, 0, 0, time.UTC)

// requirePostgresPool opens a live pool against TEST_DATABASE_URL or
// skips the test. The pool is closed on test cleanup. Mirrors the
// broker/store harness so a developer who has set up the test DB for
// one suite gets the other for free.
func requirePostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(envTestDBURL)
	if dsn == "" {
		t.Skipf("%s not set — integration test skipped", envTestDBURL)
	}
	pool, err := db.NewPostgres(context.Background(), config.PostgresConfig{
		DSN:         dsn,
		PingTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open test postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// testInstrumentPrefix returns a unique instrument-id prefix for this
// test. Candles has no account_id FK — isolation is on (market,
// instrument), and we vary instrument since market is useful to pin
// ('IN'). Test name + nanoseconds → distinct across parallel + repeat runs.
func testInstrumentPrefix(t *testing.T) string {
	t.Helper()
	safe := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("test-%s-%d", safe, time.Now().UnixNano())
}

// cleanupCandlesWithInstrumentPrefix registers a DELETE on t.Cleanup
// filtered on the per-test prefix. Idempotent and bounded — only this
// test's rows disappear.
func cleanupCandlesWithInstrumentPrefix(t *testing.T, pool *pgxpool.Pool, instrumentPrefix string) {
	t.Helper()
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(),
			`DELETE FROM candles WHERE instrument LIKE $1 || '%'`, instrumentPrefix)
		if err != nil {
			t.Logf("cleanup candles for prefix %q: %v", instrumentPrefix, err)
		}
	})
}

// seedCandle inserts one candle via the Store under test. Using Insert
// (not raw SQL) means a bug in Insert surfaces at the first seed call
// rather than being hidden behind a sidestep.
func seedCandle(t *testing.T, store *CandleStore, candle Candle) {
	t.Helper()
	if err := store.Insert(context.Background(), candle); err != nil {
		t.Fatalf("seed candle (%s, %s, %s): %v",
			candle.Market, candle.Instrument, candle.Start, err)
	}
}

// mkCandle is a tiny builder with sensible non-asserted defaults; the
// test varies only the fields that drive its assertion.
func mkCandle(market, instrument string, offsetMinutes int) Candle {
	return Candle{
		Market:     market,
		Instrument: instrument,
		Start:      candlesBaseTime.Add(time.Duration(offsetMinutes) * time.Minute),
		Open:       10_000,
		High:       10_050,
		Low:        9_990,
		Close:      10_030,
		Volume:     100,
	}
}

// ---------- Insert ----------

// Re-delivering the same (market, instrument, timestamp) is a harmless
// no-op (ON CONFLICT DO NOTHING). Both calls return nil, and exactly
// one row survives in the store (DESIGN §1.2 / IMPL §1.4).
func TestCandleStore_Insert_Idempotent_ReDeliveredMinuteIsNoop(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"

	first := mkCandle("IN", instrument, 0)
	if err := store.Insert(context.Background(), first); err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	// Second insert of the SAME key with different payload: conflict should
	// silently drop the write (the stored row keeps the first payload).
	redelivery := first
	redelivery.Close = 99_999 // would be detectable if the row were overwritten
	if err := store.Insert(context.Background(), redelivery); err != nil {
		t.Fatalf("re-delivered Insert should be a no-op, got: %v", err)
	}

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(5*time.Minute), nil, 10)
	if err != nil {
		t.Fatalf("RangeRead1m after re-delivery: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count: got %d, want 1", len(rows))
	}
	if rows[0].Close != first.Close {
		t.Errorf("stored Close mutated by re-delivery: got %d, want %d (ON CONFLICT DO NOTHING should keep the first)",
			rows[0].Close, first.Close)
	}
}

// A pool-level driver failure (closed pool) maps to ErrLoadFailed, not
// a bare error. Callers switch on this sentinel.
func TestCandleStore_Insert_InfraFail_WrapsErrLoadFailed(t *testing.T) {
	pool := requirePostgresPool(t)
	// Close the pool out from under the store so Exec returns a driver err.
	pool.Close()
	store := NewCandleStore(pool)

	err := store.Insert(context.Background(), mkCandle("IN", "ACME", 0))
	if err == nil {
		t.Fatal("Insert against a closed pool returned nil (want ErrLoadFailed-wrapped)")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Errorf("err: got %v, want wraps ErrLoadFailed", err)
	}
}

// ---------- RangeRead1m ----------

// Half-open [from, to): seed 5 minutes, request [1, 4) → 3 rows at
// offsets 1,2,3 (minute 0 is excluded by from, minute 4 by to).
func TestCandleStore_RangeRead1m_HalfOpenWindow(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	for minute := 0; minute < 5; minute++ {
		seedCandle(t, store, mkCandle("IN", instrument, minute))
	}

	from := candlesBaseTime.Add(1 * time.Minute)
	to := candlesBaseTime.Add(4 * time.Minute)

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument, from, to, nil, 10)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("row count: got %d, want 3 (offsets 1,2,3 in [1,4))", len(rows))
	}
	for index, row := range rows {
		expectedStart := candlesBaseTime.Add(time.Duration(index+1) * time.Minute)
		if !row.Start.Equal(expectedStart) {
			t.Errorf("row[%d].Start: got %s, want %s (half-open window)", index, row.Start, expectedStart)
		}
	}
}

// Rows arrive oldest-first — ORDER BY timestamp ASC — regardless of
// INSERT order. Seed out-of-order on purpose; read must still be sorted.
func TestCandleStore_RangeRead1m_OldestFirstRegardlessOfInsertOrder(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	// Insert order: 2, 0, 1 — out of chronological order on purpose.
	for _, offset := range []int{2, 0, 1} {
		seedCandle(t, store, mkCandle("IN", instrument, offset))
	}

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 10)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("row count: got %d, want 3", len(rows))
	}
	for index, row := range rows {
		expectedStart := candlesBaseTime.Add(time.Duration(index) * time.Minute)
		if !row.Start.Equal(expectedStart) {
			t.Errorf("row[%d].Start: got %s, want %s (oldest-first)", index, row.Start, expectedStart)
		}
	}
}

// Keyset-forward: afterTS = cursor returns rows STRICTLY later than
// cursor. Seed 5 rows at 0..4; afterTS = row[1].Start → rows 2,3,4.
func TestCandleStore_RangeRead1m_KeysetAfterTSStrictlyExcludesCursor(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	for minute := 0; minute < 5; minute++ {
		seedCandle(t, store, mkCandle("IN", instrument, minute))
	}

	cursor := candlesBaseTime.Add(1 * time.Minute) // = row[1].Start
	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), &cursor, 10)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("row count: got %d, want 3 (rows strictly after cursor)", len(rows))
	}
	for index, row := range rows {
		expectedStart := candlesBaseTime.Add(time.Duration(index+2) * time.Minute)
		if !row.Start.Equal(expectedStart) {
			t.Errorf("row[%d].Start: got %s, want %s", index, row.Start, expectedStart)
		}
	}
}

// Limit is honored verbatim — the store never over-returns. Seed 5 rows,
// ask for 2 → return the 2 oldest. (The rollup engine owns the row-count
// cap; the store is a straight projection.)
func TestCandleStore_RangeRead1m_LimitHonoredAsHardCap(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	for minute := 0; minute < 5; minute++ {
		seedCandle(t, store, mkCandle("IN", instrument, minute))
	}

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 2)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count: got %d, want 2 (limit honored)", len(rows))
	}
}

// limit <= 0 is a client-side shortcut: no SQL issued, empty slice back.
// Mirrors broker/store's n<=0 rule on the trades reads.
func TestCandleStore_RangeRead1m_LimitZeroReturnsEmptySlice(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	// Seed a row to prove the shortcut IS a shortcut (if we actually queried,
	// we'd get this row back).
	seedCandle(t, store, mkCandle("IN", instrument, 0))

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 0)
	if err != nil {
		t.Fatalf("RangeRead1m with limit=0: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("len(rows): got %d, want 0 (limit<=0 shortcut)", len(rows))
	}
}

// Also exercise a negative limit — same shortcut path.
func TestCandleStore_RangeRead1m_LimitNegativeReturnsEmptySlice(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	seedCandle(t, store, mkCandle("IN", instrument, 0))

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, -5)
	if err != nil {
		t.Fatalf("RangeRead1m with limit=-5: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("len(rows): got %d, want 0 (limit<=0 shortcut)", len(rows))
	}
}

// Empty window / no rows is a LEGITIMATE result, not an error. The HTTP
// durable-read family relies on this split: ErrLoadFailed → 503 but
// empty → 200 with candles:[] (DESIGN §4.5).
func TestCandleStore_RangeRead1m_EmptyWindowIsNotError(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	// No seeds. Query a wide window.
	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(60*time.Minute), nil, 100)
	if err != nil {
		t.Fatalf("empty read returned error: %v", err)
	}
	if rows == nil {
		t.Error("rows: got nil, want empty slice (never nil on success)")
	}
	if len(rows) != 0 {
		t.Errorf("len(rows): got %d, want 0 (empty window)", len(rows))
	}
}

// End is derived (Start + bucketDuration), not stored. scanCandleRow
// sets it, so every returned row has it populated — a reader never has
// to compute it. If this regresses, the HTTP handler / rollup engine
// (later stages) would silently see End=zero.
func TestCandleStore_RangeRead1m_EndIsDerived(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	for minute := 0; minute < 3; minute++ {
		seedCandle(t, store, mkCandle("IN", instrument, minute))
	}

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 10)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("row count: got %d, want 3", len(rows))
	}
	for index, row := range rows {
		expectedEnd := row.Start.Add(bucketDuration)
		if !row.End.Equal(expectedEnd) {
			t.Errorf("row[%d].End: got %s, want %s (End = Start + 1m)", index, row.End, expectedEnd)
		}
	}
}

// A rival instrument's rows must not appear in another instrument's
// read. Isolation on (market, instrument) is what makes the per-test
// instrument-prefix cleanup sufficient.
func TestCandleStore_RangeRead1m_InstrumentFilterIsStrict(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	targetInstrument := instrumentPrefix + "-ACME"
	otherInstrument := instrumentPrefix + "-BETA"
	seedCandle(t, store, mkCandle("IN", targetInstrument, 0))
	seedCandle(t, store, mkCandle("IN", otherInstrument, 0))

	rows, err := store.RangeRead1m(context.Background(), "IN", targetInstrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 10)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count: got %d, want 1 (strict instrument filter)", len(rows))
	}
	if rows[0].Instrument != targetInstrument {
		t.Errorf("instrument: got %q, want %q", rows[0].Instrument, targetInstrument)
	}
}

// Market filter is likewise strict — the schema's PK is (market,
// instrument, timestamp), so a same-instrument row under a different
// market must not leak.
func TestCandleStore_RangeRead1m_MarketFilterIsStrict(t *testing.T) {
	pool := requirePostgresPool(t)
	instrumentPrefix := testInstrumentPrefix(t)
	cleanupCandlesWithInstrumentPrefix(t, pool, instrumentPrefix)

	store := NewCandleStore(pool)
	instrument := instrumentPrefix + "-ACME"
	seedCandle(t, store, mkCandle("IN", instrument, 0))
	seedCandle(t, store, mkCandle("US", instrument, 0))

	rows, err := store.RangeRead1m(context.Background(), "IN", instrument,
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 10)
	if err != nil {
		t.Fatalf("RangeRead1m: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count: got %d, want 1 (strict market filter)", len(rows))
	}
	if rows[0].Market != "IN" {
		t.Errorf("market: got %q, want %q", rows[0].Market, "IN")
	}
}

// Same shape as the Insert infra-fail — a closed pool makes Query return
// a driver err that must wrap ErrLoadFailed.
func TestCandleStore_RangeRead1m_InfraFail_WrapsErrLoadFailed(t *testing.T) {
	pool := requirePostgresPool(t)
	pool.Close()
	store := NewCandleStore(pool)

	_, err := store.RangeRead1m(context.Background(), "IN", "ACME",
		candlesBaseTime, candlesBaseTime.Add(10*time.Minute), nil, 10)
	if err == nil {
		t.Fatal("RangeRead1m against a closed pool returned nil err (want ErrLoadFailed-wrapped)")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Errorf("err: got %v, want wraps ErrLoadFailed", err)
	}
}
