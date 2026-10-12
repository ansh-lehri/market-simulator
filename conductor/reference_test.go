package conductor

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/contract"
)

// Compile-time: *pgxpool.Pool is the production candleReader. The real
// seam check is at the Conductor construction site; this is the local
// mirror so pool-shape drift breaks here first.
var _ candleReader = (*pgxpool.Pool)(nil)

// fakeCandleRow carries one canned (close int64, err error) pair. Scan
// returns err verbatim when non-nil (including pgx.ErrNoRows for the
// no-row path); otherwise writes close into the first *int64 dest and
// returns nil.
type fakeCandleRow struct {
	closePrice int64
	scanErr    error
}

func (row fakeCandleRow) Scan(dest ...any) error {
	if row.scanErr != nil {
		return row.scanErr
	}
	if len(dest) != 1 {
		return errors.New("fakeCandleRow: want exactly one scan destination")
	}
	closeDest, ok := dest[0].(*int64)
	if !ok {
		return errors.New("fakeCandleRow: scan destination is not *int64")
	}
	*closeDest = row.closePrice
	return nil
}

// fakeCandleReader records the last (sql, args) pair and returns the
// configured row on QueryRow. The recording surface lets tests assert
// the reference lookup used the market tag (not the per-instance id)
// and the configured instrument id.
type fakeCandleReader struct {
	returnRow pgx.Row

	lastSQL  string
	lastArgs []any
	calls    int
}

func (reader *fakeCandleReader) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	reader.calls++
	reader.lastSQL = sql
	reader.lastArgs = args
	return reader.returnRow
}

func instrumentForReferenceTest() InstrumentConfig {
	return InstrumentConfig{
		ID:        contract.InstrumentID("RELIANCE"),
		TickSize:  contract.Ticks(5),
		BandBps:   1000,
		SeedPrice: contract.Ticks(250000), // 2500.00 in paise
	}
}

func TestReadReference_RowFound_ReturnsStoredClose(t *testing.T) {
	const storedClose int64 = 251575
	reader := &fakeCandleReader{returnRow: fakeCandleRow{closePrice: storedClose}}

	reference, err := readReference(context.Background(), reader, "IN", instrumentForReferenceTest())
	if err != nil {
		t.Fatalf("readReference: %v", err)
	}
	if reference != contract.Ticks(storedClose) {
		t.Errorf("reference = %d, want %d (the stored close)", reference, storedClose)
	}
	// Keyed on market tag + instrument id, in that order.
	if reader.calls != 1 {
		t.Errorf("QueryRow calls = %d, want 1", reader.calls)
	}
	if len(reader.lastArgs) != 2 {
		t.Fatalf("QueryRow args len = %d, want 2 (market, instrument)", len(reader.lastArgs))
	}
	marketArg, _ := reader.lastArgs[0].(string)
	instrumentArg, _ := reader.lastArgs[1].(string)
	if marketArg != "IN" {
		t.Errorf("market arg = %q, want %q", marketArg, "IN")
	}
	if instrumentArg != "RELIANCE" {
		t.Errorf("instrument arg = %q, want %q", instrumentArg, "RELIANCE")
	}
}

func TestReadReference_NoRow_FallsBackToSeedPrice(t *testing.T) {
	reader := &fakeCandleReader{returnRow: fakeCandleRow{scanErr: pgx.ErrNoRows}}
	instrument := instrumentForReferenceTest()

	reference, err := readReference(context.Background(), reader, "IN", instrument)
	if err != nil {
		t.Fatalf("readReference on no-row returned error (expected seed-price fallback): %v", err)
	}
	if reference != instrument.SeedPrice {
		t.Errorf("reference = %d, want seed price %d", reference, instrument.SeedPrice)
	}
}

func TestReadReference_GenericDBError_Bubbles(t *testing.T) {
	driverFailure := errors.New("connection reset by peer")
	reader := &fakeCandleReader{returnRow: fakeCandleRow{scanErr: driverFailure}}

	reference, err := readReference(context.Background(), reader, "IN", instrumentForReferenceTest())
	if err == nil {
		t.Fatal("readReference: expected error on driver failure, got nil")
	}
	if !errors.Is(err, driverFailure) {
		t.Errorf("readReference error does not wrap driver failure: %v", err)
	}
	if reference != 0 {
		t.Errorf("reference on error = %d, want 0 (zero value — caller aborts)", reference)
	}
}

func TestReadReference_KeyedOnMarketTagNotInstanceID(t *testing.T) {
	// Spec-locked behavior: the lookup uses the broker-side market tag,
	// NOT the per-instance market id "<tag>:<date>". A regression that
	// swapped these would silently stop finding references across days.
	reader := &fakeCandleReader{returnRow: fakeCandleRow{closePrice: 99999}}

	_, err := readReference(context.Background(), reader, MarketTag("US"), instrumentForReferenceTest())
	if err != nil {
		t.Fatalf("readReference: %v", err)
	}
	marketArg, _ := reader.lastArgs[0].(string)
	if marketArg != "US" {
		t.Errorf("market arg = %q, want plain tag %q (no date suffix)", marketArg, "US")
	}
	// Belt-and-braces: the argument must not look like an instance id
	// (no colon between tag and date).
	for _, char := range marketArg {
		if char == ':' {
			t.Errorf("market arg %q contains ':' — reference lookup must key on bare tag", marketArg)
		}
	}
}

func TestReadReference_ContextCancellation_Bubbles(t *testing.T) {
	// A ctx-driven error from the reader should bubble up unchanged. We
	// simulate this via scanErr; the real pool's Scan returns the ctx
	// error here.
	reader := &fakeCandleReader{returnRow: fakeCandleRow{scanErr: context.Canceled}}

	_, err := readReference(context.Background(), reader, "IN", instrumentForReferenceTest())
	if err == nil {
		t.Fatal("readReference: expected error on context.Canceled from Scan")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("readReference error does not wrap context.Canceled: %v", err)
	}
}
