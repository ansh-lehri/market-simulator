package persistence

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"simulator/country/india"
)

func TestIndiaAdapterFlattenPopulatesEveryColumn(t *testing.T) {
	// Every India column appears in the returned map (schema §2.3), even for
	// lines the caller did not supply. Total is carried through.
	adapter := IndiaCostAdapter{}
	columns, err := adapter.FlattenCostColumns(nil, 543)
	if err != nil {
		t.Fatalf("FlattenCostColumns: %v", err)
	}

	expectedColumns := []string{
		india.ColumnBrokerage,
		india.ColumnSTT,
		india.ColumnExchange,
		india.ColumnSEBI,
		india.ColumnStamp,
		india.ColumnGST,
		india.ColumnAutosquareFee,
		india.ColumnTotal,
	}
	for _, columnName := range expectedColumns {
		if _, present := columns[columnName]; !present {
			t.Errorf("column %q missing from output", columnName)
		}
	}
	if columns[india.ColumnBrokerage] != 0 {
		t.Errorf("brokerage: got %d, want 0 (D2)", columns[india.ColumnBrokerage])
	}
	if columns[india.ColumnTotal] != 543 {
		t.Errorf("total: got %d, want 543", columns[india.ColumnTotal])
	}
}

func TestIndiaAdapterFlattenMapsEveryLineToItsColumn(t *testing.T) {
	// One row per allowlisted key, each in isolation, so the mapping is
	// unambiguous.
	cases := []struct {
		lineName   string
		columnName string
	}{
		{india.LineSTT, india.ColumnSTT},
		{india.LineExchange, india.ColumnExchange},
		{india.LineSEBI, india.ColumnSEBI},
		{india.LineStamp, india.ColumnStamp},
		{india.LineGST, india.ColumnGST},
		{india.LineAutosquareFee, india.ColumnAutosquareFee},
	}
	adapter := IndiaCostAdapter{}
	for _, testCase := range cases {
		t.Run(testCase.lineName, func(subtest *testing.T) {
			amount := int64(1234)
			columns, err := adapter.FlattenCostColumns(
				map[string]int64{testCase.lineName: amount},
				amount,
			)
			if err != nil {
				subtest.Fatalf("FlattenCostColumns: %v", err)
			}
			if got := columns[testCase.columnName]; got != amount {
				subtest.Errorf("column %q: got %d, want %d", testCase.columnName, got, amount)
			}
			// Every other cost column stays 0.
			for _, otherColumn := range []string{
				india.ColumnSTT, india.ColumnExchange, india.ColumnSEBI,
				india.ColumnStamp, india.ColumnGST, india.ColumnAutosquareFee,
			} {
				if otherColumn == testCase.columnName {
					continue
				}
				if columns[otherColumn] != 0 {
					subtest.Errorf("column %q leaked non-zero: %d", otherColumn, columns[otherColumn])
				}
			}
		})
	}
}

func TestIndiaAdapterFlattenFullStack(t *testing.T) {
	// A realistic full India stack — every line populated at once.
	lines := map[string]int64{
		india.LineSTT:           25,
		india.LineExchange:      3,
		india.LineSEBI:          1,
		india.LineStamp:         15,
		india.LineGST:           5,
		india.LineAutosquareFee: 5000,
	}
	total := int64(5049)
	adapter := IndiaCostAdapter{}
	columns, err := adapter.FlattenCostColumns(lines, total)
	if err != nil {
		t.Fatalf("FlattenCostColumns: %v", err)
	}
	if columns[india.ColumnSTT] != 25 ||
		columns[india.ColumnExchange] != 3 ||
		columns[india.ColumnSEBI] != 1 ||
		columns[india.ColumnStamp] != 15 ||
		columns[india.ColumnGST] != 5 ||
		columns[india.ColumnAutosquareFee] != 5000 {
		t.Errorf("full-stack mapping wrong: %+v", columns)
	}
	if columns[india.ColumnBrokerage] != 0 {
		t.Errorf("brokerage should stay 0, got %d", columns[india.ColumnBrokerage])
	}
	if columns[india.ColumnTotal] != total {
		t.Errorf("total: got %d, want %d", columns[india.ColumnTotal], total)
	}
}

func TestIndiaAdapterFlattenUnknownLineIsAnError(t *testing.T) {
	// D1: an unrecognized key is an error, not a silent drop. The seam layer
	// (Stage 3) treats this as a programming bug at enqueue time.
	adapter := IndiaCostAdapter{}
	columns, err := adapter.FlattenCostColumns(
		map[string]int64{"nse_fee": 100},
		100,
	)
	if !errors.Is(err, ErrUnknownCostLine) {
		t.Fatalf("expected ErrUnknownCostLine, got %v", err)
	}
	if columns != nil {
		t.Errorf("on error, columns should be nil, got %+v", columns)
	}
}

func TestIndiaAdapterFlattenUnknownAmongstKnownStillErrors(t *testing.T) {
	// Map iteration order is random in Go; the unknown key may be hit first,
	// middle, or last. Every case must surface as the error.
	adapter := IndiaCostAdapter{}
	_, err := adapter.FlattenCostColumns(
		map[string]int64{
			india.LineSTT:      50,
			india.LineExchange: 3,
			"unknown_key":      47,
		},
		100,
	)
	if !errors.Is(err, ErrUnknownCostLine) {
		t.Fatalf("expected ErrUnknownCostLine, got %v", err)
	}
}

func TestIndiaAdapterFlattenBrokerageInLinesIsUnknown(t *testing.T) {
	// realize.TradeCostRow.Lines never carries "brokerage" (order-level,
	// separate path — D2 + realize CLAUDE.md). If someone tries to smuggle
	// it in, the India adapter treats it as an unknown key, not a silent
	// pass-through. That's the safety valve.
	adapter := IndiaCostAdapter{}
	_, err := adapter.FlattenCostColumns(
		map[string]int64{"brokerage": 2000},
		2000,
	)
	if !errors.Is(err, ErrUnknownCostLine) {
		t.Fatalf("expected ErrUnknownCostLine (brokerage never in trade_costs Lines), got %v", err)
	}
}

// Compile-time assertion (belt + suspenders on top of the one in india.go):
// IndiaCostAdapter satisfies CountryCostAdapter.
var _ CountryCostAdapter = IndiaCostAdapter{}

// fakeCostRowExecutor records the last Exec call for unit-level assertions
// on InsertCostRow (no DB needed).
type fakeCostRowExecutor struct {
	gotSQL  string
	gotArgs []any
	err     error
}

func (f *fakeCostRowExecutor) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	f.gotSQL = sql
	f.gotArgs = arguments
	return pgconn.CommandTag{}, f.err
}

func TestIndiaAdapterInsertCostRowIssuesFixedSQL(t *testing.T) {
	// The India adapter's InsertCostRow writes a fixed 9-column INSERT
	// targeting the India trade_costs table. Args are (trade_id, brokerage,
	// stt, exchange, sebi, stamp, gst, autosquare_fee, total).
	fake := &fakeCostRowExecutor{}
	err := IndiaCostAdapter{}.InsertCostRow(context.Background(), fake, "trd-1",
		map[string]int64{india.LineSTT: 25, india.LineGST: 5}, 30)
	if err != nil {
		t.Fatalf("InsertCostRow: %v", err)
	}
	if fake.gotSQL == "" {
		t.Fatal("expected some SQL, got empty")
	}
	if len(fake.gotArgs) != 9 {
		t.Fatalf("args len: got %d, want 9", len(fake.gotArgs))
	}
	if fake.gotArgs[0] != "trd-1" {
		t.Errorf("arg 0 (trade_id): got %v, want trd-1", fake.gotArgs[0])
	}
	if fake.gotArgs[1].(int64) != 0 {
		t.Errorf("arg 1 (brokerage): got %v, want 0 (D2)", fake.gotArgs[1])
	}
	if fake.gotArgs[2].(int64) != 25 {
		t.Errorf("arg 2 (stt): got %v, want 25", fake.gotArgs[2])
	}
	if fake.gotArgs[6].(int64) != 5 {
		t.Errorf("arg 6 (gst): got %v, want 5", fake.gotArgs[6])
	}
	if fake.gotArgs[8].(int64) != 30 {
		t.Errorf("arg 8 (total): got %v, want 30", fake.gotArgs[8])
	}
}

func TestIndiaAdapterInsertCostRowUnknownKeyErrorsBeforeSQL(t *testing.T) {
	// D1: unknown key error must fire BEFORE any SQL is issued.
	fake := &fakeCostRowExecutor{}
	err := IndiaCostAdapter{}.InsertCostRow(context.Background(), fake, "trd-1",
		map[string]int64{"nse_fee": 100}, 100)
	if !errors.Is(err, ErrUnknownCostLine) {
		t.Fatalf("expected ErrUnknownCostLine, got %v", err)
	}
	if fake.gotSQL != "" {
		t.Errorf("SQL was issued despite unknown key: %s", fake.gotSQL)
	}
}
