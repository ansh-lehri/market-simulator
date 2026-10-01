package india

import "testing"

// TestColumnValues pins every India trade_costs column name to its exact
// DDL spelling (migrations/0003_create_trade_costs.sql). This is the
// drift gate — if someone edits a column name here, the test fails
// loudly instead of a column silently differing from the migration.
func TestColumnValues(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{ColumnBrokerage, "brokerage"},
		{ColumnSTT, "stt"},
		{ColumnExchange, "exchange"},
		{ColumnSEBI, "sebi"},
		{ColumnStamp, "stamp"},
		{ColumnGST, "gst"},
		{ColumnAutosquareFee, "autosquare_fee"},
		{ColumnTotal, "total"},
	}
	for _, testCase := range cases {
		if testCase.got != testCase.want {
			t.Errorf("column drift: got %q, want %q", testCase.got, testCase.want)
		}
	}
}

// TestLineValues pins the India CostBreakdown line names emitted by
// economy/cost (5.3 India stack). The India adapter's allowlist uses
// these keys verbatim; a drift here would route a line into the "unknown"
// branch at drain time.
func TestLineValues(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{LineSTT, "stt"},
		{LineExchange, "exchange"},
		{LineSEBI, "sebi"},
		{LineStamp, "stamp"},
		{LineGST, "gst"},
		{LineAutosquareFee, "autosquare_fee"},
	}
	for _, testCase := range cases {
		if testCase.got != testCase.want {
			t.Errorf("line drift: got %q, want %q", testCase.got, testCase.want)
		}
	}
}

// TestTradeCostsTable pins the India trade_costs table name
// (migrations/0003_create_trade_costs.sql).
func TestTradeCostsTable(t *testing.T) {
	if TradeCostsTable != "trade_costs" {
		t.Errorf("table drift: got %q, want %q", TradeCostsTable, "trade_costs")
	}
}
