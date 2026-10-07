package order

import (
	"testing"

	"simulator/contract"
)

// -----------------------------------------------------------------------------
// mapping_test.go — table-driven coverage for the four pure mapping
// helpers that translate the wire-level enum strings into contract
// types. Each parser's happy paths (both branches) + at least one reject
// path + case-sensitivity + empty-string guard.
//
// The parsers are the ONLY tier at which an unknown wire-level enum
// turns into a 400 — the handlers trust `ok` here and do not re-check
// at the funnel seam. So the asserts here are literally the admission
// gate's spec.
// -----------------------------------------------------------------------------

func TestParseSide(t *testing.T) {
	cases := []struct {
		wire string
		want contract.Side
		ok   bool
	}{
		{"buy", contract.Buy, true},
		{"sell", contract.Sell, true},
		{"BUY", 0, false},         // case-sensitive on purpose
		{"long", 0, false},        // not an alias
		{"", 0, false},            // empty string
		{"buy ", 0, false},        // no trim
	}
	for _, tc := range cases {
		t.Run(tc.wire, func(t *testing.T) {
			got, ok := parseSide(tc.wire)
			if ok != tc.ok {
				t.Fatalf("parseSide(%q) ok = %v, want %v", tc.wire, ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("parseSide(%q) = %v, want %v", tc.wire, got, tc.want)
			}
		})
	}
}

func TestParseType(t *testing.T) {
	cases := []struct {
		wire string
		want contract.OrdType
		ok   bool
	}{
		{"limit", contract.Limit, true},
		{"market", contract.Market, true},
		{"stop", 0, false},        // stop is a different tier (not Phase 5)
		{"MARKET", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.wire, func(t *testing.T) {
			got, ok := parseType(tc.wire)
			if ok != tc.ok {
				t.Fatalf("parseType(%q) ok = %v, want %v", tc.wire, ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("parseType(%q) = %v, want %v", tc.wire, got, tc.want)
			}
		})
	}
}

func TestParseTIF(t *testing.T) {
	cases := []struct {
		wire string
		want contract.TIF
		ok   bool
	}{
		{"gtc", contract.GTC, true},
		{"ioc", contract.IOC, true},
		{"fok", contract.FOK, true}, // accepted on the wire even though downstream is deferred
		{"good_til_cancel", 0, false},
		{"GTC", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.wire, func(t *testing.T) {
			got, ok := parseTIF(tc.wire)
			if ok != tc.ok {
				t.Fatalf("parseTIF(%q) ok = %v, want %v", tc.wire, ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("parseTIF(%q) = %v, want %v", tc.wire, got, tc.want)
			}
		})
	}
}

func TestTypeForModify(t *testing.T) {
	if got := typeForModify(true); got != contract.Market {
		t.Errorf("typeForModify(true) = %v, want Market", got)
	}
	if got := typeForModify(false); got != contract.Limit {
		t.Errorf("typeForModify(false) = %v, want Limit", got)
	}
}

func TestTIFForModify(t *testing.T) {
	if got := tifForModify(true); got != contract.IOC {
		t.Errorf("tifForModify(true) = %v, want IOC (market never rests)", got)
	}
	if got := tifForModify(false); got != contract.GTC {
		t.Errorf("tifForModify(false) = %v, want GTC (limit rests)", got)
	}
}
