package circuit

import (
	"testing"

	"simulator/contract"
)

func TestNewCircuitBand_IntegerFloor(t *testing.T) {
	// delta = reference * bandBps / 10000, integer division floors — the
	// bounds must be exact ticks (EXCHANGE_15 §6 invariant 3).
	cases := []struct {
		name      string
		reference contract.Ticks
		bandBps   int
		wantLower contract.Ticks
		wantUpper contract.Ticks
	}{
		{"clean 10% at 10000", 10000, 1000, 9000, 11000},
		{"floor drops fractional delta", 10007, 1000, 9007, 11007}, // 10007*1000/10000 = 1000 (floors, drops the 7)
		{"small bandBps floors to zero delta", 100, 1, 100, 100},   // 100*1/10000 = 0
		{"zero bandBps yields point band", 5000, 0, 5000, 5000},
		{"large band 50%", 10000, 5000, 5000, 15000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			band := NewCircuitBand(tc.reference, tc.bandBps)
			lower, upper := band.Bounds()
			if lower != tc.wantLower || upper != tc.wantUpper {
				t.Fatalf("bounds = (%d,%d), want (%d,%d)", lower, upper, tc.wantLower, tc.wantUpper)
			}
		})
	}
}

func TestPriceAllowed_EdgesAndOutside(t *testing.T) {
	// Bounds are inclusive on both edges (EXCHANGE_15 §2). Off-by-one on
	// inclusivity is the classic mistake, so probe both edges explicitly.
	band := NewCircuitBand(10000, 1000) // lower=9000, upper=11000
	cases := []struct {
		name  string
		price contract.Ticks
		want  bool
	}{
		{"at lower (inclusive)", 9000, true},
		{"at upper (inclusive)", 11000, true},
		{"inside at reference", 10000, true},
		{"one below lower", 8999, false},
		{"one above upper", 11001, false},
		{"far below lower", 1, false},
		{"far above upper", 100000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := band.PriceAllowed(tc.price); got != tc.want {
				t.Fatalf("PriceAllowed(%d) = %v, want %v", tc.price, got, tc.want)
			}
		})
	}
}

func TestBoundsMatchesPriceAllowed(t *testing.T) {
	// EXCHANGE_15 §6 invariant 1: the edges Bounds returns are exactly the
	// edges PriceAllowed enforces. Probe across several bands so the check
	// is not a single-case accident.
	bands := []*CircuitBand{
		NewCircuitBand(10000, 1000),
		NewCircuitBand(10007, 1000),
		NewCircuitBand(500, 250),
		NewCircuitBand(1_000_000, 100),
		NewCircuitBand(5000, 0), // point band, lower==upper
	}
	for _, band := range bands {
		lower, upper := band.Bounds()
		if !band.PriceAllowed(lower) {
			t.Fatalf("PriceAllowed(lower=%d) = false, want true", lower)
		}
		if !band.PriceAllowed(upper) {
			t.Fatalf("PriceAllowed(upper=%d) = false, want true", upper)
		}
		if band.PriceAllowed(lower - 1) {
			t.Fatalf("PriceAllowed(lower-1=%d) = true, want false", lower-1)
		}
		if band.PriceAllowed(upper + 1) {
			t.Fatalf("PriceAllowed(upper+1=%d) = true, want false", upper+1)
		}
	}
}

func TestPerInstrumentIndependence(t *testing.T) {
	// EXCHANGE_15 §6 invariant 5: one band per instrument, each from its
	// own reference. Two bands with disjoint references stay disjoint.
	acme := NewCircuitBand(10000, 1000)  // 9000..11000
	widget := NewCircuitBand(500, 1000)  // 450..550

	acmeLower, acmeUpper := acme.Bounds()
	widgetLower, widgetUpper := widget.Bounds()

	if widget.PriceAllowed(acmeLower) || widget.PriceAllowed(acmeUpper) {
		t.Fatalf("ACME edges (%d,%d) unexpectedly inside WIDGET band (%d,%d)",
			acmeLower, acmeUpper, widgetLower, widgetUpper)
	}
	if acme.PriceAllowed(widgetLower) || acme.PriceAllowed(widgetUpper) {
		t.Fatalf("WIDGET edges (%d,%d) unexpectedly inside ACME band (%d,%d)",
			widgetLower, widgetUpper, acmeLower, acmeUpper)
	}
}

func TestNewCircuitBand_PanicsOnInvalidInputs(t *testing.T) {
	// A zero/negative reference or negative bandBps is a broken coupling
	// from the caller (Layer 7 / rulebook), not a business error — panic
	// per CLAUDE.md error policy.
	cases := []struct {
		name      string
		reference contract.Ticks
		bandBps   int
	}{
		{"zero reference", 0, 1000},
		{"negative reference", -1, 1000},
		{"negative bandBps", 10000, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for (reference=%d, bandBps=%d)", tc.reference, tc.bandBps)
				}
			}()
			NewCircuitBand(tc.reference, tc.bandBps)
		})
	}
}
