package account

import (
	"reflect"
	"testing"

	"simulator/contract"
)

// -----------------------------------------------------------------------------
// ReadAvailableCash
// -----------------------------------------------------------------------------

// The raw signed value passes through — including negatives that Option B
// (DebitCash) produced. A clamp-to-zero here would silently mask fee-driven
// debt and let the reserve check erroneously admit a buy.
func TestReadAvailableCash_ReturnsRawSignedValue(t *testing.T) {
	tests := []struct {
		name  string
		cash  Cash
		want  int64
	}{
		{"positive Available", Cash{Available: 1_000, Reserved: 200, Settled: 100, Bank: 50}, 1_000},
		{"zero Available", Cash{Available: 0, Reserved: 500, Settled: 300, Bank: 100}, 0},
		{"negative Available (Option B)", Cash{Available: -50, Reserved: 0, Settled: 400, Bank: 200}, -50},
		{"deeply negative", Cash{Available: -1_000_000}, -1_000_000},
		{"large positive", Cash{Available: 9_000_000_000}, 9_000_000_000},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			account := &Account{Cash: testCase.cash}
			if got := account.ReadAvailableCash(); got != testCase.want {
				t.Errorf("ReadAvailableCash() = %d, want %d", got, testCase.want)
			}
		})
	}
}

// A read never mutates. A future edit that "helpfully" clamps or lazy-inits
// something would trip this.
func TestReadAvailableCash_DoesNotMutate(t *testing.T) {
	account := &Account{
		ID:   "acct-1",
		Cash: Cash{Available: -100, Reserved: 250, Settled: 800, Bank: 1_000},
	}
	before := *account
	_ = account.ReadAvailableCash()
	if !reflect.DeepEqual(*account, before) {
		t.Errorf("account mutated by read: got %+v, want %+v", *account, before)
	}
}

// A nil Account is an ordering bug (order at gate before session init wrote
// the working copy). Panics loud, per root constitution — not a recoverable
// error path.
func TestReadAvailableCash_PanicsOnNilAccount(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on nil Account, got none")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", recovered, recovered)
		}
		want := "ReadAvailableCash: working state not primed (order before session init)"
		if msg != want {
			t.Errorf("panic msg = %q, want %q", msg, want)
		}
	}()
	var nilAccount *Account
	_ = nilAccount.ReadAvailableCash()
}

// -----------------------------------------------------------------------------
// ReadAvailablePosition
// -----------------------------------------------------------------------------

func TestReadAvailablePosition_PresentInstrument(t *testing.T) {
	tests := []struct {
		name           string
		positionAvail  int64
	}{
		{"positive", 42},
		{"zero (holds but nothing sellable — all reserved)", 0},
		{"large", 1_000_000_000},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			account := &Account{
				Positions: map[contract.InstrumentID]*Position{
					"ACME": {Available: testCase.positionAvail, Reserved: 5},
				},
			}
			if got := account.ReadAvailablePosition("ACME"); got != testCase.positionAvail {
				t.Errorf("ReadAvailablePosition(ACME) = %d, want %d", got, testCase.positionAvail)
			}
		})
	}
}

// Missing key → 0, not an error. This is what makes "you can't sell what you
// don't hold" fall out naturally: the sell reserve compares 0 < sellQty and
// refuses without any special-casing.
func TestReadAvailablePosition_MissingKeyReturnsZero(t *testing.T) {
	tests := []struct {
		name    string
		account *Account
	}{
		{
			name: "instrument not held (other instruments present)",
			account: &Account{
				Positions: map[contract.InstrumentID]*Position{
					"ACME": {Available: 100},
				},
			},
		},
		{
			name:    "empty Positions map",
			account: &Account{Positions: map[contract.InstrumentID]*Position{}},
		},
		{
			name:    "nil Positions map (uninitialised)",
			account: &Account{}, // Positions is nil map — Go allows reads from nil map
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.account.ReadAvailablePosition("MISSING"); got != 0 {
				t.Errorf("ReadAvailablePosition(MISSING) = %d, want 0", got)
			}
		})
	}
}

// A read never mutates — including no lazy-initialisation of the Positions
// map on a miss (would break single-writer if the funnel ever handed the
// account out to a read-only goroutine).
func TestReadAvailablePosition_DoesNotMutate(t *testing.T) {
	tests := []struct {
		name    string
		account *Account
		lookup  contract.InstrumentID
	}{
		{
			name: "present instrument",
			account: &Account{
				Positions: map[contract.InstrumentID]*Position{
					"ACME": {Available: 100, Reserved: 20},
				},
			},
			lookup: "ACME",
		},
		{
			name:    "missing key on nil map — must not allocate",
			account: &Account{},
			lookup:  "MISSING",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			// Snapshot: nil-map identity matters, so compare via reflect.
			var beforePositions map[contract.InstrumentID]*Position
			if testCase.account.Positions != nil {
				beforePositions = make(map[contract.InstrumentID]*Position, len(testCase.account.Positions))
				for key, value := range testCase.account.Positions {
					snap := *value
					beforePositions[key] = &snap
				}
			}
			_ = testCase.account.ReadAvailablePosition(testCase.lookup)
			// Nil-map must stay nil (not allocated by the read).
			if beforePositions == nil && testCase.account.Positions != nil {
				t.Errorf("read allocated Positions map — expected nil to stay nil")
			}
			// Present entries must be byte-identical.
			if len(beforePositions) != len(testCase.account.Positions) {
				t.Errorf("Positions map size changed: got %d, want %d", len(testCase.account.Positions), len(beforePositions))
			}
			for key, wantPos := range beforePositions {
				gotPos, ok := testCase.account.Positions[key]
				if !ok {
					t.Errorf("key %q dropped from Positions", key)
					continue
				}
				if !reflect.DeepEqual(*gotPos, *wantPos) {
					t.Errorf("Positions[%q] mutated: got %+v, want %+v", key, *gotPos, *wantPos)
				}
			}
		})
	}
}

func TestReadAvailablePosition_PanicsOnNilAccount(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on nil Account, got none")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", recovered, recovered)
		}
		want := "ReadAvailablePosition: working state not primed (order before session init)"
		if msg != want {
			t.Errorf("panic msg = %q, want %q", msg, want)
		}
	}()
	var nilAccount *Account
	_ = nilAccount.ReadAvailablePosition("ACME")
}
