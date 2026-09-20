package account

import (
	"errors"
	"testing"
)

// Category sentinels are the STABLE dispatch surface — pin their identity and
// message so a rename surfaces here.
func TestCategorySentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"invalid argument", ErrInvalidArgument, "account: invalid argument"},
		{"illegal operation", ErrIllegalOperation, "account: illegal operation"},
		{"insufficient funds", ErrInsufficientFunds, "account: insufficient funds"},
		{"insufficient units", ErrInsufficientUnits, "account: insufficient units"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.err.Error(); got != testCase.msg {
				t.Errorf("Error() = %q, want %q", got, testCase.msg)
			}
			if !errors.Is(testCase.err, testCase.err) {
				t.Errorf("errors.Is failed on category sentinel identity")
			}
		})
	}
}

// Value-shape sentinels retain their own identity AND chain to
// ErrInvalidArgument so callers can dispatch on either the specific sentinel
// or the category.
func TestValueShapeSentinelsChainToInvalidArgument(t *testing.T) {
	tests := []struct {
		name     string
		sentinel error
		wantMsg  string
	}{
		{"non-positive amount", ErrNonPositiveAmount, "account: invalid argument: amount must be > 0"},
		{"non-positive units", ErrNonPositiveUnits, "account: invalid argument: units must be > 0"},
		{"bad price", ErrBadPrice, "account: invalid argument: entryPrice must be > 0"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.sentinel.Error(); got != testCase.wantMsg {
				t.Errorf("Error() = %q, want %q", got, testCase.wantMsg)
			}
			// Identity: dispatching on the specific sentinel must still work.
			if !errors.Is(testCase.sentinel, testCase.sentinel) {
				t.Errorf("errors.Is failed on specific sentinel identity")
			}
			// Category: dispatching on ErrInvalidArgument must also work via %w chain.
			if !errors.Is(testCase.sentinel, ErrInvalidArgument) {
				t.Errorf("errors.Is(%q, ErrInvalidArgument) = false, want true", testCase.sentinel)
			}
			// Isolation: must NOT chain to unrelated categories.
			for _, unrelated := range []error{ErrIllegalOperation, ErrInsufficientFunds, ErrInsufficientUnits} {
				if errors.Is(testCase.sentinel, unrelated) {
					t.Errorf("errors.Is(%q, %q) = true, want false — must not chain to unrelated category", testCase.sentinel, unrelated)
				}
			}
		})
	}
}

// Struct errors: Error() message carries full context, Unwrap() points at the
// category sentinel, and errors.As recovers the full context struct.
func TestStructErrorsMessagesAndDispatch(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantMsg     string
		wantCat     error
		unrelated   []error // categories this error must NOT chain to
	}{
		{
			name:      "ErrIllegalMove Settled->Reserved",
			err:       ErrIllegalMove{From: Settled, To: Reserved},
			wantMsg:   "account: illegal cash move Settled→Reserved",
			wantCat:   ErrIllegalOperation,
			unrelated: []error{ErrInvalidArgument, ErrInsufficientFunds, ErrInsufficientUnits},
		},
		{
			name:      "ErrIllegalMove Bank->Reserved (bank-not-reservable rule)",
			err:       ErrIllegalMove{From: Bank, To: Reserved},
			wantMsg:   "account: illegal cash move Bank→Reserved",
			wantCat:   ErrIllegalOperation,
			unrelated: []error{ErrInvalidArgument, ErrInsufficientFunds, ErrInsufficientUnits},
		},
		{
			name:      "ErrIllegalPosMove same bucket",
			err:       ErrIllegalPosMove{From: PosAvailable, To: PosAvailable},
			wantMsg:   "account: illegal position move PosAvailable→PosAvailable",
			wantCat:   ErrIllegalOperation,
			unrelated: []error{ErrInvalidArgument, ErrInsufficientFunds, ErrInsufficientUnits},
		},
		{
			name:      "ErrIllegalCreditTarget Reserved",
			err:       ErrIllegalCreditTarget{To: Reserved},
			wantMsg:   "account: illegal credit target Reserved (only Available)",
			wantCat:   ErrIllegalOperation,
			unrelated: []error{ErrInvalidArgument, ErrInsufficientFunds, ErrInsufficientUnits},
		},
		{
			name:      "ErrIllegalBurnSource Bank",
			err:       ErrIllegalBurnSource{From: Bank},
			wantMsg:   "account: illegal burn source Bank (only Available)",
			wantCat:   ErrIllegalOperation,
			unrelated: []error{ErrInvalidArgument, ErrInsufficientFunds, ErrInsufficientUnits},
		},
		{
			name:      "ErrInsufficient Available",
			err:       ErrInsufficient{Bucket: Available, Have: 100, Want: 250},
			wantMsg:   "account: insufficient Available (have=100 want=250)",
			wantCat:   ErrInsufficientFunds,
			unrelated: []error{ErrInvalidArgument, ErrIllegalOperation, ErrInsufficientUnits},
		},
		{
			name:      "ErrInsufficient Reserved (release path)",
			err:       ErrInsufficient{Bucket: Reserved, Have: 5, Want: 10},
			wantMsg:   "account: insufficient Reserved (have=5 want=10)",
			wantCat:   ErrInsufficientFunds,
			unrelated: []error{ErrInvalidArgument, ErrIllegalOperation, ErrInsufficientUnits},
		},
		{
			name:      "ErrInsufficientPosition (double-commit guard)",
			err:       ErrInsufficientPosition{Have: 40, Want: 60},
			wantMsg:   "account: insufficient position (have=40 want=60)",
			wantCat:   ErrInsufficientUnits,
			unrelated: []error{ErrInvalidArgument, ErrIllegalOperation, ErrInsufficientFunds},
		},
		{
			name:      "ErrInsufficientReserved",
			err:       ErrInsufficientReserved{Have: 5, Want: 6},
			wantMsg:   "account: insufficient reserved position (have=5 want=6)",
			wantCat:   ErrInsufficientUnits,
			unrelated: []error{ErrInvalidArgument, ErrIllegalOperation, ErrInsufficientFunds},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.err.Error(); got != testCase.wantMsg {
				t.Errorf("Error() = %q, want %q", got, testCase.wantMsg)
			}
			if !errors.Is(testCase.err, testCase.wantCat) {
				t.Errorf("errors.Is(%q, %q) = false, want true", testCase.err, testCase.wantCat)
			}
			for _, unrelated := range testCase.unrelated {
				if errors.Is(testCase.err, unrelated) {
					t.Errorf("errors.Is(%q, %q) = true, want false — must not chain to unrelated category", testCase.err, unrelated)
				}
			}
		})
	}
}

// errors.As must recover the full concrete struct so boundary logs get the
// context fields (bucket names, have/want) without re-parsing the message.
func TestStructErrorsRecoverableViaAs(t *testing.T) {
	t.Run("ErrInsufficient", func(t *testing.T) {
		var original error = ErrInsufficient{Bucket: Available, Have: 100, Want: 250}
		var recovered ErrInsufficient
		if !errors.As(original, &recovered) {
			t.Fatalf("errors.As failed to recover ErrInsufficient")
		}
		if recovered.Bucket != Available || recovered.Have != 100 || recovered.Want != 250 {
			t.Errorf("recovered = %+v, want {Available 100 250}", recovered)
		}
	})
	t.Run("ErrIllegalMove", func(t *testing.T) {
		var original error = ErrIllegalMove{From: Bank, To: Reserved}
		var recovered ErrIllegalMove
		if !errors.As(original, &recovered) {
			t.Fatalf("errors.As failed to recover ErrIllegalMove")
		}
		if recovered.From != Bank || recovered.To != Reserved {
			t.Errorf("recovered = %+v, want {Bank Reserved}", recovered)
		}
	})
	t.Run("ErrInsufficientPosition", func(t *testing.T) {
		var original error = ErrInsufficientPosition{Have: 40, Want: 60}
		var recovered ErrInsufficientPosition
		if !errors.As(original, &recovered) {
			t.Fatalf("errors.As failed to recover ErrInsufficientPosition")
		}
		if recovered.Have != 40 || recovered.Want != 60 {
			t.Errorf("recovered = %+v, want {40 60}", recovered)
		}
	})
}

// Bucket / PosBucket String() feeds every error message and every boundary log
// — pin them so a reordering of the iota or a rename surfaces here.
func TestBucketString(t *testing.T) {
	tests := []struct {
		bucket Bucket
		want   string
	}{
		{Available, "Available"},
		{Reserved, "Reserved"},
		{Settled, "Settled"},
		{Bank, "Bank"},
		{Bucket(99), "Bucket(?)"},
	}
	for _, testCase := range tests {
		if got := testCase.bucket.String(); got != testCase.want {
			t.Errorf("Bucket(%d).String() = %q, want %q", testCase.bucket, got, testCase.want)
		}
	}
}

func TestPosBucketString(t *testing.T) {
	tests := []struct {
		bucket PosBucket
		want   string
	}{
		{PosAvailable, "PosAvailable"},
		{PosReserved, "PosReserved"},
		{PosBucket(99), "PosBucket(?)"},
	}
	for _, testCase := range tests {
		if got := testCase.bucket.String(); got != testCase.want {
			t.Errorf("PosBucket(%d).String() = %q, want %q", testCase.bucket, got, testCase.want)
		}
	}
}
