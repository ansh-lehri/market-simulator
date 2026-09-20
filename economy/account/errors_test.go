package account

import (
	"errors"
	"testing"
)

// The sentinel errors are load-bearing across boundary logs and tests; pin
// their identity and message so a rename or accidental re-wrap surfaces here.
func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"non-positive amount", ErrNonPositiveAmount, "account: amount must be > 0"},
		{"non-positive units", ErrNonPositiveUnits, "account: units must be > 0"},
		{"bad price", ErrBadPrice, "account: entryPrice must be > 0"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.err.Error() != testCase.msg {
				t.Errorf("Error() = %q, want %q", testCase.err.Error(), testCase.msg)
			}
			if !errors.Is(testCase.err, testCase.err) {
				t.Errorf("errors.Is failed on sentinel identity")
			}
		})
	}
}

// Struct errors must render bucket names + numeric context so boundary logs
// carry enough detail to diagnose without re-fetching state.
func TestStructErrorMessages(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "ErrIllegalMove Settled->Reserved",
			err:  ErrIllegalMove{From: Settled, To: Reserved},
			want: "account: illegal cash move Settled→Reserved",
		},
		{
			name: "ErrIllegalMove Bank->Reserved",
			err:  ErrIllegalMove{From: Bank, To: Reserved},
			want: "account: illegal cash move Bank→Reserved",
		},
		{
			name: "ErrIllegalPosMove same bucket",
			err:  ErrIllegalPosMove{From: PosAvailable, To: PosAvailable},
			want: "account: illegal position move PosAvailable→PosAvailable",
		},
		{
			name: "ErrInsufficient carries bucket + have/want",
			err:  ErrInsufficient{Bucket: Available, Have: 100, Want: 250},
			want: "account: insufficient Available (have=100 want=250)",
		},
		{
			name: "ErrInsufficientPosition carries have/want",
			err:  ErrInsufficientPosition{Have: 40, Want: 60},
			want: "account: insufficient position (have=40 want=60)",
		},
		{
			name: "ErrInsufficientReserved carries have/want",
			err:  ErrInsufficientReserved{Have: 5, Want: 6},
			want: "account: insufficient reserved position (have=5 want=6)",
		},
		{
			name: "ErrIllegalCreditTarget names target",
			err:  ErrIllegalCreditTarget{To: Reserved},
			want: "account: illegal credit target Reserved (only Available)",
		},
		{
			name: "ErrIllegalBurnSource names source",
			err:  ErrIllegalBurnSource{From: Bank},
			want: "account: illegal burn source Bank (only Available)",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.err.Error(); got != testCase.want {
				t.Errorf("Error() = %q, want %q", got, testCase.want)
			}
		})
	}
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
