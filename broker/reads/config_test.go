package reads

import (
	"errors"
	"testing"
	"time"
)

// Config.Validate rejects a zero or negative ReadTimeout — a bringup-time
// programming error that would otherwise collapse every request into
// instant ctx.DeadlineExceeded. Positive values pass.
func TestConfig_Validate(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{"zero rejected", 0, true},
		{"negative rejected", -1 * time.Millisecond, true},
		{"one nanosecond accepted (positive is positive)", 1, false},
		{"typical 2s accepted", 2 * time.Second, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Config{ReadTimeout: tc.timeout}.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected nil, got %v", err)
			}
		})
	}
}

// Every Validate error wraps ErrBadConfig so callers dispatch on the
// category sentinel with a single errors.Is check, rather than typing
// errors.As against each future variant.
func TestConfig_Validate_WrapsErrBadConfig(t *testing.T) {
	err := Config{ReadTimeout: 0}.Validate()
	if err == nil {
		t.Fatalf("expected error on ReadTimeout=0, got nil")
	}
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("expected errors.Is(err, ErrBadConfig) to hold; got %v", err)
	}
}
