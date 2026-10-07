package order

import (
	"errors"
	"testing"
	"time"
)

func TestConfig_Validate_HappyPath(t *testing.T) {
	cfg := Config{OrderTimeout: 500 * time.Millisecond}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate on well-formed Config = %v, want nil", err)
	}
}

func TestConfig_Validate_Rejects(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
	}{
		{"zero", 0},
		{"negative", -1 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Config{OrderTimeout: tc.timeout}.Validate()
			if err == nil {
				t.Fatalf("Validate(%v) = nil, want error", tc.timeout)
			}
			if !errors.Is(err, ErrBadConfig) {
				t.Errorf("Validate(%v) err does not wrap ErrBadConfig: %v", tc.timeout, err)
			}
		})
	}
}
