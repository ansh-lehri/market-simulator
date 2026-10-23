package lifecycle

import (
	"errors"
	"testing"
)

func TestParseOwnerType(t *testing.T) {
	cases := []struct {
		name      string
		wireValue string
		want      OwnerType
		wantErr   bool
	}{
		{"agent", "agent", OwnerAgent, false},
		{"human", "human", OwnerHuman, false},
		{"empty string is not a default here", "", OwnerAgent, true},
		{"unrecognized value", "robot", OwnerAgent, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseOwnerType(tc.wireValue)
			if tc.wantErr {
				if !errors.Is(err, ErrBadOwnerType) {
					t.Fatalf("err = %v, want ErrBadOwnerType", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOwnerType(%q): %v", tc.wireValue, err)
			}
			if got != tc.want {
				t.Errorf("ParseOwnerType(%q) = %v, want %v", tc.wireValue, got, tc.want)
			}
		})
	}
}
