package persistence

import (
	"testing"
	"time"
)

func TestConfigWithDefaultsFillsZeros(t *testing.T) {
	filled := Config{}.withDefaults()

	if filled.DrainerTick != 100*time.Millisecond {
		t.Errorf("DrainerTick default: got %v, want 100ms", filled.DrainerTick)
	}
	if filled.DrainerBatch != 64 {
		t.Errorf("DrainerBatch default: got %d, want 64", filled.DrainerBatch)
	}
	if filled.TimeEncoding != TimeRFC3339 {
		t.Errorf("TimeEncoding default: got %d, want %d (TimeRFC3339)", filled.TimeEncoding, TimeRFC3339)
	}
	if filled.SideEncoding != SideString {
		t.Errorf("SideEncoding default: got %d, want %d (SideString)", filled.SideEncoding, SideString)
	}
	if filled.KeyNamespace != "" {
		t.Errorf("KeyNamespace default: got %q, want empty", filled.KeyNamespace)
	}
}

func TestConfigWithDefaultsPreservesOverrides(t *testing.T) {
	drainerProgress := int64(42)
	_ = drainerProgress // keep the local for readability alongside other tests

	original := Config{
		DrainerTick:  500 * time.Millisecond,
		DrainerBatch: 128,
		TimeEncoding: TimeUnixNano,
		SideEncoding: SideInt,
		KeyNamespace: "test_ns",
	}
	filled := original.withDefaults()

	if filled != original {
		t.Errorf("withDefaults mutated an already-set Config: got %+v, want %+v", filled, original)
	}
}

func TestConfigWithDefaultsRejectsNonPositiveTickAndBatch(t *testing.T) {
	// negative or zero → default. Zero is the natural zero-value case; negative
	// is a caller bug we quietly correct here (the config surface promises
	// "operational defaults on zero-values").
	cases := []struct {
		name     string
		tick     time.Duration
		batch    int
	}{
		{"zero", 0, 0},
		{"negative tick", -1 * time.Second, 32},
		{"negative batch", 200 * time.Millisecond, -5},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			filled := Config{DrainerTick: testCase.tick, DrainerBatch: testCase.batch}.withDefaults()
			if filled.DrainerTick <= 0 {
				subtest.Errorf("DrainerTick still non-positive: %v", filled.DrainerTick)
			}
			if filled.DrainerBatch <= 0 {
				subtest.Errorf("DrainerBatch still non-positive: %d", filled.DrainerBatch)
			}
		})
	}
}
