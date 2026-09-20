package cost

import "testing"

func TestApplyRate(t *testing.T) {
	cases := []struct {
		name     string
		value    int64
		rate     Rate
		expected int64
	}{
		{
			name:     "zero_value",
			value:    0,
			rate:     Rate{Num: 18, Den: 100},
			expected: 0,
		},
		{
			name:     "exact_multiple",
			value:    100,
			rate:     Rate{Num: 50, Den: 100},
			expected: 50,
		},
		{
			name:     "rounds_down",
			value:    100,
			rate:     Rate{Num: 1, Den: 3}, // 33.33... -> 33
			expected: 33,
		},
		{
			name:     "rounds_up",
			value:    200,
			rate:     Rate{Num: 1, Den: 3}, // 66.66... -> 67
			expected: 67,
		},
		{
			name:     "exact_half_rounds_up",
			value:    5,
			rate:     Rate{Num: 1, Den: 2}, // 2.5 -> 3 (half-up)
			expected: 3,
		},
		{
			name:     "just_below_half",
			value:    49,
			rate:     Rate{Num: 1, Den: 100}, // 0.49 -> 0
			expected: 0,
		},
		{
			name:     "just_at_half",
			value:    50,
			rate:     Rate{Num: 1, Den: 100}, // 0.50 -> 1 (half-up)
			expected: 1,
		},
		{
			name:     "just_above_half",
			value:    51,
			rate:     Rate{Num: 1, Den: 100}, // 0.51 -> 1
			expected: 1,
		},
		{
			name:     "large_value_exchange_shape",
			value:    10_000_000,
			rate:     Rate{Num: 297, Den: 10_000_000}, // exchange rate from Golden A
			expected: 297,
		},
		{
			name:     "large_value_stt_shape",
			value:    10_000_000,
			rate:     Rate{Num: 25, Den: 100_000}, // STT rate from Golden B
			expected: 2500,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyRate(tc.value, tc.rate)
			if got != tc.expected {
				t.Fatalf("applyRate(%d, {%d, %d}) = %d, want %d",
					tc.value, tc.rate.Num, tc.rate.Den, got, tc.expected)
			}
		})
	}
}

func TestSum(t *testing.T) {
	cases := []struct {
		name     string
		lines    map[string]int64
		expected int64
	}{
		{
			name:     "empty",
			lines:    map[string]int64{},
			expected: 0,
		},
		{
			name:     "single",
			lines:    map[string]int64{"a": 5},
			expected: 5,
		},
		{
			name:     "many",
			lines:    map[string]int64{"a": 5, "b": 10, "c": 15},
			expected: 30,
		},
		{
			name:     "includes_zero",
			lines:    map[string]int64{"a": 0, "b": 7},
			expected: 7,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sum(tc.lines)
			if got != tc.expected {
				t.Fatalf("sum(%v) = %d, want %d", tc.lines, got, tc.expected)
			}
		})
	}
}
