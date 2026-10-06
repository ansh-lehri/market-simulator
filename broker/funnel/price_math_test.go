package funnel

import (
	"testing"

	"simulator/economy/capstone"
)

// NewBroker copies MoneyConfig.SlippageBufferBps into every shard's local
// slippageBufferBps field — that's how the dial reaches the owner goroutine
// without a cross-shard config lookup at place time.
func TestNewBroker_PropagatesSlippageDialToShards(t *testing.T) {
	bundles := []*capstone.Bundle{nil, nil, nil}
	broker := NewBroker("B-slip", 1, bundles, nil, MoneyConfig{SlippageBufferBps: 25})
	for shardIndex, s := range broker.shards {
		if s.slippageBufferBps != 25 {
			t.Errorf("shard %d: slippageBufferBps = %d, want 25", shardIndex, s.slippageBufferBps)
		}
	}
}

// Zero-valued MoneyConfig threads a zero dial into every shard — the
// current default, since no market-data cache is wired yet.
func TestNewBroker_ZeroMoneyConfigLeavesShardDialZero(t *testing.T) {
	bundles := []*capstone.Bundle{nil}
	broker := NewBroker("B-slip-zero", 1, bundles, nil, MoneyConfig{})
	if broker.shards[0].slippageBufferBps != 0 {
		t.Errorf("default shard dial = %d, want 0", broker.shards[0].slippageBufferBps)
	}
}

// maxSpend is bestAsk*qty + buffer — identical to reserve.Order's internal
// requiredCash formula (economy/reserve/reserve.go line 60), which is the
// "reserved == MaxSpend by construction" invariant Phase-5 §1.5 pins.
func TestMaxSpend_Formula(t *testing.T) {
	cases := []struct {
		name    string
		bestAsk int64
		qty     int64
		buffer  int64
		want    int64
	}{
		{"typical market buy", 10_021, 50, 1_000, 10_021*50 + 1_000},
		{"zero buffer (Phase-5 default)", 10_021, 50, 0, 10_021 * 50},
		{"limit / sell path (all zero)", 0, 0, 0, 0},
		{"minimal non-zero", 1, 1, 0, 1},
		{"big numbers", 1_000_000, 1_000, 500, 1_000_000_500},
	}
	for _, tc := range cases {
		if got := maxSpend(tc.bestAsk, tc.qty, tc.buffer); got != tc.want {
			t.Errorf("%s: maxSpend(%d, %d, %d) = %d, want %d",
				tc.name, tc.bestAsk, tc.qty, tc.buffer, got, tc.want)
		}
	}
}

// slippageBuffer with bps=0 is the current production default (the dial
// stays zero until the market-data cache lands). The short-circuit
// returns 0 regardless of notional size — pinned so a future refactor
// cannot accidentally make zero-bps allocate or compute.
func TestSlippageBuffer_ZeroBpsIsAlwaysZero(t *testing.T) {
	cases := []struct {
		bestAsk int64
		qty     int64
	}{
		{10_021, 50},
		{1, 1},
		{0, 0},
		{1_000_000, 1_000},
	}
	for _, tc := range cases {
		if got := slippageBuffer(tc.bestAsk, tc.qty, 0); got != 0 {
			t.Errorf("slippageBuffer(%d, %d, 0) = %d, want 0 (zero-bps short-circuit)",
				tc.bestAsk, tc.qty, got)
		}
	}
}

// slippageBuffer with a positive bps = notional * bps / 10000, truncated.
// The table covers typical bps values (50 = 0.5%, 100 = 1%, 1 = 1bp) across
// a range of notionals, plus the truncation edge where the result rounds
// down (not half-up — intentional for the current stub; a half-up variant
// can be added if the real volatility-based pin needs it).
func TestSlippageBuffer_PositiveBpsFormula(t *testing.T) {
	cases := []struct {
		name    string
		bestAsk int64
		qty     int64
		bps     int64
		want    int64
	}{
		{"50 bps on 10_021*50 notional", 10_021, 50, 50, 10_021 * 50 * 50 / 10_000},
		{"100 bps (= 1%) on 10_000*100", 10_000, 100, 100, 10_000 * 100 * 100 / 10_000},
		{"1 bp (= 0.01%) on 10_000*100", 10_000, 100, 1, 100},
		{"truncation: 50*50*1 / 10000 == 0", 50, 50, 1, 0},
		{"big: 1_000_000 * 1_000 * 25 / 10000", 1_000_000, 1_000, 25, 2_500_000},
	}
	for _, tc := range cases {
		if got := slippageBuffer(tc.bestAsk, tc.qty, tc.bps); got != tc.want {
			t.Errorf("%s: slippageBuffer(%d, %d, %d) = %d, want %d",
				tc.name, tc.bestAsk, tc.qty, tc.bps, got, tc.want)
		}
	}
}
