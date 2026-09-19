package wire

import (
	"testing"

	"simulator/contract"
)

// TestHashInstrument_Deterministic asserts the router's stability requirement
// (EXCHANGE_10 §5): an instrument always hashes to the same value, so it
// always lands on the same shard.
func TestHashInstrument_Deterministic(t *testing.T) {
	ids := []contract.InstrumentID{"ACME", "BETA", "CTL", "DELTA", ""}
	first := make(map[contract.InstrumentID]uint32, len(ids))
	for _, id := range ids {
		first[id] = hashInstrument(id)
	}
	for _, id := range ids {
		if again := hashInstrument(id); again != first[id] {
			t.Fatalf("hashInstrument(%q) = %d, then %d — not deterministic", id, first[id], again)
		}
	}
}

// TestHashInstrument_NotConstant is a smoke check that different ids produce
// different hashes (FNV-32a is not collision-free, but a handful of short
// distinct ids should not collide in practice).
func TestHashInstrument_NotConstant(t *testing.T) {
	seen := map[uint32]contract.InstrumentID{}
	for _, id := range []contract.InstrumentID{"ACME", "BETA", "CTL", "DELTA"} {
		hashed := hashInstrument(id)
		if prior, dup := seen[hashed]; dup {
			t.Fatalf("unexpected FNV collision at %d: %q and %q", hashed, prior, id)
		}
		seen[hashed] = id
	}
}
