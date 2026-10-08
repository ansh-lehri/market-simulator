package funnel

import (
	"fmt"
	"math/rand"
	"testing"

	"pgregory.net/rapid"
)

// The three properties Point 1 (BROKER_TIER0) hangs on hashAccount:
//
//   P-hash-1  determinism    same (accountID, shardCount) → same shard index every call
//   P-hash-2  bounds         result is always in [0, shardCount); no panic on any input length
//   P-hash-3  distribution   over a large uniform sample the shards fill roughly evenly
//
// (P1 and P2 are property tests; P3 is a single deterministic run over a
// fixed-seed 100k-sample so failures are reproducible and shrinkable
// without a huge rapid state.)
//
// Also covered here:
//   - the router's "single shard per account" spine invariant (BROKER_TIER0
//     spine item 2) — proven at the hash level: any two calls for the same
//     accountID on the same shardCount must produce the same shard index,
//     regardless of intervening state or other accountIDs.
//   - a boundary sweep of degenerate inputs (empty string, single byte,
//     256-byte string) that don't fit rapid's generators cleanly.

// P-hash-1 — determinism across repeated calls.
//
// rapid draws a random accountID and a random shardCount, then invokes
// hashAccount several times and asserts every call returns the same
// index. A non-deterministic hash would split an account's state across
// shards.
func TestProperty_HashAccount_DeterministicWithinRun(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		accountID := rapid.String().Draw(rt, "accountID")
		shardCount := rapid.IntRange(1, 128).Draw(rt, "shardCount")

		firstIndex := hashAccount(accountID, shardCount)
		for callNumber := 0; callNumber < 10; callNumber++ {
			nextIndex := hashAccount(accountID, shardCount)
			if nextIndex != firstIndex {
				rt.Fatalf("hashAccount(%q, %d) not deterministic: call 0 → %d, call %d → %d",
					accountID, shardCount, firstIndex, callNumber+1, nextIndex)
			}
		}
	})
}

// P-hash-2 — result always in [0, shardCount).
//
// The router indexes b.shards[k] directly; any out-of-range index would
// panic on send. Guards against a modulo mistake or a signed-integer
// wraparound on very large hashes.
func TestProperty_HashAccount_ResultInRange(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		accountID := rapid.String().Draw(rt, "accountID")
		shardCount := rapid.IntRange(1, 1024).Draw(rt, "shardCount")

		shardIndex := hashAccount(accountID, shardCount)
		if shardIndex < 0 || shardIndex >= shardCount {
			rt.Fatalf("hashAccount(%q, %d) = %d, out of [0, %d)",
				accountID, shardCount, shardIndex, shardCount)
		}
	})
}

// Boundary sweep — degenerate inputs the property tests may not hit
// (empty accountID, single-byte accountID, 256-byte accountID). All
// three are legal per the algorithm; the router must handle them.
func TestHashAccount_DegenerateInputs(t *testing.T) {
	longAccountID := make([]byte, 256)
	for byteIndex := range longAccountID {
		longAccountID[byteIndex] = byte(byteIndex)
	}

	cases := []struct {
		name      string
		accountID AccountID
	}{
		{"empty", ""},
		{"singleByte", "x"},
		{"twoBytes", "ab"},
		{"longUnicode", "αβγδεζηθικλμνξοπρστυφχψω"},
		{"256bytes", string(longAccountID)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for shardCount := 1; shardCount <= 32; shardCount++ {
				shardIndex := hashAccount(tc.accountID, shardCount)
				if shardIndex < 0 || shardIndex >= shardCount {
					t.Fatalf("hashAccount(%q, %d) = %d, out of [0, %d)",
						tc.accountID, shardCount, shardIndex, shardCount)
				}
			}
		})
	}
}

// P-hash-3 — distribution.
//
// Draws 100 000 pseudo-random accountIDs from a fixed seed (so a
// failure is reproducible) and hashes them into shard counts 2, 4, 8,
// 16, 32. Every shard's population must land within [expected*0.5,
// expected*2.0]. The 0.5–2.0 window is deliberately loose: FNV-1a is
// not a cryptographic hash, but for uniformly-random opaque strings it
// distributes far tighter than 2×; a shard outside 2× is a real hash
// regression.
//
// This test is not a property test because rapid would either run it
// many times with too-small samples (weakening the assertion) or one
// large sample (undermining shrinkability). One reproducible run
// balances both concerns.
func TestHashAccount_DistributionOverLargeSample(t *testing.T) {
	const (
		sampleSize    = 100_000
		accountIDLen  = 24
		distSeed      = int64(0xC0FFEE_BEAD)
		lowerMultiple = 0.5
		upperMultiple = 2.0
	)

	source := rand.New(rand.NewSource(distSeed))
	accountIDs := make([]AccountID, sampleSize)
	for sampleIndex := range accountIDs {
		accountBytes := make([]byte, accountIDLen)
		for byteIndex := range accountBytes {
			accountBytes[byteIndex] = byte(source.Intn(256))
		}
		accountIDs[sampleIndex] = AccountID(accountBytes)
	}

	for _, shardCount := range []int{2, 4, 8, 16, 32} {
		t.Run(fmt.Sprintf("shardCount=%d", shardCount), func(t *testing.T) {
			perShardCount := make([]int, shardCount)
			for _, accountID := range accountIDs {
				perShardCount[hashAccount(accountID, shardCount)]++
			}

			expectedPerShard := float64(sampleSize) / float64(shardCount)
			lowerBound := int(expectedPerShard * lowerMultiple)
			upperBound := int(expectedPerShard * upperMultiple)

			for shardIndex, actualCount := range perShardCount {
				if actualCount < lowerBound || actualCount > upperBound {
					t.Fatalf("shard %d received %d ids; expected %.0f, allowed [%d, %d]",
						shardIndex, actualCount, expectedPerShard, lowerBound, upperBound)
				}
			}
		})
	}
}

// Spine item 2 — an account, once hashed, always lands on the same
// shard. Distinct from P-hash-1 (which repeats one accountID); this
// interleaves many different accountIDs to catch any implicit-state
// bug (a package-level counter, an accidental time.Now(), etc.).
func TestHashAccount_AccountAlwaysSameShard(t *testing.T) {
	const shardCount = 8

	accountIDs := []AccountID{
		"acct-alpha", "acct-beta", "acct-gamma", "acct-delta",
		"acct-epsilon", "acct-zeta", "acct-eta", "acct-theta",
	}

	firstAssignments := make(map[AccountID]int, len(accountIDs))
	for _, accountID := range accountIDs {
		firstAssignments[accountID] = hashAccount(accountID, shardCount)
	}

	for interleaveRound := 0; interleaveRound < 100; interleaveRound++ {
		for _, accountID := range accountIDs {
			laterAssignment := hashAccount(accountID, shardCount)
			if laterAssignment != firstAssignments[accountID] {
				t.Fatalf("accountID %q moved shards: first=%d, round %d=%d",
					accountID, firstAssignments[accountID], interleaveRound, laterAssignment)
			}
		}
	}
}

// hashAccount is on the hot path (every command); ensure it does not
// allocate. A regression here (someone reaches for hash/fnv's Writer or
// a []byte conversion of the string) would put pressure on the router.
func TestHashAccount_ZeroAlloc(t *testing.T) {
	const shardCount = 16
	accountID := AccountID("acct-alloc-probe-xxxxxxxxxxx")

	allocsPerCall := testing.AllocsPerRun(1000, func() {
		_ = hashAccount(accountID, shardCount)
	})
	if allocsPerCall != 0 {
		t.Fatalf("hashAccount allocated %.2f times/call; want 0", allocsPerCall)
	}
}

// Smoke — newShard yields a shard whose inbox has the requested depth.
// Point-1 scope only: no run(), no dispatch, just the constructor.
func TestNewShard_InboxDepth(t *testing.T) {
	cases := []struct {
		name       string
		inboxDepth int
	}{
		{"unbuffered", 0},
		{"smallBuffer", 4},
		{"largerBuffer", 64},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builtShard := newShard("B-newShard", tc.inboxDepth, nil, nil)
			if builtShard == nil {
				t.Fatalf("newShard(%d, nil) returned nil", tc.inboxDepth)
			}
			if gotCap := cap(builtShard.inbox); gotCap != tc.inboxDepth {
				t.Fatalf("newShard(%d, nil) inbox cap = %d, want %d",
					tc.inboxDepth, gotCap, tc.inboxDepth)
			}
			if builtShard.inbox == nil {
				t.Fatalf("newShard(%d, nil) inbox is nil", tc.inboxDepth)
			}
		})
	}
}
