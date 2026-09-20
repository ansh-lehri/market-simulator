package account

import (
	"reflect"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// -----------------------------------------------------------------------------
// PROPERTY 1 — Cash conservation across pure MoveCash sequences (§0.6 #1).
//
// Fuzzes: a random starting Cash (each bucket in [0, 10^9]) and a sequence of
// up to 50 MoveCash calls with uniformly-drawn (from, to, amount). Amounts
// span both non-positive (rejected as ErrNonPositiveAmount) and positive.
// Illegal edges and insufficient sources are drawn too; they must return an
// error AND leave state untouched.
//
// After every ACCEPTED call: sum-of-4-buckets must equal the starting sum.
// After every REJECTED call: state must DeepEqual the pre-call snapshot.
// -----------------------------------------------------------------------------

func TestProperty_MoveCashPreservesConservation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cash := Cash{
			Available: rapid.Int64Range(0, 1_000_000_000).Draw(rt, "startAvailable"),
			Reserved:  rapid.Int64Range(0, 1_000_000_000).Draw(rt, "startReserved"),
			Settled:   rapid.Int64Range(0, 1_000_000_000).Draw(rt, "startSettled"),
			Bank:      rapid.Int64Range(0, 1_000_000_000).Draw(rt, "startBank"),
		}
		initialSum := cash.Available + cash.Reserved + cash.Settled + cash.Bank

		opCount := rapid.IntRange(0, 100).Draw(rt, "opCount")
		for opIndex := 0; opIndex < opCount; opIndex++ {
			from := Bucket(rapid.IntRange(0, 3).Draw(rt, "from"))
			to := Bucket(rapid.IntRange(0, 3).Draw(rt, "to"))
			amount := rapid.Int64Range(-1000, 1_000_000).Draw(rt, "amount")

			snapshot := cash
			err := cash.MoveCash(from, to, amount)

			if err != nil {
				if cash != snapshot {
					rt.Fatalf("state mutated on error (op %d): from=%s to=%s amount=%d err=%v\n before=%+v\n  after=%+v",
						opIndex, from, to, amount, err, snapshot, cash)
				}
				continue
			}
			currentSum := cash.Available + cash.Reserved + cash.Settled + cash.Bank
			if currentSum != initialSum {
				rt.Fatalf("§0.6 cash conservation broken after op %d: initial=%d current=%d state=%+v",
					opIndex, initialSum, currentSum, cash)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// PROPERTY 2 — Cash algebraic delta holds when mint/burn are interleaved
// with transfers. CreditCash and DebitCash intentionally break pure
// conservation; the delta model (running credits - debits) must exactly
// account for every deviation.
// -----------------------------------------------------------------------------

func TestProperty_CashSumTracksMintBurnDelta(t *testing.T) {
	// Op kinds as sampled strings — clearer failure traces than raw ints.
	opKinds := []string{"move", "credit", "debit"}

	rapid.Check(t, func(rt *rapid.T) {
		cash := Cash{
			Available: rapid.Int64Range(0, 1_000_000).Draw(rt, "startAvailable"),
			Reserved:  rapid.Int64Range(0, 1_000_000).Draw(rt, "startReserved"),
			Settled:   rapid.Int64Range(0, 1_000_000).Draw(rt, "startSettled"),
			Bank:      rapid.Int64Range(0, 1_000_000).Draw(rt, "startBank"),
		}
		initialSum := cash.Available + cash.Reserved + cash.Settled + cash.Bank
		var runningDelta int64

		opCount := rapid.IntRange(0, 50).Draw(rt, "opCount")
		for opIndex := 0; opIndex < opCount; opIndex++ {
			kind := rapid.SampledFrom(opKinds).Draw(rt, "opKind")
			amount := rapid.Int64Range(-100, 1_000).Draw(rt, "amount")
			snapshot := cash

			var err error
			var deltaContribution int64
			switch kind {
			case "move":
				from := Bucket(rapid.IntRange(0, 3).Draw(rt, "moveFrom"))
				to := Bucket(rapid.IntRange(0, 3).Draw(rt, "moveTo"))
				err = cash.MoveCash(from, to, amount)
				// deltaContribution stays 0 — transfers conserve.
			case "credit":
				to := Bucket(rapid.IntRange(0, 3).Draw(rt, "creditTo"))
				err = cash.CreditCash(to, amount)
				if err == nil {
					deltaContribution = +amount
				}
			case "debit":
				from := Bucket(rapid.IntRange(0, 3).Draw(rt, "debitFrom"))
				err = cash.DebitCash(from, amount)
				if err == nil {
					deltaContribution = -amount
				}
			}

			if err != nil {
				if cash != snapshot {
					rt.Fatalf("state mutated on error (op %d, kind=%s): amount=%d err=%v\n before=%+v\n  after=%+v",
						opIndex, kind, amount, err, snapshot, cash)
				}
				continue
			}

			runningDelta += deltaContribution
			currentSum := cash.Available + cash.Reserved + cash.Settled + cash.Bank
			expectedSum := initialSum + runningDelta
			if currentSum != expectedSum {
				rt.Fatalf("cash sum delta broken after op %d (kind=%s): initial=%d delta=%d expected=%d actual=%d state=%+v",
					opIndex, kind, initialSum, runningDelta, expectedSum, currentSum, cash)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// PROPERTY 3 — Position–lot coupling (§0.6 #2) across mixed sequences of
// CreditPosition / MovePosition / RemovePosition.
//
// Invariants re-checked after EVERY accepted call:
//  (a) Available + Reserved == Σ Lot.RemainingQty
//  (b) No lot has RemainingQty ≤ 0 (dropped lots must be spliced from the head)
//  (c) OpenLots stay in arrival order (EntryAt strictly ascending) — reversing
//      would be LIFO and break cost basis; RemovePosition consumes from head so
//      any remaining lots must preserve the original insertion order
//
// After every REJECTED call: state must DeepEqual the pre-call snapshot.
// -----------------------------------------------------------------------------

func TestProperty_PositionLotCouplingAcrossOps(t *testing.T) {
	opKinds := []string{"credit", "move", "remove"}

	rapid.Check(t, func(rt *rapid.T) {
		var position Position
		// Monotonic timestamp source — every credit gets a strictly greater
		// EntryAt so we can assert arrival order across surviving lots.
		var timeCursor int64 = 1_000

		opCount := rapid.IntRange(0, 60).Draw(rt, "opCount")
		for opIndex := 0; opIndex < opCount; opIndex++ {
			kind := rapid.SampledFrom(opKinds).Draw(rt, "opKind")
			snapshot := snapshotPosition(position)

			var err error
			switch kind {
			case "credit":
				units := rapid.Int64Range(-5, 100).Draw(rt, "creditUnits")
				price := rapid.Int64Range(-5, 1_000).Draw(rt, "entryPrice")
				timeCursor++
				err = position.CreditPosition(units, price, time.Unix(timeCursor, 0).UTC())
			case "move":
				from := PosBucket(rapid.IntRange(0, 1).Draw(rt, "moveFrom"))
				to := PosBucket(rapid.IntRange(0, 1).Draw(rt, "moveTo"))
				units := rapid.Int64Range(-5, 100).Draw(rt, "moveUnits")
				err = position.MovePosition(from, to, units)
			case "remove":
				units := rapid.Int64Range(-5, 100).Draw(rt, "removeUnits")
				_, err = position.RemovePosition(units)
			}

			if err != nil {
				if !reflect.DeepEqual(position, snapshot) {
					rt.Fatalf("state mutated on error (op %d, kind=%s): err=%v\n before=%+v\n  after=%+v",
						opIndex, kind, err, snapshot, position)
				}
				continue
			}

			// (a) coupling
			bucketTotal := position.Available + position.Reserved
			lotSum := sumRemaining(position.OpenLots)
			if bucketTotal != lotSum {
				rt.Fatalf("§0.6 coupling broken after op %d (kind=%s): A+R=%d Σlots=%d state=%+v",
					opIndex, kind, bucketTotal, lotSum, position)
			}
			// (b) no dead lots
			for lotIndex, lot := range position.OpenLots {
				if lot.RemainingQty <= 0 {
					rt.Fatalf("lot %d has non-positive RemainingQty=%d after op %d (kind=%s): state=%+v",
						lotIndex, lot.RemainingQty, opIndex, kind, position)
				}
			}
			// (c) arrival order — EntryAt strictly ascending across surviving lots
			for lotIndex := 1; lotIndex < len(position.OpenLots); lotIndex++ {
				prev := position.OpenLots[lotIndex-1].EntryAt
				curr := position.OpenLots[lotIndex].EntryAt
				if !curr.After(prev) {
					rt.Fatalf("arrival order broken at lot %d after op %d (kind=%s): prev=%s curr=%s state=%+v",
						lotIndex, opIndex, kind, prev, curr, position)
				}
			}
			// (d) bucket non-negativity for positions (only Cash.Available can go negative)
			if position.Available < 0 || position.Reserved < 0 {
				rt.Fatalf("negative position bucket after op %d (kind=%s): state=%+v", opIndex, kind, position)
			}
		}
	})
}
