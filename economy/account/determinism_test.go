package account

import (
	"reflect"
	"testing"
	"time"

	"simulator/contract"
)

// -----------------------------------------------------------------------------
// Reproducibility gate (root constitution): every write hand and both reads
// are pure functions of (state, arguments) — no clock, no randomness, no
// external read. Replaying the same ordered call sequence from the same
// starting state must produce the exact same ledger.
//
// The scripted sequence below exercises every hand at least twice and every
// read once; mid-sequence reads are captured to prove reads are pure too.
// -----------------------------------------------------------------------------

// scriptedRun runs a fixed 24-op sequence against a fresh Account and returns
// (final Account, mid-sequence read snapshots). Every op is designed to
// succeed given the starting state — an unexpected error fails the test.
func scriptedRun(t *testing.T) (*Account, []int64) {
	t.Helper()
	account := &Account{
		ID:        "acct-1",
		Cash:      Cash{Available: 100_000, Reserved: 0, Settled: 0, Bank: 20_000},
		Positions: map[contract.InstrumentID]*Position{},
	}
	mustNoErr := func(step int, err error) {
		if err != nil {
			t.Fatalf("scripted op %d returned unexpected error: %v", step, err)
		}
	}

	var readSnapshots []int64
	capture := func() {
		readSnapshots = append(readSnapshots,
			account.ReadAvailableCash(),
			account.ReadAvailablePosition("ACME"),
			account.ReadAvailablePosition("MISSING"),
		)
	}

	// 1. Reserve cash for a buy.
	mustNoErr(1, account.Cash.MoveCash(Available, Reserved, 30_000))
	// 2. Convert (buy fill principal).
	mustNoErr(2, account.Cash.MoveCash(Reserved, Settled, 30_000))
	// 3. Instrument arrives — allocate position, credit units.
	position := &Position{}
	account.Positions["ACME"] = position
	mustNoErr(3, position.CreditPosition(50, 600, t1))
	// 4. Second buy fill at a different price → second lot.
	mustNoErr(4, position.CreditPosition(30, 610, t2))
	capture() // read after two credits

	// 5. Pay a fee (Option B may or may not drive negative — start is +40k so still positive).
	mustNoErr(5, account.Cash.DebitCash(Available, 25))
	// 6. Reserve units for a sell.
	mustNoErr(6, position.MovePosition(PosAvailable, PosReserved, 40))
	// 7. Release part of the reservation (order shrunk).
	mustNoErr(7, position.MovePosition(PosReserved, PosAvailable, 5))
	// 8. Sell fill delivers 35 units — FIFO consumes across lots.
	matched, err := position.RemovePosition(35)
	mustNoErr(8, err)
	if len(matched) == 0 {
		t.Fatalf("scripted op 8 produced empty matched slice")
	}
	// 9. Credit sell proceeds.
	mustNoErr(9, account.Cash.CreditCash(Available, 21_500))
	capture()

	// 10. Bank refill from proceeds.
	mustNoErr(10, account.Cash.MoveCash(Available, Bank, 5_000))
	// 11. Bank pull later.
	mustNoErr(11, account.Cash.MoveCash(Bank, Available, 2_500))
	// 12. Third credit — new lot.
	mustNoErr(12, position.CreditPosition(20, 620, t3))
	// 13. Reserve another sell.
	mustNoErr(13, position.MovePosition(PosAvailable, PosReserved, 60))
	// 14. Partial sell fill.
	_, err = position.RemovePosition(45)
	mustNoErr(14, err)
	// 15. Fee big enough to drive Available negative (proves Option B replay-stable).
	mustNoErr(15, account.Cash.DebitCash(Available, 200_000))
	capture()

	// 16-24: winding down.
	mustNoErr(16, position.MovePosition(PosReserved, PosAvailable, 15))
	mustNoErr(17, account.Cash.CreditCash(Available, 150_000))
	mustNoErr(18, account.Cash.MoveCash(Available, Reserved, 500))
	mustNoErr(19, account.Cash.MoveCash(Reserved, Available, 500))
	mustNoErr(20, position.CreditPosition(10, 630, time.Unix(4_000, 0).UTC()))
	mustNoErr(21, position.MovePosition(PosAvailable, PosReserved, 5))
	_, err = position.RemovePosition(5)
	mustNoErr(22, err)
	mustNoErr(23, account.Cash.DebitCash(Available, 12))
	mustNoErr(24, account.Cash.MoveCash(Available, Bank, 1_000))
	capture()

	return account, readSnapshots
}

func TestDeterminism_ReplayProducesIdenticalLedger(t *testing.T) {
	firstAccount, firstReads := scriptedRun(t)
	secondAccount, secondReads := scriptedRun(t)

	if !reflect.DeepEqual(firstAccount, secondAccount) {
		t.Errorf("determinism broken — final Account differed across replays:\n first: %+v\n  cash: %+v\n   pos: %+v\nsecond: %+v\n  cash: %+v\n   pos: %+v",
			firstAccount, firstAccount.Cash, firstAccount.Positions,
			secondAccount, secondAccount.Cash, secondAccount.Positions,
		)
	}
	if !reflect.DeepEqual(firstReads, secondReads) {
		t.Errorf("reads differed across replays: first=%v second=%v", firstReads, secondReads)
	}
}
