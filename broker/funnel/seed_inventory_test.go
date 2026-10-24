package funnel

// handleSeedInventory coverage (BROKER_SEED_INVENTORY §4/§8). Each case
// traces to the doc's three outcomes plus the position↔lot coupling
// invariant (economy/account §0.6) that CreditPosition must preserve:
//
//   SI1 — fresh instrument, live session        → Present:true, Available==Qty, one lot
//   SI2 — second mint on an already-held instr.  → Available accumulates, a SECOND FIFO lot appended
//   SI3 — no live session                        → Present:false (hard error, no durable fallback — SI-D1)
//   SI4 — seeded units are sellable: ReserveIfAvailable succeeds off the
//         seeded PosAvailable (doc §8's reserveSell trace, exercised for real)

import (
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/reserve"
)

func TestHandleSeedInventory_FreshInstrument_MintsOneLot(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{}, nil)

	result := harness.shard.handleSeedInventory(SeedInventory{
		AccountID:  harness.accountID,
		Instrument: goldenInstrument,
		Qty:        100,
		EntryPrice: 250,
	})

	if !result.Present {
		t.Fatalf("Present = false, want true (live session installed)")
	}
	if result.Err != nil {
		t.Fatalf("Err = %v, want nil", result.Err)
	}
	if result.Available != 100 {
		t.Fatalf("Available = %d, want 100", result.Available)
	}

	pos := harness.ledger.Get(harness.accountID).Positions[goldenInstrument]
	if pos == nil {
		t.Fatalf("Positions[%q] = nil, want a seeded Position", goldenInstrument)
	}
	if pos.Available != 100 {
		t.Errorf("pos.Available = %d, want 100", pos.Available)
	}
	if len(pos.OpenLots) != 1 {
		t.Fatalf("len(OpenLots) = %d, want 1", len(pos.OpenLots))
	}
	if got, want := pos.OpenLots[0].RemainingQty, int64(100); got != want {
		t.Errorf("OpenLots[0].RemainingQty = %d, want %d", got, want)
	}
	if got, want := pos.OpenLots[0].EntryPrice, int64(250); got != want {
		t.Errorf("OpenLots[0].EntryPrice = %d, want %d", got, want)
	}

	// §0.6 coupling: Available + Reserved == Σ RemainingQty.
	if coupled := pos.Available + pos.Reserved; coupled != sumRemainingForTest(pos.OpenLots) {
		t.Errorf("position-lot coupling broken: Available+Reserved=%d, ΣRemainingQty=%d",
			coupled, sumRemainingForTest(pos.OpenLots))
	}

	// MarkSnapshotDirty lands on the shard's own bundle.Persist (a real
	// persistence.Component), NOT harness.persist — that spy is wired
	// into the reserve service only. DrainDirty is the documented
	// non-destructive read for exactly this assertion.
	dirty := harness.shard.bundle.Persist.DrainDirty()
	if len(dirty) != 1 {
		t.Fatalf("DrainDirty() = %d entries, want 1", len(dirty))
	}
	if dirty[0].AccountID != string(harness.accountID) || dirty[0].SessionID != goldenSessionID {
		t.Errorf("dirty entry = %+v, want {SessionID:%q AccountID:%q}",
			dirty[0], goldenSessionID, harness.accountID)
	}
}

func TestHandleSeedInventory_SecondMintOnHeldInstrument_AccumulatesAndAppendsLot(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{}, nil)

	first := harness.shard.handleSeedInventory(SeedInventory{
		AccountID: harness.accountID, Instrument: goldenInstrument, Qty: 100, EntryPrice: 250,
	})
	if first.Err != nil {
		t.Fatalf("first mint: Err = %v, want nil", first.Err)
	}

	second := harness.shard.handleSeedInventory(SeedInventory{
		AccountID: harness.accountID, Instrument: goldenInstrument, Qty: 40, EntryPrice: 300,
	})
	if second.Err != nil {
		t.Fatalf("second mint: Err = %v, want nil", second.Err)
	}
	if second.Available != 140 {
		t.Fatalf("second mint: Available = %d, want 140 (100+40)", second.Available)
	}

	pos := harness.ledger.Get(harness.accountID).Positions[goldenInstrument]
	if len(pos.OpenLots) != 2 {
		t.Fatalf("len(OpenLots) = %d, want 2 (one lot per mint, FIFO tail-appended)", len(pos.OpenLots))
	}
	if got, want := pos.OpenLots[1].EntryPrice, int64(300); got != want {
		t.Errorf("OpenLots[1].EntryPrice = %d, want %d", got, want)
	}
}

func TestHandleSeedInventory_NoLiveSession_ReturnsPresentFalse(t *testing.T) {
	harness := newGoldenHarness(t)
	// No seedAccount call — the shard's sessions map has no entry for
	// harness.accountID.

	result := harness.shard.handleSeedInventory(SeedInventory{
		AccountID: harness.accountID, Instrument: goldenInstrument, Qty: 10, EntryPrice: 10,
	})

	if result.Present {
		t.Fatalf("Present = true, want false (no live session)")
	}
	if result.Err != nil {
		t.Errorf("Err = %v, want nil on the Present:false path", result.Err)
	}
	if result.Available != 0 {
		t.Errorf("Available = %d, want 0 on the Present:false path", result.Available)
	}
	if dirty := harness.shard.bundle.Persist.DrainDirty(); len(dirty) != 0 {
		t.Errorf("DrainDirty() = %d entries, want 0 (no mutation happened)", len(dirty))
	}
}

// SI4 — doc §8's claim made concrete: seeded units flow straight into
// ReserveIfAvailable's sell-reserve path with no special-casing. A sell
// reservation for exactly the seeded Qty must succeed; reserving one unit
// MORE than seeded must fail on insufficient position.
func TestHandleSeedInventory_SeededUnits_AreReservableForSell(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{}, nil)

	seeded := harness.shard.handleSeedInventory(SeedInventory{
		AccountID: harness.accountID, Instrument: goldenInstrument, Qty: 50, EntryPrice: 100,
	})
	if seeded.Err != nil {
		t.Fatalf("seed: Err = %v, want nil", seeded.Err)
	}

	sellAll := reserve.Order{
		OrderID:      "seed-sell-1",
		AccountID:    account.AccountID(harness.accountID),
		SessionID:    goldenSessionID,
		InstrumentID: goldenInstrument,
		Side:         contract.Sell,
		Qty:          50,
		LimitPrice:   100,
	}
	if err := harness.shard.resv.ReserveIfAvailable(reserve.Participant{ID: goldenParticipantID}, sellAll); err != nil {
		t.Fatalf("ReserveIfAvailable(qty=seeded): unexpected error: %v", err)
	}

	pos := harness.ledger.Get(harness.accountID).Positions[goldenInstrument]
	if pos.Available != 0 || pos.Reserved != 50 {
		t.Errorf("post-reserve position = {Available:%d Reserved:%d}, want {0, 50}", pos.Available, pos.Reserved)
	}

	overReserve := reserve.Order{
		OrderID:      "seed-sell-2",
		AccountID:    account.AccountID(harness.accountID),
		SessionID:    goldenSessionID,
		InstrumentID: goldenInstrument,
		Side:         contract.Sell,
		Qty:          1,
		LimitPrice:   100,
	}
	if err := harness.shard.resv.ReserveIfAvailable(reserve.Participant{ID: goldenParticipantID}, overReserve); err == nil {
		t.Fatalf("ReserveIfAvailable(qty=1 beyond seeded): want an insufficient-position error, got nil")
	}
}

// sumRemainingForTest mirrors economy/account's package-private
// sumRemaining (unexported there) so this test can restate the §0.6
// coupling check without reaching into account's internals.
func sumRemainingForTest(lots []account.Lot) int64 {
	var total int64
	for _, lot := range lots {
		total += lot.RemainingQty
	}
	return total
}
