package persistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"simulator/contract"
)

// buildStoreRedis returns a store bound to Redis only (Postgres unused). The
// namespace scopes all keys to this test.
func buildStoreRedis(t *testing.T, timeEnc TimeEncoding, sideEnc SideEncoding) (*store, string) {
	client, namespace := requireRedis(t)
	cfg := Config{
		TimeEncoding: timeEnc,
		SideEncoding: sideEnc,
		KeyNamespace: namespace,
	}.withDefaults()
	return newStore(nil, client, IndiaCostAdapter{}, cfg), namespace
}

func makeSamplePayload() SnapshotPayload {
	dripProgress := int64(4200)
	entryTime := time.Date(2026, 6, 21, 12, 0, 0, 500, time.UTC)
	return SnapshotPayload{
		SessionID: "sess-42",
		AccountID: "acct-42",
		Cash: CashSnap{
			Available:    -777, // signed
			Reserved:     100,
			Settled:      50,
			BankCash:     12345,
			DripActive:   true,
			DripProgress: &dripProgress,
		},
		Positions: []PositionSnap{
			{
				InstrumentID:   "ACME",
				AvailableUnits: 80,
				ReservedUnits:  20,
				OpenLots: []LotSnap{
					{EntryPrice: 50000, EntryAt: entryTime, RemainingQty: 30},
					{EntryPrice: 50100, EntryAt: entryTime.Add(1 * time.Minute), RemainingQty: 50},
				},
			},
			{
				InstrumentID:   "ZOOM",
				AvailableUnits: 10,
				ReservedUnits:  0,
				OpenLots: []LotSnap{
					{EntryPrice: 20000, EntryAt: entryTime.Add(2 * time.Minute), RemainingQty: 10},
				},
			},
		},
		Reservations: []ReservationSnap{
			{
				OrderID:          "ord-1",
				AccountID:        "acct-42",
				SessionID:        "sess-42",
				InstrumentID:     "ACME",
				Side:             contract.Buy,
				IsMarket:         false,
				ReservedCash:     50000,
				ReservedUnits:    0,
				RemainingQty:     10,
				OrigQty:          10,
				EverFilled:       false,
				BrokerageCharged: false,
			},
			{
				OrderID:          "ord-2",
				AccountID:        "acct-42",
				SessionID:        "sess-42",
				InstrumentID:     "ACME",
				Side:             contract.Sell,
				IsMarket:         true,
				ReservedCash:     0,
				ReservedUnits:    5,
				RemainingQty:     5,
				OrigQty:          5,
				EverFilled:       false,
				BrokerageCharged: false,
			},
		},
	}
}

func sortSnapshot(payload *SnapshotPayload) {
	sort.Slice(payload.Positions, func(i, j int) bool {
		return payload.Positions[i].InstrumentID < payload.Positions[j].InstrumentID
	})
	sort.Slice(payload.Reservations, func(i, j int) bool {
		return payload.Reservations[i].OrderID < payload.Reservations[j].OrderID
	})
}

func TestSnapshotWriteReadRoundTrip(t *testing.T) {
	// Full three-key-group round-trip. FIFO OpenLots order preserved.
	// Runs across all four TimeEncoding × SideEncoding combinations so no
	// encoding pair breaks silently.
	encodings := []struct {
		name     string
		timeEnc  TimeEncoding
		sideEnc  SideEncoding
	}{
		{"rfc3339+string", TimeRFC3339, SideString},
		{"rfc3339+int", TimeRFC3339, SideInt},
		{"unixnano+string", TimeUnixNano, SideString},
		{"unixnano+int", TimeUnixNano, SideInt},
	}
	for _, enc := range encodings {
		t.Run(enc.name, func(subtest *testing.T) {
			sto, _ := buildStoreRedis(subtest, enc.timeEnc, enc.sideEnc)

			original := makeSamplePayload()
			if err := sto.writeSnapshot(context.Background(), original); err != nil {
				subtest.Fatalf("writeSnapshot: %v", err)
			}
			got, err := sto.readSnapshot(context.Background(), original.SessionID, original.AccountID)
			if err != nil {
				subtest.Fatalf("readSnapshot: %v", err)
			}

			// Normalize map/SCAN iteration order before comparing.
			sortSnapshot(&original)
			sortSnapshot(&got)

			if !reflect.DeepEqual(got.Cash, original.Cash) {
				subtest.Errorf("Cash mismatch:\n got  %+v\n want %+v", got.Cash, original.Cash)
			}
			if len(got.Positions) != len(original.Positions) {
				subtest.Fatalf("Positions len: got %d, want %d", len(got.Positions), len(original.Positions))
			}
			for i := range got.Positions {
				gotPos := got.Positions[i]
				wantPos := original.Positions[i]
				if gotPos.InstrumentID != wantPos.InstrumentID ||
					gotPos.AvailableUnits != wantPos.AvailableUnits ||
					gotPos.ReservedUnits != wantPos.ReservedUnits {
					subtest.Errorf("Position[%d] header: got %+v, want %+v", i, gotPos, wantPos)
				}
				if len(gotPos.OpenLots) != len(wantPos.OpenLots) {
					subtest.Fatalf("Position[%d] OpenLots len: got %d, want %d", i, len(gotPos.OpenLots), len(wantPos.OpenLots))
				}
				// FIFO order MUST be preserved inside a position (§7 + §11).
				for j := range gotPos.OpenLots {
					gotLot := gotPos.OpenLots[j]
					wantLot := wantPos.OpenLots[j]
					if gotLot.EntryPrice != wantLot.EntryPrice ||
						gotLot.RemainingQty != wantLot.RemainingQty ||
						!gotLot.EntryAt.Equal(wantLot.EntryAt) {
						subtest.Errorf("Position[%d].OpenLots[%d]: got %+v, want %+v", i, j, gotLot, wantLot)
					}
				}
			}
			if !reflect.DeepEqual(got.Reservations, original.Reservations) {
				subtest.Errorf("Reservations mismatch:\n got  %+v\n want %+v", got.Reservations, original.Reservations)
			}
		})
	}
}

func TestSnapshotWriteCleansStaleKeys(t *testing.T) {
	// A subsequent write with fewer positions/reservations must DELete the
	// dropped keys from Redis (D3 write-then-clean semantics baked into
	// writeSnapshot).
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)
	full := makeSamplePayload()
	if err := sto.writeSnapshot(context.Background(), full); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Rewrite with only one position + one reservation.
	shrunk := SnapshotPayload{
		SessionID:    full.SessionID,
		AccountID:    full.AccountID,
		Cash:         full.Cash,
		Positions:    []PositionSnap{full.Positions[0]},
		Reservations: []ReservationSnap{full.Reservations[0]},
	}
	if err := sto.writeSnapshot(context.Background(), shrunk); err != nil {
		t.Fatalf("second write: %v", err)
	}

	got, err := sto.readSnapshot(context.Background(), full.SessionID, full.AccountID)
	if err != nil {
		t.Fatalf("readSnapshot: %v", err)
	}
	if len(got.Positions) != 1 {
		t.Errorf("stale position not cleaned: got %d, want 1", len(got.Positions))
	}
	if len(got.Reservations) != 1 {
		t.Errorf("stale reservation not cleaned: got %d, want 1", len(got.Reservations))
	}
	// Verify the kept ones are the ones we passed.
	if got.Positions[0].InstrumentID != full.Positions[0].InstrumentID {
		t.Errorf("kept position mismatch: got %s, want %s",
			got.Positions[0].InstrumentID, full.Positions[0].InstrumentID)
	}
	if got.Reservations[0].OrderID != full.Reservations[0].OrderID {
		t.Errorf("kept reservation mismatch: got %s, want %s",
			got.Reservations[0].OrderID, full.Reservations[0].OrderID)
	}
}

func TestSnapshotDeleteRemovesAllThreeKeyGroups(t *testing.T) {
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)
	original := makeSamplePayload()
	if err := sto.writeSnapshot(context.Background(), original); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}

	if err := sto.deleteSnapshot(context.Background(), original.SessionID, original.AccountID); err != nil {
		t.Fatalf("deleteSnapshot: %v", err)
	}

	got, err := sto.readSnapshot(context.Background(), original.SessionID, original.AccountID)
	if !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("readSnapshot after delete: err = %v, want ErrSnapshotMissing", err)
	}
	if got.SessionID != original.SessionID || got.AccountID != original.AccountID {
		t.Errorf("readSnapshot: ids not populated on ErrSnapshotMissing: %+v", got)
	}
	if got.Cash != (CashSnap{}) {
		t.Errorf("Cash not cleared after delete: %+v", got.Cash)
	}
	if len(got.Positions) != 0 {
		t.Errorf("Positions not cleared: %v", got.Positions)
	}
	if len(got.Reservations) != 0 {
		t.Errorf("Reservations not cleared: %v", got.Reservations)
	}
}

func TestSnapshotReadMissingAccountReturnsErrSnapshotMissing(t *testing.T) {
	// An account never snapshotted returns ErrSnapshotMissing so lifecycle
	// can hard-stop rather than fabricate a balance (LAYER_5_1_READ_OPS §5).
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)
	got, err := sto.readSnapshot(context.Background(), "no-session", "no-account")
	if !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("readSnapshot: err = %v, want ErrSnapshotMissing", err)
	}
	if got.SessionID != "no-session" || got.AccountID != "no-account" {
		t.Errorf("readSnapshot: ids not populated on ErrSnapshotMissing: %+v", got)
	}
	if got.Cash != (CashSnap{}) {
		t.Errorf("Cash: got %+v, want zero-value", got.Cash)
	}
	if len(got.Positions) != 0 || len(got.Reservations) != 0 {
		t.Errorf("positions/reservations: got %d/%d", len(got.Positions), len(got.Reservations))
	}
}

func TestSnapshotReadZeroCashIsDistinctFromMissing(t *testing.T) {
	// A brand-new session with zero starting capital writes an all-zero
	// CashSnap. It MUST be distinguishable from a missing snapshot — the
	// former is a legitimate seeded state; the latter is a bootstrap-order
	// bug. Without ErrSnapshotMissing the two collapse into the same
	// (payload, nil) return and lifecycle cannot tell them apart.
	sto, namespace := buildStoreRedis(t, TimeRFC3339, SideString)
	_ = namespace

	sessionID := "sess-zero"
	accountID := "acct-zero"

	// 1. Missing → ErrSnapshotMissing.
	if _, err := sto.readSnapshot(context.Background(), sessionID, accountID); !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("missing key: err = %v, want ErrSnapshotMissing", err)
	}

	// 2. Write an all-zero CashSnap (a valid seed for a zero-capital user).
	payload := SnapshotPayload{
		SessionID: sessionID,
		AccountID: accountID,
		Cash:      CashSnap{}, // Available=0, Reserved=0, Settled=0, BankCash=0, drip inactive.
	}
	if err := sto.writeSnapshot(context.Background(), payload); err != nil {
		t.Fatalf("writeSnapshot zero: %v", err)
	}

	// 3. Now the read succeeds with a zero payload — NOT ErrSnapshotMissing.
	got, err := sto.readSnapshot(context.Background(), sessionID, accountID)
	if err != nil {
		t.Fatalf("readSnapshot after zero write: %v", err)
	}
	if got.Cash != (CashSnap{}) {
		t.Errorf("Cash: got %+v, want zero-value", got.Cash)
	}
}

func TestSnapshotTwoAccountsAreIndependent(t *testing.T) {
	// Two accounts under the same session must not collide.
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)
	a := makeSamplePayload()
	a.AccountID = "acct-A"
	b := makeSamplePayload()
	b.AccountID = "acct-B"
	b.Cash.Available = 999

	for _, payload := range []SnapshotPayload{a, b} {
		if err := sto.writeSnapshot(context.Background(), payload); err != nil {
			t.Fatalf("write %s: %v", payload.AccountID, err)
		}
	}

	gotA, err := sto.readSnapshot(context.Background(), a.SessionID, a.AccountID)
	if err != nil {
		t.Fatalf("read A: %v", err)
	}
	gotB, err := sto.readSnapshot(context.Background(), b.SessionID, b.AccountID)
	if err != nil {
		t.Fatalf("read B: %v", err)
	}
	if gotA.Cash.Available != -777 {
		t.Errorf("A leaked from B or lost data: got %d", gotA.Cash.Available)
	}
	if gotB.Cash.Available != 999 {
		t.Errorf("B leaked from A or lost data: got %d", gotB.Cash.Available)
	}
}

// helper to make the sample payload compile-check the imports
var _ = fmt.Sprintf
