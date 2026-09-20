package account

import "time"

// -----------------------------------------------------------------------------
// Legality + pointer helpers for the position side.
// -----------------------------------------------------------------------------

// legalPosMove reports whether a (from, to) transfer is a permitted position
// move. Only two entries — the sell-side lock and unlock. Buy fills (units in)
// go through CreditPosition; sell fills (units out) go through RemovePosition;
// neither is MovePosition. There is no PosSettled and no cross-pool concept.
func legalPosMove(from, to PosBucket) bool {
	switch {
	case from == PosAvailable && to == PosReserved:
		return true // sell reserve (lock)
	case from == PosReserved && to == PosAvailable:
		return true // sell release (unlock)
	default:
		return false
	}
}

// posBucketPtr returns a pointer to the named position bucket. Returns nil for
// an unknown PosBucket value — legalPosMove filters first so no live path can
// nil-deref; a nil here would be a programming error (a new PosBucket variant
// added without updating this switch).
func (position *Position) posBucketPtr(bucket PosBucket) *int64 {
	switch bucket {
	case PosAvailable:
		return &position.Available
	case PosReserved:
		return &position.Reserved
	}
	return nil
}

// sumRemaining totals RemainingQty across the FIFO queue. Used by
// RemovePosition's §0.6 coupling assert; kept package-private since 5.1
// external callers should read Position fields directly rather than reduce.
func sumRemaining(lots []Lot) int64 {
	var total int64
	for index := range lots {
		total += lots[index].RemainingQty
	}
	return total
}

// -----------------------------------------------------------------------------
// The three position hands. All in-memory, zero I/O. On ANY error, state is
// untouched (validation completes before the first field write).
// -----------------------------------------------------------------------------

// MovePosition moves `units` of one instrument between the two position
// buckets (PosAvailable ↔ PosReserved). Conserving; does NOT touch OpenLots —
// lot matching happens at FILL time (RemovePosition), never at reserve time.
// The sufficiency guard on PosAvailable→PosReserved is where "you can't
// double-sell the same units" becomes structural: units already locked by a
// prior resting sell live in Reserved, not Available, so a second reserve
// cannot re-lock them.
//
// Asserts unit conservation (Available+Reserved unchanged) with a panic —
// same-bucket edges are illegal and every legal edge moves between two
// distinct buckets, so a violation here means posBucketPtr or legalPosMove
// has been mis-edited. OpenLots is untouched by construction (the hand
// never reads or writes it), so §0.6 coupling is preserved without a
// separate assert.
func (position *Position) MovePosition(from, to PosBucket, units int64) error {
	if units <= 0 {
		return ErrNonPositiveUnits
	}
	if !legalPosMove(from, to) {
		return ErrIllegalPosMove{From: from, To: to}
	}

	source := position.posBucketPtr(from)
	destination := position.posBucketPtr(to)

	if *source < units {
		return ErrInsufficientPosition{Have: *source, Want: units}
	}

	sumBefore := position.Available + position.Reserved

	*source -= units
	*destination += units

	sumAfter := position.Available + position.Reserved
	if sumBefore != sumAfter {
		panic("MovePosition violated unit conservation — posBucketPtr/legalPosMove out of sync")
	}
	return nil
}

// CreditPosition adds `units` newly-bought units to PosAvailable and appends
// a lot at the TAIL of the FIFO queue. Increases total held units on purpose
// (no unit-conservation assert) — preserves the §0.6 coupling by adding the
// same `units` to Available AND to the new lot's RemainingQty.
//
// entryPrice and entryAt are passed in (not read from a clock/price source)
// so the hand stays deterministic: replaying the same call sequence from the
// same starting state reproduces the exact ledger, including lot timestamps.
// The caller (buy-fill path in `convert`) already has the maker price from
// the engine's fill event and the fill timestamp.
func (position *Position) CreditPosition(units int64, entryPrice int64, entryAt time.Time) error {
	if units <= 0 {
		return ErrNonPositiveUnits
	}
	if entryPrice <= 0 {
		return ErrBadPrice
	}

	position.Available += units
	position.OpenLots = append(position.OpenLots, Lot{
		EntryPrice:   entryPrice,
		EntryAt:      entryAt,
		RemainingQty: units,
	})
	return nil
}

// RemovePosition removes `units` from PosReserved (delivered by a sell fill)
// and consumes matching open lots FIFO from the HEAD, returning the
// cost-basis breakdown. Decreases total held units on purpose; does NO money
// math — 5.4 turns `matched` + the exit price into realized P&L.
//
// Guard is `units ≤ Reserved` (NOT Available): the sell reserved exactly its
// quantity up front via MovePosition, so units-in-flight live in Reserved.
// Even if Available would cover the request, the guard fires — this is the
// structural rule that a fill can only deliver what was actually locked.
//
// Asserts §0.6 position–lot coupling with a panic at the end. Under normal
// operation this is unreachable — the units ≤ Reserved guard combined with
// the invariant means the FIFO loop always completes cleanly. A panic here
// signals a pre-existing coupling break (e.g. CreditPosition and
// MovePosition drifted apart).
func (position *Position) RemovePosition(units int64) ([]LotMatch, error) {
	if units <= 0 {
		return nil, ErrNonPositiveUnits
	}
	if units > position.Reserved {
		return nil, ErrInsufficientReserved{Have: position.Reserved, Want: units}
	}

	position.Reserved -= units

	var matched []LotMatch
	remaining := units
	for remaining > 0 {
		lot := &position.OpenLots[0] // oldest lot (head)
		take := lot.RemainingQty
		if remaining < take {
			take = remaining
		}
		matched = append(matched, LotMatch{
			EntryPrice: lot.EntryPrice,
			Qty:        take,
			EntryAt:    lot.EntryAt,
		})
		lot.RemainingQty -= take
		remaining -= take
		if lot.RemainingQty == 0 {
			position.OpenLots = position.OpenLots[1:] // drop fully-consumed head
		}
	}

	if position.Available+position.Reserved != sumRemaining(position.OpenLots) {
		panic("RemovePosition desynced buckets from lots — §0.6 coupling broken")
	}
	return matched, nil
}
