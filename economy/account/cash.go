package account

// -----------------------------------------------------------------------------
// Legality helpers — this is where structural rules (bank-not-reservable,
// no un-settling, proceeds-only-into-Available, costs-only-out-of-Available)
// become STRUCTURAL, not merely documented.
// -----------------------------------------------------------------------------

// legalCashMove reports whether a (from, to) transfer is a permitted move for
// MoveCash. The 5-entry set covers the buy reserve/convert/release/release_excess
// paths plus bank-refill/bank-pull; everything else — Settled→Reserved
// (un-settle nonsense), Bank→Reserved (bank is not trading-visible), same-bucket
// no-ops — is refused so the invariant is enforced by the switch, not by the
// caller remembering to check.
func legalCashMove(from, to Bucket) bool {
	switch {
	case from == Available && to == Reserved:
		return true // reserve (buy)
	case from == Reserved && to == Settled:
		return true // buy convert
	case from == Reserved && to == Available:
		return true // release / release_excess
	case from == Available && to == Bank:
		return true // bank-refill
	case from == Bank && to == Available:
		return true // bank-pull
	default:
		return false
	}
}

// legalCreditTarget constrains where minted cash may land. Proceeds are
// tradeable on arrival (STL-2); minting into Reserved (money behind a
// non-existent order), Settled (pretending cash was already spent-and-realized),
// or Bank (proceeds silently vanishing) is nonsense.
func legalCreditTarget(to Bucket) bool {
	return to == Available
}

// legalBurnSource constrains where burned cash leaves from. Costs come out of
// spendable cash; debiting Reserved would fee-charge locked order money, and
// debiting Settled would muddy the realized figure 5.4 reconciles against.
func legalBurnSource(from Bucket) bool {
	return from == Available
}

// bucketPtr returns a pointer to the named cash bucket, so writes through it
// hit the real struct. Returns nil for an unknown Bucket value — legalCashMove
// / legalCreditTarget / legalBurnSource filter first so no live path can
// nil-deref; a nil here would be a programming error (someone added a Bucket
// variant without updating this switch).
func (cash *Cash) bucketPtr(bucket Bucket) *int64 {
	switch bucket {
	case Available:
		return &cash.Available
	case Reserved:
		return &cash.Reserved
	case Settled:
		return &cash.Settled
	case Bank:
		return &cash.Bank
	}
	return nil
}

// -----------------------------------------------------------------------------
// The three cash hands. All in-memory, zero I/O. On ANY error, state is
// untouched (validation completes before the first field write).
// -----------------------------------------------------------------------------

// MoveCash moves `amount` coins from bucket `from` to bucket `to` within one
// account's cash. Pure, conserving, in-memory, zero I/O. Direction is entirely
// in from/to; amount is always > 0. Enforces §0.6 cash conservation with a
// panic — since same-bucket edges are illegal and every legal edge moves
// between two distinct buckets, a violation here means bucketPtr or
// legalCashMove has been mis-edited.
func (cash *Cash) MoveCash(from, to Bucket, amount int64) error {
	if amount <= 0 {
		return ErrNonPositiveAmount
	}
	if !legalCashMove(from, to) {
		return ErrIllegalMove{From: from, To: to}
	}

	source := cash.bucketPtr(from)
	destination := cash.bucketPtr(to)

	if *source < amount {
		return ErrInsufficient{Bucket: from, Have: *source, Want: amount}
	}

	sumBefore := cash.Available + cash.Reserved + cash.Settled + cash.Bank

	*source -= amount
	*destination += amount

	sumAfter := cash.Available + cash.Reserved + cash.Settled + cash.Bank
	if sumBefore != sumAfter {
		panic("MoveCash violated cash conservation — bucketPtr/legalCashMove out of sync")
	}
	return nil
}

// CreditCash mints `amount` coins (arriving from outside the account, e.g.
// sell proceeds) into a cash bucket. Increases total cash on purpose — no
// conservation assert. Strictly positive amount. In-memory only, zero I/O.
//
// Debt priority (MON-7) is NOT this hand's decision: the caller (5.5) either
// routes proceeds to debt settlement or calls CreditCash(Available, amount).
// Branching on debt state here would put policy in a hand.
func (cash *Cash) CreditCash(to Bucket, amount int64) error {
	if amount <= 0 {
		return ErrNonPositiveAmount
	}
	if !legalCreditTarget(to) {
		return ErrIllegalCreditTarget{To: to}
	}

	*cash.bucketPtr(to) += amount
	return nil
}

// DebitCash burns `amount` coins out of a cash bucket (costs leaving the sim).
// Under Option B it MAY drive Available negative — there is deliberately NO
// sufficiency gate, matching real-broker behaviour (fees always apply, even
// if the resulting balance goes below zero). Decreases total cash on purpose —
// no conservation assert. Strictly positive amount. In-memory only, zero I/O.
//
// The resulting negative naturally suppresses new buying: a subsequent buy's
// reserve_if_available check fails any positive requirement while Available < 0.
// Coming back from negative is 5.5's job (debt-priority routing of later funds).
func (cash *Cash) DebitCash(from Bucket, amount int64) error {
	if amount <= 0 {
		return ErrNonPositiveAmount
	}
	if !legalBurnSource(from) {
		return ErrIllegalBurnSource{From: from}
	}

	*cash.bucketPtr(from) -= amount
	return nil
}
