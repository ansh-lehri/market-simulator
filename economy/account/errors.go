package account

import (
	"errors"
	"fmt"
)

// Context-free sentinel errors — returned when the guard's identity is the
// entire message (nothing else to carry).
var (
	// ErrNonPositiveAmount: a hand received amount <= 0. Direction lives in
	// buckets, never in the sign of an argument.
	ErrNonPositiveAmount = errors.New("account: amount must be > 0")

	// ErrNonPositiveUnits: a position hand received units <= 0.
	ErrNonPositiveUnits = errors.New("account: units must be > 0")

	// ErrBadPrice: CreditPosition received entryPrice <= 0. A zero/negative
	// cost basis would corrupt every future P&L that matches this lot; the
	// caller (a buy-fill event) is buggy.
	ErrBadPrice = errors.New("account: entryPrice must be > 0")
)

// ErrIllegalMove is returned by MoveCash when (From, To) is not in the legal
// cash-move set (§1.1). Encodes structural rules like "bank is not reservable"
// and "no un-settling a fill".
type ErrIllegalMove struct {
	From Bucket
	To   Bucket
}

func (err ErrIllegalMove) Error() string {
	return fmt.Sprintf("account: illegal cash move %s→%s", err.From, err.To)
}

// ErrIllegalPosMove is returned by MovePosition when (From, To) is not in the
// two-entry legal position-move set.
type ErrIllegalPosMove struct {
	From PosBucket
	To   PosBucket
}

func (err ErrIllegalPosMove) Error() string {
	return fmt.Sprintf("account: illegal position move %s→%s", err.From, err.To)
}

// ErrInsufficient is returned by MoveCash when the source bucket does not hold
// enough coins to cover the transfer. Bucket-underflow protection — a
// different concept from Option B's signed Available (which is DebitCash's
// doing, not a transfer's).
type ErrInsufficient struct {
	Bucket Bucket
	Have   int64
	Want   int64
}

func (err ErrInsufficient) Error() string {
	return fmt.Sprintf("account: insufficient %s (have=%d want=%d)", err.Bucket, err.Have, err.Want)
}

// ErrInsufficientPosition is returned by MovePosition when the source position
// bucket does not hold enough units. This is where "you can't double-sell the
// same units" becomes structural.
type ErrInsufficientPosition struct {
	Have int64
	Want int64
}

func (err ErrInsufficientPosition) Error() string {
	return fmt.Sprintf("account: insufficient position (have=%d want=%d)", err.Have, err.Want)
}

// ErrInsufficientReserved is returned by RemovePosition when the requested
// deliver-out exceeds Reserved. Units in flight for a sell live in Reserved;
// checking Available here would be wrong.
type ErrInsufficientReserved struct {
	Have int64
	Want int64
}

func (err ErrInsufficientReserved) Error() string {
	return fmt.Sprintf("account: insufficient reserved position (have=%d want=%d)", err.Have, err.Want)
}

// ErrIllegalCreditTarget is returned by CreditCash for any target other than
// Available. Proceeds are tradeable on arrival (STL-2); minting into Reserved
// / Settled / Bank is nonsense.
type ErrIllegalCreditTarget struct {
	To Bucket
}

func (err ErrIllegalCreditTarget) Error() string {
	return fmt.Sprintf("account: illegal credit target %s (only Available)", err.To)
}

// ErrIllegalBurnSource is returned by DebitCash for any source other than
// Available. Costs come out of spendable cash; debiting Reserved/Settled/Bank
// would corrupt in-flight reservations or the realized figure.
type ErrIllegalBurnSource struct {
	From Bucket
}

func (err ErrIllegalBurnSource) Error() string {
	return fmt.Sprintf("account: illegal burn source %s (only Available)", err.From)
}
