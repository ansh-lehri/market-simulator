package account

import (
	"errors"
	"fmt"
)

// -----------------------------------------------------------------------------
// Category sentinels — the STABLE dispatch surface for callers.
//
// Callers (the Tier-2 brain, boundary handlers) branch on these with
// errors.Is, so they never depend on which concrete struct a hand returned.
// Concrete errors below either ARE one of these (the value-shape sentinels)
// or Unwrap() to one of these (the struct errors), so a single errors.Is
// check works uniformly.
// -----------------------------------------------------------------------------

var (
	// ErrInvalidArgument: a hand received a malformed argument (non-positive
	// amount/units, non-positive entryPrice). Signals a caller bug — the
	// value should never have been passed — but is returned so the boundary
	// can surface it rather than crash the session.
	ErrInvalidArgument = errors.New("account: invalid argument")

	// ErrIllegalOperation: a hand received a (from,to) or target/source that
	// is not in the legal move-set (bank-not-reservable, no un-settling,
	// crediting into non-Available, debiting non-Available, same-bucket).
	// Structural rule violation — always a caller bug.
	ErrIllegalOperation = errors.New("account: illegal operation")

	// ErrInsufficientFunds: a cash transfer's source bucket does not cover
	// the requested amount. This is the buy-side fundability signal Tier-2
	// dispatches on to translate into an engine rejection.
	ErrInsufficientFunds = errors.New("account: insufficient funds")

	// ErrInsufficientUnits: a position operation's source bucket does not
	// cover the requested units — sell-side fundability signal (either
	// PosAvailable for a reserve, or PosReserved for a fill delivery).
	ErrInsufficientUnits = errors.New("account: insufficient units")
)

// -----------------------------------------------------------------------------
// Value-shape sentinels — retain their own identity AND chain to
// ErrInvalidArgument via %w, so callers can errors.Is either the specific
// sentinel or the category and get the right answer.
// -----------------------------------------------------------------------------

var (
	// ErrNonPositiveAmount: a cash hand received amount <= 0. Direction lives
	// in buckets, never in the sign of an argument.
	ErrNonPositiveAmount = fmt.Errorf("%w: amount must be > 0", ErrInvalidArgument)

	// ErrNonPositiveUnits: a position hand received units <= 0.
	ErrNonPositiveUnits = fmt.Errorf("%w: units must be > 0", ErrInvalidArgument)

	// ErrBadPrice: CreditPosition received entryPrice <= 0. A zero/negative
	// cost basis would corrupt every future P&L that matches this lot.
	ErrBadPrice = fmt.Errorf("%w: entryPrice must be > 0", ErrInvalidArgument)
)

// -----------------------------------------------------------------------------
// Struct errors — carry the context boundary logs need (bucket names,
// have/want values). Unwrap() points at the category sentinel above so
// errors.Is(err, ErrInsufficientFunds) etc. dispatches uniformly regardless
// of which concrete type the hand returned.
// -----------------------------------------------------------------------------

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

func (err ErrIllegalMove) Unwrap() error { return ErrIllegalOperation }

// ErrIllegalPosMove is returned by MovePosition when (From, To) is not in the
// two-entry legal position-move set.
type ErrIllegalPosMove struct {
	From PosBucket
	To   PosBucket
}

func (err ErrIllegalPosMove) Error() string {
	return fmt.Sprintf("account: illegal position move %s→%s", err.From, err.To)
}

func (err ErrIllegalPosMove) Unwrap() error { return ErrIllegalOperation }

// ErrIllegalCreditTarget is returned by CreditCash for any target other than
// Available. Proceeds are tradeable on arrival (STL-2); minting into
// Reserved / Settled / Bank is nonsense.
type ErrIllegalCreditTarget struct {
	To Bucket
}

func (err ErrIllegalCreditTarget) Error() string {
	return fmt.Sprintf("account: illegal credit target %s (only Available)", err.To)
}

func (err ErrIllegalCreditTarget) Unwrap() error { return ErrIllegalOperation }

// ErrIllegalBurnSource is returned by DebitCash for any source other than
// Available. Costs come out of spendable cash; debiting Reserved/Settled/Bank
// would corrupt in-flight reservations or the realized figure.
type ErrIllegalBurnSource struct {
	From Bucket
}

func (err ErrIllegalBurnSource) Error() string {
	return fmt.Sprintf("account: illegal burn source %s (only Available)", err.From)
}

func (err ErrIllegalBurnSource) Unwrap() error { return ErrIllegalOperation }

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

func (err ErrInsufficient) Unwrap() error { return ErrInsufficientFunds }

// ErrInsufficientPosition is returned by MovePosition when the source position
// bucket does not hold enough units. Where "you can't double-sell the same
// units" becomes structural.
type ErrInsufficientPosition struct {
	Have int64
	Want int64
}

func (err ErrInsufficientPosition) Error() string {
	return fmt.Sprintf("account: insufficient position (have=%d want=%d)", err.Have, err.Want)
}

func (err ErrInsufficientPosition) Unwrap() error { return ErrInsufficientUnits }

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

func (err ErrInsufficientReserved) Unwrap() error { return ErrInsufficientUnits }
