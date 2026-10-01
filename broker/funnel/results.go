package funnel

// Reply-payload skeletons for the Tier-0 command model.
//
// Tier 0 defines the machine; the body of each result — every field
// beyond what the funnel itself needs to *transport* an answer — is
// owned by the tier that fills its handler:
//   - PlaceResult / CancelResult          → Tier 1 (order intake / cancel)
//   - BankResult                          → Tier 3 (bank transfer)
//   - BalanceSnapshot / PositionsSnapshot → Tier 4 (read surfaces)
//
// Similarly, OrderSpec and StopRecord are the *payload* shapes carried
// in a command; their fields are the owning tier's concern. The funnel
// only needs the type names to exist so the command structs compile and
// the reply channels have a concrete element type.
//
// Each type below is deliberately a bare struct — a tier-owned seam,
// not a Tier-0 contract. Adding a field here is a signal that the owning
// tier is being implemented; add it in that tier's stage, not in this
// file.

// PlaceResult is the reply to a PlaceOrder command. Body owned by Tier 1.
type PlaceResult struct{}

// CancelResult is the reply to a CancelOrder command. Body owned by Tier 1.
type CancelResult struct{}

// BankResult is the reply to a BankMove command. Body owned by Tier 3.
type BankResult struct{}

// BalanceSnapshot is the reply to a ReadBalance command. Body owned by Tier 4.
type BalanceSnapshot struct{}

// PositionsSnapshot is the reply to a ReadPositions command. Body owned by Tier 4.
type PositionsSnapshot struct{}

// OrderSpec is the payload of a PlaceOrder command (side, instrument,
// qty, type, limit price, etc.). Body owned by Tier 1.
type OrderSpec struct{}

// StopRecord is the payload of a FireStop command. Body owned by Tier 2.
type StopRecord struct{}

// BankDir names the two legal directions of a bank transfer. The values
// are declared here so command dispatch code can name them; the wiring
// that consumes them (durableTransfer, MoveCash) is Tier 3.
type BankDir int

const (
	// PullToTrading moves money Bank → Available (funds a live session).
	PullToTrading BankDir = iota + 1
	// PushToBank moves money Available → Bank (parks funds off-session).
	PushToBank
)
