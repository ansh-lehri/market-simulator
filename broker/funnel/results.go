package funnel

// Reply-payload skeletons for the Tier-0 command model.
//
// Tier 0 defines the machine; the body of each result — every field
// beyond what the funnel itself needs to *transport* an answer — is
// owned by the tier that fills its handler:
//   - PlaceResult / CancelResult          → Tier 1 (order intake / cancel)
//   - TransferResult                      → Tier 3 (bank transfer)
//   - BalanceSnapshot / PositionsSnapshot → Tier 4 (read surfaces)
//
// Similarly, OrderSpec and StopRecord are the *payload* shapes carried
// in a command; their fields are the owning tier's concern. The funnel
// only needs the type names to exist so the command structs compile and
// the reply channels have a concrete element type.
//
// Each struct is a tier-owned seam, not a Tier-0 contract — add fields
// in the owning tier's stage, not here. TransferResult (below) is the
// one exception: its field set is fixed by Phase-3 (P3-D5) because the
// HTTP handler reads all four fields verbatim.

// PlaceResult is the reply to a PlaceOrder command. Body owned by Tier 1.
type PlaceResult struct{}

// CancelResult is the reply to a CancelOrder command. Body owned by Tier 1.
type CancelResult struct{}

// TransferResult is the reply to a Transfer command (BROKER_PHASE3 §1.3,
// P3-D5). Three outcomes, discriminated by (Present, Err):
//
//   - Present:false              — account is not live on this shard.
//                                  Internal-only signal; the HTTP handler
//                                  falls to Store.DurableTransfer. Never
//                                  surfaces to the client as a 409 (P3-D8).
//   - Present:true,  Err != nil  — account was live, MoveCash rejected
//                                  (e.g. account.ErrInsufficient). Nothing
//                                  moved — MoveCash fails before mutating.
//                                  HTTP maps to 422 (insufficient) or 500
//                                  (programming error).
//   - Present:true,  Err == nil  — success; Available/Bank are the live
//                                  post-move bucket values. HTTP → 200.
//
// Mode-blind {Available, Bank} shape (P3-D7): the on-demand vs canonical
// distinction never shows up on the wire — the handler persists or not
// based on PersistOnClose at session_save, not here.
type TransferResult struct {
	Present   bool
	Available int64
	Bank      int64
	Err       error
}

// BalanceSnapshot is the reply to a ReadBalance command. Body owned by Tier 4.
type BalanceSnapshot struct{}

// PositionsSnapshot is the reply to a ReadPositions command. Body owned by Tier 4.
type PositionsSnapshot struct{}

// OrderSpec is the payload of a PlaceOrder command (side, instrument,
// qty, type, limit price, etc.). Body owned by Tier 1.
type OrderSpec struct{}

// StopRecord is the payload of a FireStop command. Body owned by Tier 2.
type StopRecord struct{}
