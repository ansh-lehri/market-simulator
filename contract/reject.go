package contract

// RejectReason is what the broker acts on when admission fails.
// See Exchange_Broker_Contract §4.3.
type RejectReason int

const (
	RejectMalformed     RejectReason = iota // structural: bad shape/type/qty/price fields
	RejectMarketClosed                      // session phase: closed / outside hours
	RejectNoLiquidity                       // liquidity: market order vs empty opposite book
	RejectBadTick                           // tick grid: price off the grid
	RejectBeyondCircuit                     // circuit band: price beyond the band
)

// Rejection carries the admission-failure detail. Reason is logic-bearing;
// Stage is human-readable traceability only ("structural", "session_phase",
// "liquidity", "tick_grid", "circuit_band"). See Exchange_Broker_Contract §4.3.
type Rejection struct {
	Reason RejectReason
	Stage  string
}
