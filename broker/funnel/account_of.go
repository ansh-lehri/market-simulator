package funnel

import (
	"strings"

	"simulator/contract"
	"simulator/economy/account"
)

// accountOf parses the owning AccountID out of a broker-minted OrderID.
//
// Broker intake mints every OrderID as "<AccountID>:<seq>" where seq is a
// colon-free uint64 (BROKER_TIER0 §Point-3). The last ":" is therefore
// always the account/seq boundary — parsing with LastIndex (not SplitN)
// keeps the primitive correct even if an AccountID itself ever carries a
// ":" (BROKER_PHASE4 §2.4, P4-D6).
//
// Returns parsed = false for a malformed OrderID:
//   - no ":"             → not minted by this broker's intake.
//   - leading ":" only   → empty AccountID prefix.
//   - trailing ":" only  → empty seq slot (broker-mint invariant violation).
//
// The settlement consumer logs-and-skips on parsed = false rather than
// panicking: a malformed OrderID is a broker bug, not a runtime condition,
// and killing the consumer would halt settlement for the entire exchange
// instance for the sake of one unroutable result (BROKER_PHASE4 §2.4,
// P4-D7). No account exists to route the result to anyway.
func accountOf(orderID contract.OrderID) (account.AccountID, bool) {
	lastColon := strings.LastIndex(orderID, ":")
	if lastColon <= 0 || lastColon == len(orderID)-1 {
		return "", false
	}
	return account.AccountID(orderID[:lastColon]), true
}
