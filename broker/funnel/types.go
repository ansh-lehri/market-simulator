package funnel

import (
	"simulator/contract"
	"simulator/economy/account"
)

// Identity and key aliases used across the funnel.
//
// AccountID is aliased to the L5 type so the funnel key is exactly the
// type the economy hands take — no conversion at the L5 seam.
//
// OrderID, ParticipantID, InstrumentID, BrokerID are re-aliased from the
// contract package so the funnel reads self-contained without hiding the
// fact that these are seam types owned by contract/.
//
// SessionID and ExchangeID are string aliases local to the funnel; they
// do not cross the exchange seam. (economy/reserve and economy/realize
// each define their own SessionID = string; all three aliases resolve to
// the same underlying string type.)
type (
	AccountID     = account.AccountID    // = string, the L5 type
	SessionID     = string               // funnel-local; distinct from realize/reserve aliases in name only
	ExchangeID    = string               // funnel-local; identifies an exchange instance for logging
	OrderID       = contract.OrderID     // = string, minted "<AccountID>:<seq>" (encoding decided in Point 3)
	BrokerID      = contract.BrokerID    // = string
	ParticipantID = contract.ParticipantID // = string; self-match id, distinct from BrokerID
	InstrumentID  = contract.InstrumentID  // = string
)

// Command is the funnel's unit of work.
//
// Every account-touching job is a typed struct that satisfies this
// interface. The router (Broker.route) reads ONLY Account() to pick the
// owning shard; no other method is consulted on the ingress path.
//
// Two locked design decisions (BROKER_TIER0 §3, Q1/Q4) live behind this
// shape:
//   - Typed commands, not closures — account-mutating logic stays inside
//     the owner, auditable and consistent with the L5 hands living in one
//     place.
//   - One inbox per shard, keyed on AccountID — many accounts share one
//     shard's inbox, tagged by account.
//
// Concrete commands live in commands.go. Each implements Account()
// trivially by returning its stored AccountID.
type Command interface {
	Account() AccountID
}
