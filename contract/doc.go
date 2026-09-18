// Package contract defines the exchange<->broker seam types.
//
// It contains only the exact shapes that cross the seam: value-type aliases,
// the inbound-message wrapper and its two payloads (FundedOrder, CancelRequest),
// and the private-result shape (EngineResult) with all its sub-types
// (MatchResult, Trade, RestingOrder, Rejection).
//
// The authority for every shape here is docs/exchange/Exchange_Broker_Contract.md.
// Where a component doc (EXCHANGE_11/12/14) disagrees, the contract doc wins.
//
// Pure data only: no logic, no I/O, no methods that do work (an enum String()
// for logging is fine). This package imports nothing from exchange/ or broker/;
// they import it, never the reverse. Prices and quantities are int64, never
// float.
package contract
