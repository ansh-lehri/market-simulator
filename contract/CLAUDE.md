# contract — the exchange↔broker seam types

## Spec / authority
`docs/exchange/Exchange_Broker_Contract` is THE authority for every shape here. Read
it first. Cross-check against EXCHANGE_11 (value types, FundedOrder), EXCHANGE_12
(Trade, MatchResult), EXCHANGE_14 (EngineResult, Rejection), but **the contract doc
wins on any disagreement.** Known example: the contract's `Outcome` enum has 6 values
(`OutcomeMatched, OutcomeDiverted, OutcomeRejected, OutcomeCancelled,
OutcomeCancelRejected, OutcomeMakerFill`) — EXCHANGE_14 lists only 3. Use the 6.

## What this package owns
The exact types that cross the seam, and nothing else:
- Value aliases: `Ticks, Qty (int64)`; `OrderID, ParticipantID, InstrumentID,
  BrokerID (string)`; `Seq (uint64)`.
- Inbound: `InboundMessage` + `InboundKind`; `FundedOrder` + `Side`/`OrdType`/`TIF`/
  `STPMode`; `CancelRequest`.
- Private result: `EngineResult` + `Outcome`; `MatchResult`; `Rejection` +
  `RejectReason`; `Trade` + `CancelReason`.

## Rules
- **Pure data types only.** No logic, no I/O, no methods that do work (an enum
  `String()` for logging is fine). No imports from `exchange/` or `broker/` —
  `contract` is imported BY them, never the reverse.
- Integer discipline: money/prices are `int64` ticks/coins, never float.

## Do NOT
- Invent or rename a field. Match the contract doc exactly.
- Reconstruct a shape from memory — read the doc.
- If a component doc and the contract doc disagree, the contract doc wins — implement
  that and flag the discrepancy, don't silently pick one.