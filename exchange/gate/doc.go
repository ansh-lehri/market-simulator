// Package gate implements the exchange's admission pipeline (EXCHANGE_14).
// A FundedOrder from the broker enters here first; the pipeline validates it
// (structural, session phase, liquidity, tick grid, circuit band), stamps its
// admission sequence, and either routes it to the continuous matcher or the
// closing-auction batch — or rejects it. It produces exactly one
// contract.EngineResult per submission. Gate 4 (funding) is absent: gate
// consults no money state and holds no account reference.
package gate
