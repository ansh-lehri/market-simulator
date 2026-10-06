package funnel

// MoneyConfig carries the money dials the Broker reads at the off-shard
// Phase-2 orchestration surface:
//
//   - StartingCapital / StartingBank — stamped on every account the broker
//     brings into existence via EnsureAccount (lifecycle.OpenAccount takes
//     these as arguments; the broker is the policy owner of the values).
//   - OnDemandMinimum — the gate QualifyOnDemand compares the account's
//     signed trading_cash against. The orchestrator never sees this
//     number; it receives only the resulting boolean (BROKER_PHASE2 §2 /
//     P2-2·D1 — orchestrator money-blind, A2.8).
//   - SlippageBufferBps — Phase-5 Point-1 market-buy sizing dial
//     (BROKER_PHASE5 §1.4 P5-D8). The buffer term in reserve.Order's
//     requiredCash = bestAsk*qty + buffer is sized as
//     notional*bps/10000, truncated. Zero disables the buffer. The
//     Phase-5 default is 0 pending a real volatility-based pin when
//     B10 (market-data cache) lands.
//
// The struct is declared in funnel (not bringup) because the Broker holds
// the field and the type must be nameable from NewBroker's signature
// without funnel importing bringup. bringup composes a MoneyConfig from
// its own BrokerConfig at wiring time.
//
// All fields are int64; strictly >= 0 (validated at bringup). The actual
// values are open dials per BROKER_PHASE2 §1 / §2 and BROKER_PHASE5 §1.4
// — this struct pins the HOME, not the number.
type MoneyConfig struct {
	StartingCapital  int64
	StartingBank     int64
	OnDemandMinimum  int64
	SlippageBufferBps int64
}
