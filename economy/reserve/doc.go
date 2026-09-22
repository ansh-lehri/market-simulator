// Package reserve is Layer 5.2 — the reservation brain of the broker's
// money engine. It decides WHAT moves and WHY on the four order-lifecycle
// events (admission, fill, non-fill terminal, market-buy true-up) and
// directs the Tier-1 hands (account) to perform the mutation. It holds no
// balances and never mutates an Account field directly.
//
// The four methods:
//
//  1. ReserveIfAvailable — the engine's one synchronous consult at
//     Admission Gate 4. Atomic check-and-reserve.
//  2. Convert — per-fill lifecycle bookkeeping.
//  3. Release — non-fill terminal (cancel, expire, IOC/market remainder,
//     FOK reject, auction lapse, qty-decrease modify).
//  4. ReleaseExcess — market-buy buffer true-up on completion.
//
// The core invariant (LAYER_5_2_RESERVATION_FLOWS.md §0.1): the reservation
// covers EXACTLY the still-unfilled quantity; debits/credits cover EXACTLY
// the filled quantity; the two never overlap. Every method preserves it.
//
// State ownership: the ReservationBook is 5.2-held (separate from Account),
// whole-economy scope, keyed by OrderID. Each record carries its own
// AccountID/SessionID (link-back ids, never a pointer into an Account).
// Cost-lifecycle flags OrigQty / EverFilled / BrokerageCharged drive the
// once-per-executed-order brokerage charge (LOCKED §3.4 / §5).
//
// Deps (top of the compute stack, built last): account (hands +
// PositionFlat), cost (CostModule), realize (Realizer.OnSellFill).
// Seams: AccountLedger (read handle), Persistence (async MarkSnapshotDirty
// only — reserve never enqueues), EventSink (call sites wired; system
// deferred §9).
//
// Spec: docs/economy/LAYER_5_2_RESERVATION_FLOWS.md.
package reserve
