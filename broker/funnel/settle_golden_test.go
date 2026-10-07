package funnel

import (
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// Stage-2 SETTLE GOLDENS — RED, expected values traced to the docs.
//
// Doc anchors used throughout:
//   - CONTRACT §N  → docs/broker/Exchange_Broker_Contract.md §N worked example
//   - P5 §3.4      → docs/broker/BROKER_PHASE5_IMPLEMENTATION_DESIGN.md §3.4
//                     (settle adapter, five-branch 5-branch switch)
//   - P5-DN        → Phase-5 decision log entry
//
// Current state: handleApplyResult is an empty-stub seam, so every golden
// FAILS its post-state assertions. That is the RED we want — expected
// values are pinned here, implementation (Stage 5) makes them green.
//
// What each golden pins:
//   (a) OrderTrack.Status transition after the handler returns
//   (b) Track-deletion status (finalize fired or not)
//   (c) The ordered reserve-event log (events.emitted) — ConvertEvent per fill,
//       ConvertEvent for terminal brokerage, ReleaseEvent for Release or
//       ReleaseExcess. Mirrors the exact Convert/Release/ReleaseExcess call
//       sequence Stage 5 must produce (verified via the economy/reserve
//       event seam, no interface spy needed).
//
// Pre-seed: every golden installs an account + reservation (so the reserve book has a
// record Convert / Release can act on) + an OrderTrack. events.reset() is
// then called so the pre-seed's own ReserveEvent is excluded from the
// settle-side assertions.
//
// Cost module: India (goldenIndiaRates) so Convert emits realistic fill
// costs AND the terminal brokerage debit (2360 paise = 2000 + 18% GST)
// fires on every Filled/Cancelled-with-fills case. Goldens do not pin the
// cost rupee amounts; they pin that the brokerage ConvertEvent is emitted
// exactly once at terminal — the India rates themselves have their own
// goldens in economy/cost / economy/reserve.
// -----------------------------------------------------------------------------

const (
	goldenOrderID OrderID = "acct-G:1"
)

// -----------------------------------------------------------------------------
// Golden 1 — OutcomeMatched / full fill (market buy, residue returned).
//
// Shape adapted from CONTRACT §9.1 (market buy, two prints). Values use the
// Phase-5 default slippageBufferBps=0 so numbers trace to that default.
//
// Setup:
//   bestAsk cached = 10021, qty = 50, bps = 0 → buffer = 0
//   MaxSpend  = 10021*50 + 0 = 501050
//   reserved  = 501050 (invariant: reserved == MaxSpend)
//   Walk fills (price improvement — book had better asks than cached):
//     Trade1: Price=10020, Qty=30 → cost 300600
//     Trade2: Price=10021, Qty=20 → cost 200420
//   actualCost = 501020
//   residue    = 501050 − 501020 = 30 (unused reservation returned by
//                                      ReleaseExcess — the market-buy
//                                      true-up on a fully-filled order)
//
// reserve call sequence Stage-5 must produce, per P5 §3.4 case "full fill":
//   Convert(oid, 30, 10020, _, "acct-G:1:1")   → ConvertEvent (fill)
//   Convert(oid, 20, 10021, _, "acct-G:1:2")   → ConvertEvent (fill)
//   chargeOrderCost (shared tail at RemainingQty=0, !BrokerageCharged)
//                                              → ConvertEvent (brokerage)
//   ReleaseExcess(oid)                         → ReleaseEvent (residue 30)
//
// Expected post-state: Status = Filled, track deleted.
func TestSettleGolden_Matched_FullFill_MarketBuy_WithResidue(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 50, IsMarket: true,
		BestAsk: 10_021, SlippageBuffer: 0,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, true /* isMarketBuy */)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{Seq: 1, Instrument: goldenInstrument, Price: 10_020, Qty: 30,
					TakerOrder: goldenOrderID, MakerOrder: "o-X", TakerSide: contract.Buy},
				{Seq: 2, Instrument: goldenInstrument, Price: 10_021, Qty: 20,
					TakerOrder: goldenOrderID, MakerOrder: "o-D", TakerSide: contract.Buy},
			},
			LastPrice: 10_021, LastPriceMoved: true,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	// (a) Status → Filled, (b) track deleted.
	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted after full fill, want finalize")
	}
	// (c) reserve-event log: 2 fill ConvertEvents + 1 brokerage ConvertEvent + 1 ReleaseEvent.
	if got := len(harness.events.emitted); got != 4 {
		t.Fatalf("event count = %d, want 4 (fill1 + fill2 + brokerage + ReleaseExcess)", got)
	}
	fill1 := mustConvertEvent(t, harness.events.emitted[0])
	if fill1.UnitsCredited != 30 || fill1.CashDebited != 10_020*30 {
		t.Errorf("fill1 = %+v, want UnitsCredited=30 CashDebited=300600", fill1)
	}
	fill2 := mustConvertEvent(t, harness.events.emitted[1])
	if fill2.UnitsCredited != 20 || fill2.CashDebited != 10_021*20 {
		t.Errorf("fill2 = %+v, want UnitsCredited=20 CashDebited=200420", fill2)
	}
	brokerage := mustConvertEvent(t, harness.events.emitted[2])
	if brokerage.UnitsCredited != 0 || brokerage.CashDebited != 0 || brokerage.Cost.Total == 0 {
		t.Errorf("brokerage event = %+v, want Cost.Total>0 and no fill-side fields", brokerage)
	}
	residue := mustReleaseEvent(t, harness.events.emitted[3])
	if residue.Cash != 30 || residue.Units != 0 {
		t.Errorf("ReleaseExcess residue = %+v, want Cash=30 (501050 reserved − 501020 actual)", residue)
	}
}

// -----------------------------------------------------------------------------
// Golden 2 — OutcomeMatched / partial fill + remainder rests (limit buy).
//
// CONTRACT §9.2. One print of 30 at 10021; remainder 70 rests at 10021.
// Expected: Convert×1 (fill), Status=PartiallyFilled, track RETAINED,
// NO terminal brokerage (RemainingQty > 0), NO Release/ReleaseExcess.
func TestSettleGolden_Matched_PartialFill_RemainderRests(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 100, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{Seq: 1, Instrument: goldenInstrument, Price: 10_021, Qty: 30,
					TakerOrder: goldenOrderID, MakerOrder: "o-X", TakerSide: contract.Buy},
			},
			Rested: &contract.RestingOrder{
				OrderID: goldenOrderID, BrokerID: goldenBrokerID, Side: contract.Buy,
				Price: 10_021, Remaining: 70,
			},
			LastPrice: 10_021, LastPriceMoved: true,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	tr, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("track deleted on partial fill, want retained for the resting remainder")
	}
	if tr.Status != OrderStatusPartiallyFilled {
		t.Errorf("Status = %v, want OrderStatusPartiallyFilled", tr.Status)
	}
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (one fill, no brokerage, no release)", got)
	}
	fill := mustConvertEvent(t, harness.events.emitted[0])
	if fill.UnitsCredited != 30 || fill.CashDebited != 10_021*30 {
		t.Errorf("fill = %+v, want UnitsCredited=30 CashDebited=300630", fill)
	}
}

// -----------------------------------------------------------------------------
// Golden 3 — OutcomeMatched / pure rest (P5-D17).
//
// Zero trades, Rested != nil — a limit that rested outright without any
// aggressing fill. Expected: NO reserve call, Status = Working (unchanged),
// track retained. The pure-rest guard is the specific fix the Phase-5 doc
// §3.4 case 1 added to resolve the lifecycle-doc §6.1 code-vs-note
// contradiction.
func TestSettleGolden_Matched_PureRest(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 100, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: nil,
			Rested: &contract.RestingOrder{
				OrderID: goldenOrderID, BrokerID: goldenBrokerID, Side: contract.Buy,
				Price: 10_021, Remaining: 100,
			},
			LastPrice: 0, LastPriceMoved: false,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	tr, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("track deleted on pure rest, want retained")
	}
	if tr.Status != OrderStatusWorking {
		t.Errorf("Status = %v, want OrderStatusWorking (pure rest, no fill)", tr.Status)
	}
	if got := len(harness.events.emitted); got != 0 {
		t.Fatalf("event count = %d, want 0 (zero reserve calls on pure rest)", got)
	}
}

// -----------------------------------------------------------------------------
// Golden 4 — OutcomeMatched / fills + funded-cap leftover (market buy).
//
// CONTRACT §9.3 shape: market buy capped by MaxSpend. Two fills (30 + 4
// units of 60 requested) and 26 cancelled with ReasonFundedCap.
//
// Reservation sized to §9.3 directly: MaxSpend = 350000 (an explicit buffer
// above bestAsk*qty — see Contract §9.3 inputs). Phase-5 passes
// BestAsk/SlippageBuffer into reserve.Order; we encode the 350000 as
// `BestAsk=5833 (floor), SlippageBuffer=1 (padding to total 350000)` is
// awkward — instead we use the exchange-oriented path and seed a market
// reserve whose requiredCash lands at exactly 350000 by using an inline
// bestAsk/buffer pair.
//
// Simpler: skip Contract-exact numbers here and use a self-contained
// scenario that exercises the SAME branch — fills + non-resting cancelled
// leftover. The Phase-5 branch logic is what we're pinning, not §9.3's
// specific tick values. Setup:
//   bestAsk = 10_000, qty = 60, bps = 0 → buffer = 0 → MaxSpend = 600_000
//   Fills: Trade1 = 10_000 × 40, Trade2 = 10_000 × 15 → total 55 units, cost 550_000
//   CancelledQty = 5, CancelReason = ReasonFundedCap
// Expected reserve-call sequence: Convert×2 (fills) → Convert×1 (brokerage) → Release(5).
// NO ReleaseExcess event: in case 4, `Release(CancelledQty)` is ALWAYS an exact-tail
// release — fills + CancelledQty = origQty, so RemainingQty-at-release equals CancelledQty.
// economy/reserve/release.go treats releaseQty == RemainingQty as a FULL release (not
// proportional), draining ReservedCash to zero and REMOVING the record. The subsequent
// ReleaseExcess call then finds no record and silently returns nil without emitting
// a ReleaseEvent. The gate `IsMarketBuy && Status==Filled` still fires the call
// (uniform with case 5 — clean full fill with residue), but it is a no-op in case 4.
//
// The ONLY market-buy branch that produces a ReleaseExcess event is case 5 (clean full
// fill), where Convert's shared tail may leave ReservedCash > 0 (price improvement +
// slippage buffer) and ReleaseExcess drains it. Golden 1 pins that path.
//
// Rupee numbers come from economy/reserve goldens one layer down.
func TestSettleGolden_Matched_FillsPlusFundedCap_MarketBuy(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 60, IsMarket: true,
		BestAsk: 10_000, SlippageBuffer: 0,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, true)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{Seq: 1, Instrument: goldenInstrument, Price: 10_000, Qty: 40,
					TakerOrder: goldenOrderID, MakerOrder: "o-X", TakerSide: contract.Buy},
				{Seq: 2, Instrument: goldenInstrument, Price: 10_000, Qty: 15,
					TakerOrder: goldenOrderID, MakerOrder: "o-D", TakerSide: contract.Buy},
			},
			CancelledQty: 5, CancelReason: contract.ReasonFundedCap,
			LastPrice: 10_000, LastPriceMoved: true,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted after fills + funded-cap leftover, want finalize")
	}
	// reserve-event sequence: 2 fill ConvertEvents + 1 brokerage ConvertEvent +
	// 1 Release (CancelledQty=5). ReleaseExcess fires in the handler but
	// no-ops (record already removed by Release's exact-tail drain) — no event.
	if got := len(harness.events.emitted); got != 4 {
		t.Fatalf("event count = %d, want 4 (fill1 + fill2 + brokerage + Release(5))", got)
	}
	mustConvertEvent(t, harness.events.emitted[0])
	mustConvertEvent(t, harness.events.emitted[1])
	brokerage := mustConvertEvent(t, harness.events.emitted[2])
	if brokerage.Cost.Total == 0 {
		t.Errorf("brokerage event has Cost.Total=0, want non-zero (terminal brokerage debit)")
	}
	releaseLeftover := mustReleaseEvent(t, harness.events.emitted[3])
	if releaseLeftover.Cash == 0 {
		t.Errorf("Release(CancelledQty=5) emitted zero cash, want full-drain of ReservedCash")
	}
}

// -----------------------------------------------------------------------------
// Golden 5 — OutcomeMatched / zero-fill SELF-MATCH (deviation from doc).
//
// CONTRACT §9.5 shape: self-match. Trades=[], Rested=nil, CancelledQty=20,
// CancelReason=ReasonSelfMatch.
//
// zero-fill-cancel branch (deviates from doc): Phase-5 doc §3.4 case 1 lumps this with "fills + leftover"
// → Status=Filled. The implemented 5-branch switch puts zero-fill-
// cancel on its own branch → Status=Cancelled. This golden pins the new
// branch.
//
// Expected: Release(CancelledQty=20) returns the full reservation (20 units
// requested, 0 filled, 20 cancelled → release all reserved cash), NO
// Convert (no fills), NO brokerage (EverFilled stays false), NO
// ReleaseExcess (Status is NOT Filled → gate fails). Status=Cancelled,
// track finalized.
func TestSettleGolden_Matched_ZeroFillSelfMatch_StatusCancelled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 20, IsMarket: true,
		BestAsk: 10_500, SlippageBuffer: 0,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, true)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades:       nil,
			Rested:       nil,
			CancelledQty: 20, CancelReason: contract.ReasonSelfMatch,
			LastPrice: 0, LastPriceMoved: false,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	tr, exists := harness.shard.tracks[goldenOrderID]
	if exists {
		t.Errorf("track NOT deleted after zero-fill self-match cancel, want finalize (track Status=%v)", tr.Status)
	}
	// Should see exactly one ReleaseEvent (the full reservation return) and
	// NO ConvertEvents (zero fills, no brokerage), NO ReleaseExcess.
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (Release only; no Convert, no ReleaseExcess)", got)
	}
	release := mustReleaseEvent(t, harness.events.emitted[0])
	if release.Cash == 0 {
		t.Errorf("Release emitted Cash=0, want full reserved cash returned")
	}
}

// -----------------------------------------------------------------------------
// Golden 6 — OutcomeMatched / zero-fill NO-LIQUIDITY (zero-fill-cancel branch).
//
// Market order against an empty opposite book. Trades=[], Rested=nil,
// CancelledQty=qty, CancelReason=ReasonNoLiquidity. Same adapter branch as
// self-match → Status=Cancelled.
func TestSettleGolden_Matched_ZeroFillNoLiquidity_StatusCancelled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: true,
		BestAsk: 10_000, SlippageBuffer: 0,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, true)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			CancelledQty: 10, CancelReason: contract.ReasonNoLiquidity,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted on zero-fill no-liquidity, want finalize")
	}
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (Release only)", got)
	}
	mustReleaseEvent(t, harness.events.emitted[0])
}

// -----------------------------------------------------------------------------
// Golden 7 — OutcomeMatched / zero-fill IOC-LEFTOVER (zero-fill-cancel branch).
//
// IOC order that never crossed. Same adapter branch; Status=Cancelled.
func TestSettleGolden_Matched_ZeroFillIOCLeftover_StatusCancelled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false,
		LimitPrice: 10_000,
	})
	// IOC isn't a market-buy here (limit with TIF=IOC); IsMarketBuy=false.
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			CancelledQty: 10, CancelReason: contract.ReasonIOCLeftover,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted on zero-fill IOC leftover, want finalize")
	}
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (Release only; no ReleaseExcess on non-market-buy)", got)
	}
}

// -----------------------------------------------------------------------------
// Golden 8 — OutcomeMatched / zero-fill CIRCUIT-CLAMP (zero-fill-cancel branch).
//
// Limit price beyond the circuit band → 0 fills, cancelled with
// ReasonCircuitClamp. Same adapter branch; Status=Cancelled.
func TestSettleGolden_Matched_ZeroFillCircuitClamp_StatusCancelled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false,
		LimitPrice: 10_000,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			CancelledQty: 10, CancelReason: contract.ReasonCircuitClamp,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted on zero-fill circuit-clamp, want finalize")
	}
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (Release only)", got)
	}
}

// -----------------------------------------------------------------------------
// Golden 9 — OutcomeMakerFill / incremental (remainder still working).
//
// CONTRACT §9.9. Resting buy at 10021 (100 units reserved as a limit buy)
// gets hit by a taker for 40 units at 10021 (our resting = maker price).
// After the fill, 60 units still rest in the book → settleMakerFill reads
// ViewReservation; RemainingQty > 0 → Status=PartiallyFilled, track
// retained, NO brokerage, NO finalize.
func TestSettleGolden_MakerFill_Incremental_PartiallyFilled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 100, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMakerFill, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{Seq: 108, Instrument: goldenInstrument, Price: 10_021, Qty: 40,
					TakerOrder: "o-T", MakerOrder: goldenOrderID, TakerSide: contract.Sell},
			},
			LastPrice: 10_021, LastPriceMoved: true,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	tr, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("track deleted on incremental maker fill, want retained for 60 remaining")
	}
	if tr.Status != OrderStatusPartiallyFilled {
		t.Errorf("Status = %v, want OrderStatusPartiallyFilled", tr.Status)
	}
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (one fill ConvertEvent, no brokerage yet)", got)
	}
	fill := mustConvertEvent(t, harness.events.emitted[0])
	if fill.UnitsCredited != 40 || fill.CashDebited != 10_021*40 {
		t.Errorf("fill = %+v, want UnitsCredited=40 CashDebited=400840", fill)
	}
}

// -----------------------------------------------------------------------------
// Golden 10 — OutcomeMakerFill / completing fill (RemainingQty → 0).
//
// Same resting order as §9.9, but the taker eats the entire remaining 100.
// After Convert, reservation record is removed by Convert's shared tail
// (RemainingQty==0 && ReservedCash==0 && ReservedUnits==0 for a resting
// limit at the maker price). settleMakerFill then sees ViewReservation
// return !ok → Status=Filled + finalize. Expected reserve-events:
//   Convert (fill 100) → fill ConvertEvent
//   chargeOrderCost (terminal) → brokerage ConvertEvent
func TestSettleGolden_MakerFill_Completing_Filled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 100, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeMakerFill, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{Seq: 200, Instrument: goldenInstrument, Price: 10_021, Qty: 100,
					TakerOrder: "o-T", MakerOrder: goldenOrderID, TakerSide: contract.Sell},
			},
			LastPrice: 10_021, LastPriceMoved: true,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted on maker-fill completing, want finalize")
	}
	if got := len(harness.events.emitted); got != 2 {
		t.Fatalf("event count = %d, want 2 (fill + brokerage)", got)
	}
	mustConvertEvent(t, harness.events.emitted[0])
	brokerage := mustConvertEvent(t, harness.events.emitted[1])
	if brokerage.Cost.Total == 0 {
		t.Errorf("brokerage event Cost.Total=0, want non-zero")
	}
}

// -----------------------------------------------------------------------------
// Golden 11 — OutcomeDiverted (auction ACK, Time 1).
//
// CONTRACT §9.6. Order diverted into the closing auction batch; no fills
// yet, no payload. Expected: markDiverted holds the reservation — ZERO reserve
// calls, Status=Working, track retained for the later auction-clear result.
func TestSettleGolden_Diverted_AuctionACK_HoldsReservation(t *testing.T) {
	harness := newGoldenHarness(t)
	// Position must exist BEFORE the sell reserve runs (reserveSell moves
	// Available=100 → Reserved=100 at admission).
	harness.seedAccount(account.Cash{Available: 10_000_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 100, OpenLots: []account.Lot{{EntryPrice: 10_000, RemainingQty: 100}}},
	})
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Sell, Qty: 100,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeDiverted, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	tr, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("track deleted on Diverted, want retained for auction-clear result")
	}
	if tr.Status != OrderStatusWorking {
		t.Errorf("Status = %v, want OrderStatusWorking (hold)", tr.Status)
	}
	if got := len(harness.events.emitted); got != 0 {
		t.Fatalf("event count = %d, want 0 (zero reserve calls on Diverted)", got)
	}
}

// -----------------------------------------------------------------------------
// Golden 12 — Diverted Time 2 (auction clears via settleMatch branch).
//
// CONTRACT §9.6 Time 2. SAME OrderID, OutcomeMatched, fills at P*=10020,
// 40 cancelled with ReasonAuctionUnfilled. Reuses settleMatch's "fills +
// cancelled leftover" branch (case 4) — identical shape to the funded-cap
// case, different CancelReason. Golden pins the branch reuse.
func TestSettleGolden_DivertedTime2_AuctionClear_RoutesThroughSettleMatch(t *testing.T) {
	harness := newGoldenHarness(t)
	// Pre-seed as if the order had been Diverted (reservation present, Status=Working).
	// Position must exist before the sell reserve.
	harness.seedAccount(account.Cash{Available: 10_000_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 100, OpenLots: []account.Lot{{EntryPrice: 10_000, RemainingQty: 100}}},
	})
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Sell, Qty: 100,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	// Time 2 — the auction clears, SAME OrderID, Outcome=Matched.
	result := contract.EngineResult{
		Outcome: contract.OutcomeMatched, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{Seq: 500, Instrument: goldenInstrument, Price: 10_020, Qty: 60,
					TakerOrder: "o-T", MakerOrder: goldenOrderID, TakerSide: contract.Buy},
			},
			CancelledQty: 40, CancelReason: contract.ReasonAuctionUnfilled,
			LastPrice: 10_020, LastPriceMoved: true,
		},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted after auction clear, want finalize")
	}
	// Expected: Convert×1 (fill 60 at P*) + Convert×1 (brokerage) + Release(40 unfilled).
	// No ReleaseExcess (sell side, IsMarketBuy=false).
	if got := len(harness.events.emitted); got != 3 {
		t.Fatalf("event count = %d, want 3 (fill + brokerage + Release(40))", got)
	}
	fill := mustConvertEvent(t, harness.events.emitted[0])
	if fill.UnitsDelivered != 60 || fill.CashCredited != 10_020*60 {
		t.Errorf("fill = %+v, want UnitsDelivered=60 CashCredited=601200", fill)
	}
}

// -----------------------------------------------------------------------------
// Golden 13 — OutcomeRejected.
//
// CONTRACT §9.4. Admission failed (e.g. off-tick limit) — nothing entered
// the book. Expected: Release(oid, 0) returns the full reservation, NO
// Convert (no fills), Status=Rejected, track finalized.
func TestSettleGolden_Rejected_ReleasesFullReservation(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeRejected, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		Rejection: &contract.Rejection{Reason: contract.RejectBadTick, Stage: "tick_grid"},
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted after Rejected, want finalize")
	}
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (Release-all)", got)
	}
	release := mustReleaseEvent(t, harness.events.emitted[0])
	if release.Cash != 10_021*10 {
		t.Errorf("Release cash = %d, want %d (full reservation)", release.Cash, 10_021*10)
	}
}

// -----------------------------------------------------------------------------
// Golden 14 — OutcomeCancelled / echoed CancelledQty (race discipline).
//
// CONTRACT §9.7. Cancel succeeded; exchange echoes CancelledQty=70 (the
// portion still resting at the moment of the pull — may be less than the
// qty we assumed if a fill beat the cancel). Expected: Release(oid, 70)
// — NEVER release the assumed remainder; always the echoed figure (P5-D18).
// Status=Cancelled, CancelInFlight cleared, track finalized.
func TestSettleGolden_Cancelled_ReleasesEchoedCancelledQty(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 100, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	// Mark CancelInFlight = true to prove settleCancelled clears it.
	harness.shard.tracks[goldenOrderID].CancelInFlight = true
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeCancelled, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		CancelledQty: 70,
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("track NOT deleted after Cancelled, want finalize")
	}
	// One ReleaseEvent with Cash sized to the echoed 70 (partial release
	// of the 100-qty reservation; perQtyCashShare rounds).
	if got := len(harness.events.emitted); got != 1 {
		t.Fatalf("event count = %d, want 1 (Release(70) only)", got)
	}
	release := mustReleaseEvent(t, harness.events.emitted[0])
	if release.Cash == 0 {
		t.Errorf("Release emitted Cash=0, want 70-slice proportional release")
	}
}

// -----------------------------------------------------------------------------
// Golden 15 — OutcomeCancelRejected.
//
// CONTRACT §9.8. Cancel found nothing to remove (order already filled or
// already cancelled). Phase-5 §3.4 case 6 / P5-D21: clear CancelInFlight
// only, NO reserve call, NO finalize, NO MarkSnapshotDirty — channel order
// guarantees the terminating result already finalized the track.
//
// For this golden we pre-set CancelInFlight=true to prove the clear fires.
// The track survives (not finalized by this handler).
func TestSettleGolden_CancelRejected_ClearsFlagOnly(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 100, IsMarket: false,
		LimitPrice: 10_021,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.shard.tracks[goldenOrderID].CancelInFlight = true
	harness.events.reset()

	result := contract.EngineResult{
		Outcome: contract.OutcomeCancelRejected, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
	}

	harness.shard.handleApplyResult(ApplyResult{AccountID: goldenAccountID, Result: result})

	tr, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("track deleted on CancelRejected, want retained (no finalize)")
	}
	if tr.CancelInFlight {
		t.Errorf("CancelInFlight still true after CancelRejected, want cleared")
	}
	if got := len(harness.events.emitted); got != 0 {
		t.Fatalf("event count = %d, want 0 (zero reserve calls on CancelRejected)", got)
	}
}

// -----------------------------------------------------------------------------
// Property — Release completeness (B4).
//
// Rather than an exhaustive rapid generator (deferred to Stage 5), this
// property runs a representative mix of outcomes against fresh harnesses
// and asserts: at the end of each scenario, the sum of released cash +
// converted cash recorded by the reserve service == originally reserved cash. Pins the invariant
// "every non-filled unit is released exactly once" across the branches.
//
// NOTE: this is RED today because handleApplyResult does nothing, so
// released+converted sum == 0, which != reserved. Stage 5 makes it green.
func TestSettleGolden_Property_ReleaseCompletenessAcrossOutcomes(t *testing.T) {
	cases := []struct {
		name    string
		outcome contract.Outcome
		match   *contract.MatchResult
	}{
		{"rejected", contract.OutcomeRejected, nil},
		{"self-match cancelled", contract.OutcomeMatched, &contract.MatchResult{CancelledQty: 10, CancelReason: contract.ReasonSelfMatch}},
		{"full fill", contract.OutcomeMatched, &contract.MatchResult{
			Trades: []contract.Trade{{Seq: 1, Instrument: goldenInstrument, Price: 10_021, Qty: 10, TakerOrder: goldenOrderID, TakerSide: contract.Buy}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newGoldenHarness(t)
			harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
			const reservedCash int64 = 10_021 * 10
			harness.seedReservation(reserve.Order{
				OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
				InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false,
				LimitPrice: 10_021,
			})
			harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
			harness.events.reset()

			rejection := (*contract.Rejection)(nil)
			if tc.outcome == contract.OutcomeRejected {
				rejection = &contract.Rejection{Reason: contract.RejectMalformed}
			}
			harness.shard.handleApplyResult(ApplyResult{
				AccountID: goldenAccountID,
				Result: contract.EngineResult{
					Outcome: tc.outcome, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
					Match: tc.match, Rejection: rejection,
				},
			})

			var converted, released int64
			for _, event := range harness.events.emitted {
				switch typed := event.(type) {
				case reserve.ConvertEvent:
					converted += typed.CashDebited
				case reserve.ReleaseEvent:
					released += typed.Cash
				}
			}
			// Terminal brokerage ConvertEvents carry Cost but not CashDebited,
			// so they don't inflate `converted`. The sum of principal moves
			// out of Reserved == originally reserved cash.
			if converted+released != reservedCash {
				t.Errorf("converted(%d) + released(%d) = %d, want %d (reservedCash)",
					converted, released, converted+released, reservedCash)
			}
		})
	}
}
