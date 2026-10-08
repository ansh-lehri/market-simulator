package funnel

import (
	"errors"
	"fmt"
	"log/slog"

	"simulator/contract"
	"simulator/economy/reserve"
)

// handlePlaceOrder is the order-intake body
// (BROKER_PHASE5_IMPLEMENTATION_DESIGN.md §1.3). It runs on the owner
// goroutine, so every state touch (sessions / tracks / resv / seq /
// mktCache / exchange edge) is single-writer safe without a lock.
//
// Order of operations is load-bearing: session → validateSpec → market-buy
// price → nextOrderID → reserve → track → punch. Any pre-punch failure
// returns PlaceRejected and PUNCHES NOTHING; the exchange never sees a
// half-funded order and the broker never carries a dangling track.
//
// PlaceOrder.SessionID (a Tier-0 field) is ignored here — the shard
// resolves the live session via sessions[AccountID] so a client cannot
// assert a wrong session. The real SessionID crosses into reserve.Order
// via sess.binding.SessionID, where it was authoritatively bound at
// session-install time.
func (ownerShard *shard) handlePlaceOrder(cmd PlaceOrder) PlaceResult {
	slog.Debug("funnel: handlePlaceOrder begin",
		"account_id", cmd.AccountID,
		"instrument", cmd.Order.Instrument,
		"side", cmd.Order.Side,
		"type", cmd.Order.Type,
		"qty", cmd.Order.Qty,
		"limit_price", cmd.Order.LimitPrice,
	)

	liveSession, isLive := ownerShard.sessions[cmd.AccountID]
	if !isLive {
		slog.Info("funnel: handlePlaceOrder rejected — no session",
			"account_id", cmd.AccountID,
		)
		return PlaceResult{Status: PlaceRejected, Reason: ReasonNoSession}
	}

	order := cmd.Order

	// STEP 1 — structural validation. Pre-pricing, pre-reserve: a bad spec
	// never calls the reservation service and never bumps the OrderID
	// counter.
	if reason := ownerShard.validateSpec(liveSession, order); reason != ReasonOK {
		slog.Warn("funnel: handlePlaceOrder rejected — invalid spec",
			"account_id", cmd.AccountID,
			"instrument", order.Instrument,
			"reason", reason,
		)
		return PlaceResult{Status: PlaceRejected, Reason: reason}
	}

	// STEP 2 — market-buy ONLY needs a cached price to size requiredCash +
	// MaxSpend (P5-D3). The invariant maxSpend == requiredCash is enforced
	// by using the SAME (bestAsk, buffer) pair for both: buildReserveOrder
	// passes them into reserve.Order (where the reservation service
	// computes requiredCash internally) and maxSpend() stamps the
	// FundedOrder cap. A cold cache → honest reject; no synthetic price,
	// no fallback.
	var bestAsk contract.Ticks
	var buffer int64
	isMarketBuy := order.Side == contract.Buy && order.Type == contract.Market
	if isMarketBuy {
		cachedBestAsk, hit := ownerShard.mktCache.BestAsk(liveSession.binding.ExchangeID, order.Instrument)
		if !hit {
			slog.Warn("funnel: handlePlaceOrder rejected — no market price",
				"account_id", cmd.AccountID,
				"instrument", order.Instrument,
			)
			return PlaceResult{Status: PlaceRejected, Reason: ReasonNoMarketPrice}
		}
		bestAsk = cachedBestAsk
		buffer = slippageBuffer(bestAsk, order.Qty, ownerShard.slippageBufferBps)
	}

	// STEP 3 — mint the OrderID. Shape "<AccountID>:<seq>" so settlement's
	// accountOf can parse the owner out of the echoed id. Any reserve
	// failure past this point simply "wastes" one seq value — a gap in
	// the sequence, harmless, no cleanup needed.
	orderID := ownerShard.nextOrderID(cmd.AccountID)

	// STEP 4 — Gate 4. ReserveIfAvailable returns a typed sentinel on
	// reject (P5-D2); reserveReason maps it to a funnel RejectReason via
	// errors.Is. On error, no record exists in the book (reserve
	// fails-before-mutate), so there is nothing to clean up.
	err := ownerShard.resv.ReserveIfAvailable(
		reserve.Participant{ID: liveSession.participant},
		buildReserveOrder(order, orderID, cmd.AccountID, liveSession.binding.SessionID, bestAsk, buffer),
	)
	if err != nil {
		reason := reserveReason(err)
		slog.Warn("funnel: handlePlaceOrder rejected — reserve failed",
			"account_id", cmd.AccountID,
			"order_id", orderID,
			"err", err,
			"reason", reason,
		)
		return PlaceResult{Status: PlaceRejected, Reason: reason}
	}

	// STEP 5 — thin OrderTrack (two-record division). No qty, no money,
	// no reservation state — those live on the ReservationRecord in the
	// reserve package and are read back via ViewReservation when
	// settlement needs them.
	ownerShard.tracks[orderID] = &OrderTrack{
		OrderID:     orderID,
		AccountID:   cmd.AccountID,
		Instrument:  order.Instrument,
		Status:      OrderStatusWorking,
		IsMarketBuy: isMarketBuy,
	}

	// STEP 6 — punch the FundedOrder onto the exchange edge. s.punch stamps
	// BrokerID and does the (unbuffered) blocking send. STP: CancelIncoming
	// is stamped on every submit (P5-D6, the real-exchange default).
	// MaxSpend uses the SAME (bestAsk, buffer) pair used in STEP 4 — this
	// is the "reserved == MaxSpend by construction" invariant P5-D3 names.
	ownerShard.punch(liveSession, contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		Instrument: order.Instrument,
		Submit: &contract.FundedOrder{
			OrderID:     orderID,
			Participant: liveSession.participant,
			Instrument:  order.Instrument,
			Side:        order.Side,
			Type:        order.Type,
			TIF:         tifFor(order.Type),
			Qty:         order.Qty,
			LimitPrice:  order.LimitPrice,
			MaxSpend:    maxSpend(bestAsk, order.Qty, buffer),
			STP:         contract.CancelIncoming,
		},
	})

	slog.Info("funnel: handlePlaceOrder accepted",
		"account_id", cmd.AccountID,
		"order_id", orderID,
		"instrument", order.Instrument,
		"side", order.Side,
		"type", order.Type,
		"qty", order.Qty,
		"is_market_buy", isMarketBuy,
	)
	return PlaceResult{Status: PlaceAccepted, OrderID: orderID}
}

// validateSpec is the structural-only check that runs before pricing and
// before Gate 4 (BROKER_PHASE5 §1.4). It owns these rejections:
//
//   - ReasonUnknownInstrument — session allowlist miss (via knownInstrument)
//   - ReasonBadQty            — Qty <= 0
//   - ReasonBadSide           — Side not in {Buy, Sell}
//   - ReasonBadPrice          — Limit with LimitPrice <= 0
//   - ReasonBadType           — Type not in {Limit, Market}
//
// The switch order matches the Phase-5 doc verbatim so a diagnostic log
// line is deterministic across versions: whichever is the first failing
// predicate is the reason returned.
//
// What validateSpec does NOT check: TIF validity (the exchange rejects a
// malformed TIF at admission), MaxSpend (broker-computed, never
// user-supplied), self-match policy (zero value is CancelIncoming per
// contract §3.2).
func (ownerShard *shard) validateSpec(session *ownedSession, order OrderSpec) RejectReason {
	switch {
	case !session.knownInstrument(order.Instrument):
		return ReasonUnknownInstrument
	case order.Qty <= 0:
		return ReasonBadQty
	case order.Side != contract.Buy && order.Side != contract.Sell:
		return ReasonBadSide
	case order.Type == contract.Limit && order.LimitPrice <= 0:
		return ReasonBadPrice
	case order.Type != contract.Limit && order.Type != contract.Market:
		return ReasonBadType
	}
	return ReasonOK
}

// reserveReason maps a reserve.ReserveIfAvailable error to a funnel
// RejectReason. The reserve package exports exactly two typed sentinels at
// Gate 4 (ErrInsufficientFunds / ErrInsufficientPosition —
// economy/reserve/types.go); funnel collapses BOTH into
// ReasonInsufficientFunds per P5-D28 (one reason, one 422 — the broker
// does not split funds-vs-units at the HTTP surface). Anything else from
// reserve is a programming error and is surfaced as a panic rather than
// silently absorbed — root policy on invariant violations.
func reserveReason(err error) RejectReason {
	switch {
	case errors.Is(err, reserve.ErrInsufficientFunds), errors.Is(err, reserve.ErrInsufficientPosition):
		return ReasonInsufficientFunds
	}
	panic(fmt.Sprintf("funnel: reserveReason: unexpected reserve error %v (reserve exports only ErrInsufficientFunds / ErrInsufficientPosition)", err))
}

// buildReserveOrder projects the broker-internal OrderSpec into the
// reserve.Order shape consumed by ReserveIfAvailable. Carries ids by
// value (never a pointer into shard state) — the admission data crosses
// the broker↔reserve seam as a plain value.
//
// IsMarket is derived from Type here so the reserve package does not
// depend on contract's OrdType enum; BestAsk/SlippageBuffer are 0 for
// limits and sells (the reserve package ignores them, per reserve.go's
// reserveBuy/reserveSell branches).
func buildReserveOrder(order OrderSpec, orderID OrderID, accountID AccountID, sessionID reserve.SessionID, bestAsk contract.Ticks, buffer int64) reserve.Order {
	return reserve.Order{
		OrderID:        orderID,
		AccountID:      accountID,
		SessionID:      sessionID,
		InstrumentID:   order.Instrument,
		Side:           order.Side,
		Qty:            order.Qty,
		IsMarket:       order.Type == contract.Market,
		LimitPrice:     order.LimitPrice,
		BestAsk:        bestAsk,
		SlippageBuffer: buffer,
	}
}

// tifFor derives the FundedOrder.TIF value from the OrderSpec.Type (per
// Phase-5 doc §1.3 inline helper): Limit→GTC (rests), Market→IOC (never
// rests). FOK is deferred (contract §3.2 note).
//
// Pure function; no I/O, no logger. A future per-order TIF selector (user-
// supplied GTC vs IOC on a limit order) would replace this with an
// OrderSpec.TIF field; today's prototype uses the type-implied default.
func tifFor(ordType contract.OrdType) contract.TIF {
	if ordType == contract.Limit {
		return contract.GTC
	}
	return contract.IOC
}
