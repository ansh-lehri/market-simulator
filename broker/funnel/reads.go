package funnel

import (
	"simulator/economy/account"
	"simulator/economy/realize"
)

// Phase-6 shared read helpers (BROKER_PHASE6 §1.4 / BROKER_READ_SURFACES
// _tier4 §1).
//
// Each helper runs on the OWNING SHARD GOROUTINE — invoked from the shard's
// read handler (handleReadBalance / handleReadPositions / handleReadOpenOrders
// / handleReadObservation; those live in shard_handlers.go, filled by
// Stage 4). The shard is the single writer for its accounts' state
// (sessions, tracks, resv, mktCache), so these reads need no lock and
// return by-value snapshots the caller forwards on the reply channel.
// Nothing outside the shard ever holds a pointer into Account.Cash,
// Account.Positions, OrderTrack, or ReservationRecord — the project's
// message-boundary discipline.
//
// Pure read code: no log/slog, no store writes, no mutation. (The handler
// wrappers in Stage 4 do boundary logging; these are the pure "copy out
// state into view structs" step.)

// readBalance returns the four-bucket Cash copy for an already-resolved
// live session. The caller (handleReadBalance) has established Present:true
// before invoking this helper; the Present field is stamped by the caller
// alongside the fields set here.
func readBalance(session *ownedSession) BalanceSnapshot {
	cash := session.account.Account.Cash
	return BalanceSnapshot{
		Present:   true,
		Available: cash.Available,
		Reserved:  cash.Reserved,
		Settled:   cash.Settled,
		Bank:      cash.Bank,
	}
}

// readPositions walks the live Account's Positions map and projects each
// non-flat holding into a PositionView. instrument == "" means "all held
// instruments"; otherwise exactly the one filter instrument (empty slice
// if the account holds nothing in it). A flat position (Available==0 and
// Reserved==0) is skipped — the view represents live holdings, and a
// zero-row would be noise.
//
// WeightedAvgEntry is qty-weighted over OpenLots (realize.ComputeWeightedAvgEntry),
// zero when flat (the flat-skip above means this returns a value only on
// non-flat rows anyway). MarkAvailable degrades to false when the shard's
// MarketPriceCache misses on LastPrice for (sess.binding.ExchangeID,
// instrument) — the production stand-in nopMarketCache always misses
// until Phase 7 wires a real tape relay (BROKER_PHASE6 X4/X7); on a hit,
// GrossUnrealized is ComputeUnrealizedMark with incurredEntryCosts=0 so
// the "gross" leg is returned (the "net" leg is the Phase-6 deferred gap,
// §3.8 — nothing accumulates per-position entry costs today).
func readPositions(ownerShard *shard, session *ownedSession, instrument InstrumentID) []PositionView {
	acct := session.account.Account
	views := make([]PositionView, 0, len(acct.Positions))
	emit := func(instr InstrumentID, pos *account.Position) {
		if pos.Available == 0 && pos.Reserved == 0 {
			return
		}
		avg, _ := realize.ComputeWeightedAvgEntry(pos.OpenLots)
		view := PositionView{
			Instrument:       instr,
			Qty:              pos.Available,
			ReservedQty:      pos.Reserved,
			WeightedAvgEntry: avg,
		}
		if lastPrice, hit := ownerShard.mktCache.LastPrice(session.binding.ExchangeID, instr); hit {
			gross, _ := realize.ComputeUnrealizedMark(pos.OpenLots, lastPrice, 0)
			view.MarkAvailable = true
			view.LTP = lastPrice
			view.GrossUnrealized = gross
		}
		views = append(views, view)
	}
	if instrument == "" {
		for instr, pos := range acct.Positions {
			emit(instr, pos)
		}
	} else if pos, held := acct.Positions[instrument]; held {
		emit(instrument, pos)
	}
	return views
}

// readOpenOrders projects the shard's open-order tracks for one account
// into OpenOrderView rows. Join of two sources on the shard:
//
//   - shard.tracks            → OrderID, Instrument, Status, CancelInFlight.
//   - resv.ViewReservation    → Side, IsMarket, LimitPrice, OrigQty,
//                               RemainingQty, ReservedCash, ReservedUnits.
//
// tracks is the index — terminal orders are deleted at settlement, so
// everything that survives in tracks is open-set by construction. We
// still filter by AccountID because one shard carries many accounts and
// tracks is keyed by OrderID (per-account filter is O(open orders on
// this shard) — fine for the demo per BROKER_PHASE6 §4.7).
//
// A track whose ReservationView has been removed is unusual (the shard's
// finalize should drop track + reservation together), but we tolerate it:
// the view ships with the track-side fields set and the money/qty/side/
// price fields at their zero values. The slice always slots in a view
// per matching track — the join never silently drops rows.
func readOpenOrders(ownerShard *shard, accountID AccountID) []OpenOrderView {
	orders := make([]OpenOrderView, 0)
	for orderID, track := range ownerShard.tracks {
		if track.AccountID != accountID {
			continue
		}
		view := OpenOrderView{
			OrderID:        orderID,
			Instrument:     track.Instrument,
			Status:         track.Status,
			CancelInFlight: track.CancelInFlight,
		}
		if reservation, held := ownerShard.resv.ViewReservation(orderID); held {
			view.Side = reservation.Side
			view.IsMarket = reservation.IsMarket
			view.LimitPrice = reservation.LimitPrice
			view.OrigQty = reservation.OrigQty
			view.RemainingQty = reservation.RemainingQty
			view.ReservedCash = reservation.ReservedCash
			view.ReservedUnits = reservation.ReservedUnits
		}
		orders = append(orders, view)
	}
	return orders
}
