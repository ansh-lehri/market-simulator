package funnel

import (
	"simulator/economy/account"
	"simulator/economy/realize"
)

// Shared read helpers for the live-read handlers. Each runs on the
// owning shard goroutine (invoked from the shard's handleReadX wrapper
// — the one in shard_handlers.go that does the liveness check). The
// shard is the single writer for its accounts' state (sessions,
// tracks, resv, mktCache), so these helpers need no lock and return
// by-value snapshots the caller forwards on the reply channel.
// Nothing outside the shard ever holds a pointer into Cash, Positions,
// OrderTrack, or a reservation record.
//
// Pure read code: no logging, no store writes, no mutation. The
// boundary logs live on the handler wrappers one layer up.

// readBalance copies the four Cash buckets off an already-resolved
// live session. The caller has already established Present:true; this
// helper stamps it on the snapshot alongside the fields set here.
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

// readPositions walks the live Account's positions and projects each
// non-flat holding into a PositionView. instrument == "" means every
// held instrument; a specific instrument the account doesn't hold
// returns an empty slice (the handler surfaces that as a live 200,
// not a 404).
//
// Flat positions (Available and Reserved both zero) are skipped so a
// stale map entry doesn't appear as an empty row in the response.
// WeightedAvgEntry comes from a qty-weighted sum over the open lots.
// The mark fields degrade honestly: when the market-price cache
// misses on LastPrice (the production stand-in misses on everything
// until the tape relay goes live), MarkAvailable stays false and LTP
// / GrossUnrealized stay zero — the position, reserved qty, and
// weighted-avg entry still ship.
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

// readOpenOrders projects the shard's open-order tracks for one
// account into OpenOrderView rows. Join of two sources on the shard:
//
//   - the tracks map              → OrderID, Instrument, Status,
//                                   CancelInFlight;
//   - the reservation book's view → Side, IsMarket, LimitPrice,
//                                   OrigQty, RemainingQty,
//                                   ReservedCash, ReservedUnits.
//
// Tracks is the index — terminal orders are deleted at settlement, so
// anything still in tracks is open-set by construction. We filter by
// AccountID because one shard carries many accounts and the tracks
// map is keyed by OrderID (per-account scan is O(open orders on this
// shard) — fine at the scale of a demo).
//
// A track whose reservation view has been removed is unusual (the
// shard's finalize should drop both together), but we tolerate it:
// the view ships with the track-side fields set and the money / qty
// / side / price fields at their zero values. The slice always slots
// in a view per matching track — the join never silently drops rows.
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
