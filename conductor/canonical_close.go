package conductor

import (
	"context"

	"simulator/broker/funnel"
)

// closeCanonicalMarket is the per-market scheduled-close procedure the
// timer fires at Market.closeAt. One function per canonical market
// lifecycle; sibling of spinUpCanonical.
//
// Flow — the one registry mutex is held ONLY for the two microsecond
// regions (guard flip + reap); every slow call-out runs OFF-LOCK
// (broker.FinalizeMarket, agents.WindDown, exchange.Stop). This is the
// load-bearing concurrency discipline — a lock held across any of those
// three would freeze the whole registry for the barrier's duration.
//
//  1. GUARD + SNAPSHOT (lock, µs):
//     - look up the market; nil or State != Live → return (run-once
//       guard: a second trigger finds nothing to do).
//     - flip State Live → TearingDown. From this instant,
//       findLiveCanonical skips the market (joins land as 409) and
//       fanoutForSession returns nil for its sessions (WebSocket
//       connects land as 404). No new attachment is possible.
//     - snapshot members, exchangeID, tag, exchange handle.
//     - unlock.
//
//  2. OFF-LOCK barrier + wind-down + stop:
//     - broker.FinalizeMarket(ctx, exchangeID): the broker fans out to
//       every shard, persists every account bound to this exchangeID
//       (both attached members AND users who left earlier via explicit
//       Leave — the broker's exchange→accounts index spans both),
//       uninstalls, unloads. Synchronous barrier.
//     - If FinalizeMarket errors: LOG + PROCEED. Forward-progress is the
//       contract — the conductor would rather close a market with some
//       accounts unpersisted (the Redis snapshot survives per the
//       don't-discard-on-fail rule) than hang the close.
//     - agents.WindDown(tag, Canonical): fire-and-forget signal to the
//       agent-orchestrator layer. The conductor does not wait on it.
//     - exchange.Stop(): the shipped exchange handle tears down its
//       inbound → shards drain → tape closes → market-data self-cleans.
//       Nothing in L7 unwires market-data (Model A: the browser's
//       socket close + the shipped market-data Disconnect own the
//       feed-side cleanup).
//
//  3. REAP (lock, µs):
//     - delete every member session + its byUser slot (users who were
//       still attached at close).
//     - delete the market record.
//     - users who LEFT before close are NOT in members and are NOT
//       touched here — the broker's FinalizeMarket swept them in step 2
//       via the exchange→accounts index.
//
// After reap: everything about this market is gone from the registry.
// The next weekday's timer loop re-arms spin-up and the cycle repeats.
func (conductor *Conductor) closeCanonicalMarket(ctx context.Context, marketID MarketID) {
	// ---- GUARD + SNAPSHOT (lock take #1 — microseconds) ----
	conductor.reg.mu.Lock()
	market := conductor.reg.markets[marketID]
	if market == nil || market.State != Live {
		var observedState MarketState
		if market != nil {
			observedState = market.State
		}
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: closeCanonicalMarket skipped — market not Live",
			"market_id", marketID,
			"state", observedState,
			"market_present", market != nil,
		)
		return
	}
	market.State = TearingDown
	memberSnapshot := make([]SessionID, 0, len(market.members))
	for sessionID := range market.members {
		memberSnapshot = append(memberSnapshot, sessionID)
	}
	exchangeID := funnel.ExchangeID(marketID)
	marketTag := market.Tag
	exchange := market.exch
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: closeCanonicalMarket flipped to TearingDown",
		"market_id", marketID,
		"tag", marketTag,
		"member_count", len(memberSnapshot),
	)

	// ---- OFF-LOCK: broker barrier (forward-progress on error) ----
	if finalizeErr := conductor.broker.FinalizeMarket(ctx, exchangeID); finalizeErr != nil {
		conductor.log.Error("conductor: closeCanonicalMarket FinalizeMarket incomplete; proceeding to wind-down + stop",
			"market_id", marketID,
			"exchange_id", exchangeID,
			"err", finalizeErr,
		)
	} else {
		conductor.log.Info("conductor: closeCanonicalMarket FinalizeMarket complete",
			"market_id", marketID,
			"exchange_id", exchangeID,
		)
	}

	// ---- OFF-LOCK: agent wind-down (fire-and-forget) ----
	conductor.agents.WindDown(marketTag, Canonical)

	// ---- OFF-LOCK: exchange stop ----
	// The shipped exchange.Stop() is idempotent and safe without a
	// prior Start (no-op in that case). Nothing to guard here.
	if exchange != nil {
		exchange.Stop()
	}

	// ---- REAP (lock take #2 — microseconds) ----
	conductor.reg.mu.Lock()
	for _, sessionID := range memberSnapshot {
		memberSession := conductor.reg.sessions[sessionID]
		if memberSession == nil {
			continue
		}
		delete(conductor.reg.sessions, sessionID)
		if conductor.reg.byUser[memberSession.User] == memberSession {
			delete(conductor.reg.byUser, memberSession.User)
		}
	}
	delete(conductor.reg.markets, marketID)
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: closeCanonicalMarket reap complete",
		"market_id", marketID,
		"tag", marketTag,
		"reaped_sessions", len(memberSnapshot),
	)
}
