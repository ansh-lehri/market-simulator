package wire

import (
	"fmt"
	"log/slog"
	"time"

	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// handleTick runs per-second time-based work for this shard's instruments
// (EXCHANGE_10 §6.3 part b): roll each instrument's OHLCV aggregator when
// its interval has elapsed and emit the resulting candle. Iterates
// instrIDs (sorted) instead of the map so cross-instrument event order on
// the shared public tape is deterministic across runs.
//
// Session-phase detection is NOT here — the monitor goroutine owns
// CheckTransition and reaches shards via handleControl.
func (shard *Shard) handleTick(now time.Time) {
	for _, instrument := range shard.instrIDs {
		state := shard.instrs[instrument]
		if !state.ohlcv.IntervalElapsed(now) {
			continue
		}
		candle := state.ohlcv.Roll(now)
		state.emitter.EmitCandle(candle)
	}
}

// handleControl processes one message from the shard's control channel
// (fan-out target for the monitor). For a session-phase transition, it
// emits the corresponding session event on every instrument's emitter,
// in sorted-instrument order.
//
// The TODO(auction) marker is the extension point where the closing
// auction clear will slot in once EXCHANGE_13 lands. Everything above
// EmitSession there stays; adding auction is a purely additive edit.
func (shard *Shard) handleControl(ctrl controlMessage) {
	switch ctrl.kind {
	case controlSessionTransition:
		kind, mappable := sessionEventKindFor(ctrl.transition.To)
		if !mappable {
			slog.Debug("handleControl: unmappable transition target",
				"shardID", shard.id,
				"to", ctrl.transition.To,
			)
			return
		}
		slog.Info("shard fanning out session event",
			"shardID", shard.id,
			"kind", kind,
			"from", ctrl.transition.From,
			"to", ctrl.transition.To,
			"instrumentCount", len(shard.instrIDs),
		)
		for _, instrument := range shard.instrIDs {
			state := shard.instrs[instrument]
			// TODO(auction): if ctrl.transition.From == PhaseClosingAuction
			// and ctrl.transition.To == PhaseClosed, clear the auction here
			// before EmitSession — call state.auction.Clear, emit trades,
			// ingest into OHLCV, emit per-order EngineResults. See doc §7's
			// onClearAuction for the full sequence.
			state.emitter.EmitSession(kind)
		}
	default:
		panic(fmt.Sprintf("wire: shard %d received unknown controlKind %d", shard.id, ctrl.kind))
	}
}

// sessionEventKindFor maps a phase (the transition's To phase) to the
// public-tape SessionEventKind that means "phase X is now active." A
// phase value outside the enum returns mappable=false so callers can
// decide what to do (handleControl logs and skips).
func sessionEventKindFor(to clock.Phase) (kind emit.SessionEventKind, mappable bool) {
	switch to {
	case clock.PhaseContinuous:
		return emit.SessionOpened, true
	case clock.PhaseClosingAuction:
		return emit.AuctionWindowOpened, true
	case clock.PhaseClosed:
		return emit.SessionClosed, true
	default:
		return 0, false
	}
}
