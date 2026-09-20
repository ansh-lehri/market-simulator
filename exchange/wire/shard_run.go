package wire

import (
	"fmt"
	"log/slog"

	"simulator/contract"
)

// run is the shard's serial loop (EXCHANGE_10 §6). Selects over three
// channels:
//   - inbound: broker submits/cancels from ingress. Close signals shutdown.
//   - control: fan-out messages from the monitor (session transitions today,
//     auction-clear when EXCHANGE_13 lands). Nil during unit tests that
//     don't set it up; a nil channel arm never fires.
//   - ticker:  per-second time-based work (OHLCV rolls). Nil in tests that
//     don't drive time.
//
// Exit condition: inbound closes (ingress-driven shutdown). Any pending
// control or ticker events after that point are dropped — Stage 6's
// shutdown ordering closes the ticker and drains the monitor first so this
// isn't a real hazard in production.
//
// Missing instrument on an InboundMessage is an invariant violation
// (routing bug in ingress) and panics per root CLAUDE.md.
func (shard *Shard) run() {
	slog.Debug("shard.run starting", "shardID", shard.id)
	for {
		select {
		case msg, ok := <-shard.inbound:
			if !ok {
				slog.Debug("shard.run stopped", "shardID", shard.id)
				return
			}
			shard.dispatchInbound(msg)
		case ctrl := <-shard.control:
			shard.handleControl(ctrl)
		case now := <-shard.ticker:
			shard.handleTick(now)
		}
	}
}

func (shard *Shard) dispatchInbound(msg contract.InboundMessage) {
	state, present := shard.instrs[msg.Instrument]
	if !present {
		panic(fmt.Sprintf(
			"wire: shard %d received instrument %q it does not own — ingress routing bug",
			shard.id, msg.Instrument,
		))
	}
	switch msg.Kind {
	case contract.SubmitOrder:
		shard.handleSubmit(state, msg.BrokerID, msg.Submit)
	case contract.CancelOrder:
		shard.handleCancel(state, msg.BrokerID, msg.Cancel)
	default:
		panic(fmt.Sprintf("wire: shard %d received unknown InboundKind %d", shard.id, msg.Kind))
	}
}
