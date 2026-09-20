package wire

import (
	"fmt"
	"log/slog"

	"simulator/contract"
)

// run is the shard's serial loop (EXCHANGE_10 §6). It reads its inbound
// channel, looks up the target instrument (guaranteed present because
// ingress routes by hash(Instrument)%numShards to the shard that owns the
// map entry), and dispatches on Kind. Exits when inbound closes — the
// signal ingress sends on session shutdown.
//
// Missing instrument is an invariant violation (routing bug in ingress)
// and panics per root CLAUDE.md. Cancel is a Stage 4 scope guard: the
// panic makes the missing piece loud in tests rather than silently
// no-op'ing the message.
func (shard *Shard) run() {
	slog.Debug("shard.run starting", "shardID", shard.id)
	for msg := range shard.inbound {
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
	slog.Debug("shard.run stopped", "shardID", shard.id)
}
