package funnel

import "context"

// waitReply is BROKER_PHASE2 §3's named reply primitive for commands whose
// shard-side handler reports a single error (InstallSession today;
// subsequent per-session Phase-4 teardown signals when they land). It
// returns the handler's error on reply or ErrBrokerTimeout if the caller's
// context fires first.
//
// Scope: this helper is intentionally specific to `chan error` and takes
// a receive-only channel so a caller cannot accidentally send on the
// shard's reply slot. The five existing typed request/response entry
// points (PlaceOrder, CancelOrder, Transfer, ReadBalance, ReadPositions)
// keep their inlined `select` because their reply channels carry typed
// payloads (PlaceResult / CancelResult / TransferResult / ...);
// refactoring those to a generic helper is a separate increment, not
// part of Phase 2.
//
// Ctx-timeout semantics mirror the inlined pattern: the shard still
// processes the command (already in its inbox) and still sends on the
// caller's buffered-1 reply channel; the value drops into the buffer and
// is garbage-collected when the caller walks away. A timeout is NOT
// "nothing happened" — the install may well have landed; the caller just
// did not stay to see it.
func (broker *Broker) waitReply(ctx context.Context, reply <-chan error) error {
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ErrBrokerTimeout
	}
}
