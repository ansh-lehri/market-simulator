package funnel

import (
	"strings"
	"testing"

	"simulator/economy/account"
)

// Dispatch coverage — every Tier-0 command type must land in its own
// case of the type switch. Request/response cases send the handler's
// return on the command's ReplyTo channel (BROKER_TIER0 §Point-4);
// push cases run and return nothing. An unknown Command panics
// (programming error, not runtime condition).
//
// Because the empty-seam handlers return zero-value results in Phase 0,
// the reply-happy assertions below only check that a value LANDED on
// the reply channel — they do not (and cannot) assert business content
// that has not been implemented yet.

func TestShardDispatch_PlaceOrderSendsReply(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	replyChannel := make(chan PlaceResult, 1)

	dispatchedShard.dispatch(PlaceOrder{AccountID: "acct-a", ReplyTo: replyChannel})

	select {
	case <-replyChannel:
	default:
		t.Fatalf("PlaceOrder: dispatch did not send on ReplyTo")
	}
}

func TestShardDispatch_CancelOrderSendsReply(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	replyChannel := make(chan CancelResult, 1)

	dispatchedShard.dispatch(CancelOrder{AccountID: "acct-a", OrderID: "acct-a:1", ReplyTo: replyChannel})

	select {
	case <-replyChannel:
	default:
		t.Fatalf("CancelOrder: dispatch did not send on ReplyTo")
	}
}

func TestShardDispatch_TransferSendsReply(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	replyChannel := make(chan TransferResult, 1)

	dispatchedShard.dispatch(Transfer{
		AccountID: "acct-a",
		From:      account.Bank,
		To:        account.Available,
		Amount:    100,
		ReplyTo:   replyChannel,
	})

	select {
	case <-replyChannel:
	default:
		t.Fatalf("Transfer: dispatch did not send on ReplyTo")
	}
}

func TestShardDispatch_ReadBalanceSendsReply(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	replyChannel := make(chan BalanceSnapshot, 1)

	dispatchedShard.dispatch(ReadBalance{AccountID: "acct-a", ReplyTo: replyChannel})

	select {
	case <-replyChannel:
	default:
		t.Fatalf("ReadBalance: dispatch did not send on ReplyTo")
	}
}

func TestShardDispatch_ReadPositionsSendsReply(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	replyChannel := make(chan PositionsSnapshot, 1)

	dispatchedShard.dispatch(ReadPositions{AccountID: "acct-a", ReplyTo: replyChannel})

	select {
	case <-replyChannel:
	default:
		t.Fatalf("ReadPositions: dispatch did not send on ReplyTo")
	}
}

// Push commands: dispatch must run the handler without panicking; there
// is no reply channel to observe. A separate observation (e.g. the
// stress test) proves that dispatch actually reached the shard.
func TestShardDispatch_ApplyResultRunsWithoutPanic(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	dispatchedShard.dispatch(ApplyResult{AccountID: "acct-a"})
}

func TestShardDispatch_FireStopRunsWithoutPanic(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)
	dispatchedShard.dispatch(FireStop{AccountID: "acct-a"})
}

// unknownTestCommand is a test-only Command type that dispatch has no
// case for — used to drive the default-panic path.
type unknownTestCommand struct{ accountID AccountID }

func (c unknownTestCommand) Account() AccountID { return c.accountID }

func TestShardDispatch_UnknownCommandPanics(t *testing.T) {
	dispatchedShard := newShard("B-dispatch", 4, nil)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic on unknown command type; got none")
		}
		panicMessage, ok := recovered.(string)
		if !ok {
			t.Fatalf("panic value is not a string: %T (%v)", recovered, recovered)
		}
		if !strings.Contains(panicMessage, "unknown command") {
			t.Fatalf("panic message missing 'unknown command': %q", panicMessage)
		}
		if !strings.Contains(panicMessage, "unknownTestCommand") {
			t.Fatalf("panic message missing type name 'unknownTestCommand': %q", panicMessage)
		}
	}()

	dispatchedShard.dispatch(unknownTestCommand{accountID: "acct-a"})
}
