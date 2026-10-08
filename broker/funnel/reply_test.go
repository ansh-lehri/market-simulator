package funnel

// waitReply unit tests. The helper is pure and in-package; no infra,
// no shard, no bundle. Covers the three branches of the select:
//   - reply arrives first   → return the reply's error (nil AND non-nil)
//   - ctx fires first       → return ErrBrokerTimeout
//   - ctx already done      → return ErrBrokerTimeout immediately
//
// A happy integration call (StartSession) exercises the first branch
// end-to-end; this file pins the semantics without needing a Runtime.

import (
	"context"
	"errors"
	"testing"
	"time"

	"simulator/economy/capstone"
)

// Fresh broker helper for in-package waitReply tests. Minimum legal shape:
// one nil-bundle shard, zero-valued MoneyConfig. No shard goroutine is
// started — waitReply does not touch shards, only the reply channel the
// test owns.
func newBrokerForReplyTest() *Broker {
	return NewBroker("B-reply-test", 1 /*inboxDepth*/, []*capstone.Bundle{nil}, nil /*store*/, MoneyConfig{}, nil)
}

// Reply arrives with nil error (the install-success case) → waitReply
// returns nil, no timeout.
func TestWaitReply_NilReply(t *testing.T) {
	broker := newBrokerForReplyTest()
	reply := make(chan error, 1)
	reply <- nil

	gotErr := broker.waitReply(context.Background(), reply)
	if gotErr != nil {
		t.Errorf("waitReply: got %v, want nil", gotErr)
	}
}

// Reply arrives with a non-nil error (the XOR-refusal case) → waitReply
// returns that exact error, verbatim; it never swallows and never
// transforms.
func TestWaitReply_NonNilReply(t *testing.T) {
	broker := newBrokerForReplyTest()
	reply := make(chan error, 1)
	reply <- ErrSessionAlreadyLive

	gotErr := broker.waitReply(context.Background(), reply)
	if !errors.Is(gotErr, ErrSessionAlreadyLive) {
		t.Errorf("waitReply: got %v, want errors.Is ErrSessionAlreadyLive", gotErr)
	}
}

// Ctx cancelled before any reply → ErrBrokerTimeout. Reply channel is
// never written to in this test; proves the ctx branch of the select
// fires without needing the reply branch to be ready.
func TestWaitReply_CtxCancelled_ReturnsBrokerTimeout(t *testing.T) {
	broker := newBrokerForReplyTest()
	reply := make(chan error, 1) // never written to

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before waitReply even enters select

	gotErr := broker.waitReply(ctx, reply)
	if !errors.Is(gotErr, ErrBrokerTimeout) {
		t.Errorf("waitReply: got %v, want errors.Is ErrBrokerTimeout", gotErr)
	}
}

// Ctx deadline fires before a slow reply → ErrBrokerTimeout. The reply
// IS eventually written (after the test's budget), but the select picks
// ctx.Done first. The buffered-1 reply channel absorbs the late send
// without blocking the sender — the Tier-0 §7 contract.
func TestWaitReply_CtxDeadlineBeforeReply_ReturnsBrokerTimeout(t *testing.T) {
	broker := newBrokerForReplyTest()
	reply := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Fire a late reply so the test does not leak a goroutine blocked
	// forever. Buffered-1 means the send never blocks.
	go func() {
		time.Sleep(50 * time.Millisecond)
		reply <- nil
	}()

	gotErr := broker.waitReply(ctx, reply)
	if !errors.Is(gotErr, ErrBrokerTimeout) {
		t.Errorf("waitReply: got %v, want errors.Is ErrBrokerTimeout", gotErr)
	}
}
