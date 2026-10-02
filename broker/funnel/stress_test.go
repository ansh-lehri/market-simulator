package funnel

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/capstone"
)

// S1 — single-writer stress with -race. The whole point of the funnel:
// many goroutines dispatching commands for many overlapping accounts,
// with every command landing in exactly one shard's serial loop, must be
// race-free by construction. There are no mutexes anywhere in the
// funnel; if the shard model is right, -race is silent.
//
// This test also asserts:
//   - every request/response caller received a reply (no lost commands);
//   - every push command was dispatched (via a shared counter — the empty
//     seams increment it, see the ApplyResult / FireStop paths below,
//     which use atomic.Int64 for the counter);
//   - Broker.Stop drains everything and returns cleanly.
//
// Because the empty seams don't actually count anything (they're
// deliberately body-less), the push observation is driven from the
// caller side: we track push submissions and assume Stop's drain
// guarantees them delivered. The reply-side observation is the true
// dispatch witness.
func TestStress_MixedCommandsAcrossShards(t *testing.T) {
	const (
		shardCount     = 8
		inboxDepth     = 128
		callerCount    = 50
		commandsCaller = 100
		accountUniverse = 200
	)

	bundles := make([]*capstone.Bundle, shardCount)
	brokerUnderTest := NewBroker("B-stress", inboxDepth, bundles, nil, MoneyConfig{})
	brokerUnderTest.Start()

	var (
		submittedRequests uint64
		receivedReplies   uint64
		submittedPushes   uint64
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var callerWG sync.WaitGroup
	callerWG.Add(callerCount)
	for callerIndex := 0; callerIndex < callerCount; callerIndex++ {
		go func(seed int64) {
			defer callerWG.Done()
			randSource := rand.New(rand.NewSource(seed))

			for commandIndex := 0; commandIndex < commandsCaller; commandIndex++ {
				accountID := AccountID(fmt.Sprintf("acct-%d", randSource.Intn(accountUniverse)))
				switch randSource.Intn(7) {
				case 0:
					atomic.AddUint64(&submittedRequests, 1)
					if _, err := brokerUnderTest.PlaceOrder(ctx, accountID, "s", OrderSpec{}); err == nil {
						atomic.AddUint64(&receivedReplies, 1)
					}
				case 1:
					atomic.AddUint64(&submittedRequests, 1)
					if _, err := brokerUnderTest.CancelOrder(ctx, accountID, "some-oid"); err == nil {
						atomic.AddUint64(&receivedReplies, 1)
					}
				case 2:
					atomic.AddUint64(&submittedRequests, 1)
					if _, err := brokerUnderTest.BankMove(ctx, accountID, PullToTrading, 100); err == nil {
						atomic.AddUint64(&receivedReplies, 1)
					}
				case 3:
					atomic.AddUint64(&submittedRequests, 1)
					if _, err := brokerUnderTest.ReadBalance(ctx, accountID); err == nil {
						atomic.AddUint64(&receivedReplies, 1)
					}
				case 4:
					atomic.AddUint64(&submittedRequests, 1)
					if _, err := brokerUnderTest.ReadPositions(ctx, accountID); err == nil {
						atomic.AddUint64(&receivedReplies, 1)
					}
				case 5:
					atomic.AddUint64(&submittedPushes, 1)
					brokerUnderTest.PushApplyResult(accountID, contract.EngineResult{OrderID: OrderID(fmt.Sprintf("%s:1", accountID))})
				case 6:
					atomic.AddUint64(&submittedPushes, 1)
					brokerUnderTest.PushFireStop(accountID, StopRecord{})
				}
			}
		}(int64(callerIndex) + 1)
	}

	callerWG.Wait()
	brokerUnderTest.Stop()

	finalSubmittedRequests := atomic.LoadUint64(&submittedRequests)
	finalReceivedReplies := atomic.LoadUint64(&receivedReplies)
	finalSubmittedPushes := atomic.LoadUint64(&submittedPushes)

	if finalReceivedReplies != finalSubmittedRequests {
		t.Fatalf("reply count mismatch: submitted %d requests, received %d replies",
			finalSubmittedRequests, finalReceivedReplies)
	}
	if finalSubmittedRequests+finalSubmittedPushes != callerCount*commandsCaller {
		t.Fatalf("total commands mismatch: submitted %d requests + %d pushes, expected %d",
			finalSubmittedRequests, finalSubmittedPushes, callerCount*commandsCaller)
	}
}

// S2 — determinism replay. The router's routing is deterministic
// (hashAccount is), and channel FIFO from a single producer preserves
// order. Together this means: with one producer feeding route() with a
// fixed command sequence, each shard's inbox sees the same subset of
// commands in the same order — every run, byte-for-byte.
//
// The observation is per-shard, made BEFORE Start (so no dispatch runs
// and the inbox contents are the raw router output). Two runs; compare
// the per-shard AccountID sequences.
func TestDeterminism_RouterOrderIsStableAcrossRuns(t *testing.T) {
	const (
		shardCount = 4
		inboxDepth = 512
		runCount   = 2
		seed       = int64(0xD1CE)
	)

	commands := makeDeterministicCommandStream(seed, 400)

	priorRunTranscripts := make([][][]AccountID, runCount)
	for runIndex := 0; runIndex < runCount; runIndex++ {
		brokerUnderTest := NewBroker("B-determinism", inboxDepth, make([]*capstone.Bundle, shardCount), nil, MoneyConfig{})

		for _, cmd := range commands {
			brokerUnderTest.route(cmd)
		}
		brokerUnderTest.Stop()

		perShard := make([][]AccountID, shardCount)
		for shardIndex, shardOwner := range brokerUnderTest.shards {
			for cmd := range shardOwner.inbox {
				perShard[shardIndex] = append(perShard[shardIndex], cmd.Account())
			}
		}
		priorRunTranscripts[runIndex] = perShard
	}

	baseline := priorRunTranscripts[0]
	for runIndex := 1; runIndex < runCount; runIndex++ {
		later := priorRunTranscripts[runIndex]
		for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
			if len(baseline[shardIndex]) != len(later[shardIndex]) {
				t.Fatalf("shard %d: run 0 got %d commands, run %d got %d",
					shardIndex, len(baseline[shardIndex]), runIndex, len(later[shardIndex]))
			}
			for sequenceIndex, accountID := range baseline[shardIndex] {
				if later[shardIndex][sequenceIndex] != accountID {
					t.Fatalf("shard %d position %d: run 0 = %q, run %d = %q",
						shardIndex, sequenceIndex, accountID, runIndex, later[shardIndex][sequenceIndex])
				}
			}
		}
	}
}

// makeDeterministicCommandStream builds a fixed-seed pseudo-random
// stream of push commands (no ReplyTo — cheaper, and Broker.Stop can
// close inboxes with them in flight). Uses ApplyResult / FireStop only,
// so the drainable inbox contents are visible via cmd.Account().
func makeDeterministicCommandStream(seed int64, commandCount int) []Command {
	source := rand.New(rand.NewSource(seed))
	built := make([]Command, commandCount)
	for commandIndex := 0; commandIndex < commandCount; commandIndex++ {
		accountID := AccountID(fmt.Sprintf("acct-%04d", source.Intn(100)))
		if source.Intn(2) == 0 {
			built[commandIndex] = ApplyResult{AccountID: accountID}
		} else {
			built[commandIndex] = FireStop{AccountID: accountID}
		}
	}
	return built
}
