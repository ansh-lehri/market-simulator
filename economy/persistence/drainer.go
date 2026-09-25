package persistence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// drainer is the async worker for the closed-trade write-behind buffer
// (LAYER_5_PERSISTENCE §5). On each tick it RPOPs up to cfg.DrainerBatch
// rows and calls store.insertClosedTrade for each. A row whose Postgres
// insert fails is RPUSHed back onto the buffer's tail (requeueClosedTrade)
// so it retries first on the next drain — the row never leaves the buffer
// without landing durably in Postgres.
//
// Locked decisions this worker follows (§8):
//   - D4: buffer is one Redis LIST, single drainer.
//   - D5: tick + max batch from Config; failed row stays in buffer;
//         idempotent replay via ON CONFLICT (trade_id) DO NOTHING;
//         shutdown drains what is cheaply drainable then exits — anything
//         left rides the buffer to the next boot.
//
// The drainer is the ONLY goroutine this package owns. Snapshotting stays
// synchronous / owner-driven in Phase 2 (per LAYER_5_PERSISTENCE §5 and
// this package's CLAUDE.md).
type drainer struct {
	store *store
	cfg   Config

	// runtime state — mutated by Start/Stop only; the goroutine reads
	// nothing here after Start returns. runMu guards the pair (running,
	// stopCh, doneCh) against a concurrent Start/Stop, so double-start
	// returns an error and double-stop is safe.
	runMu   sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// newDrainer builds a drainer bound to a store. The store's redis client
// and Postgres pool MUST be non-nil for Start to succeed at runtime — a
// nil store here is tolerated only to construct a Component in tests that
// never call Start (e.g. seam-only tests).
func newDrainer(component *store, cfg Config) *drainer {
	return &drainer{store: component, cfg: cfg}
}

// Start launches the drainer goroutine. ctx bounds the worker's lifetime:
// on ctx cancellation the goroutine exits cleanly (same path as Stop).
// Calling Start twice without an intervening Stop returns an error — the
// worker is a singleton per Component.
//
// The goroutine loop:
//  1. Tick every cfg.DrainerTick (default 100ms), plus an immediate first
//     drain to flush any rows that piled up before Start.
//  2. Each tick calls drainOnce; on repeated failures, apply exponential
//     backoff so a struggling Postgres is not hammered.
//  3. Exit when either stopCh or ctx.Done fires. Best-effort final drain
//     on shutdown (D5: "drain what is cheaply drainable, then exit").
func (worker *drainer) Start(ctx context.Context) error {
	worker.runMu.Lock()
	defer worker.runMu.Unlock()

	if worker.running {
		return errors.New("persistence: drainer already started")
	}
	worker.stopCh = make(chan struct{})
	worker.doneCh = make(chan struct{})
	worker.running = true

	slog.Info("persistence: drainer starting",
		"tick", worker.cfg.DrainerTick,
		"batch", worker.cfg.DrainerBatch,
	)
	go worker.run(ctx)
	return nil
}

// Stop signals the drainer goroutine to exit and waits for it to finish
// its current tick. Idempotent — calling Stop on a not-running or
// already-stopped drainer is a no-op. Safe to call from any goroutine.
//
// Rows that remain in Redis after Stop are LEFT there for the next boot
// (D5). The buffer is durable, so nothing is lost.
func (worker *drainer) Stop() {
	worker.runMu.Lock()
	if !worker.running {
		worker.runMu.Unlock()
		return
	}
	worker.running = false
	stopCh := worker.stopCh
	doneCh := worker.doneCh
	worker.runMu.Unlock()

	close(stopCh)
	<-doneCh
	slog.Info("persistence: drainer stopped")
}

// run is the drainer goroutine's whole body. Extracted so Start stays a
// pure launcher.
func (worker *drainer) run(ctx context.Context) {
	defer close(worker.doneCh)

	ticker := time.NewTicker(worker.cfg.DrainerTick)
	defer ticker.Stop()

	// Consecutive-failure counter drives exponential backoff between
	// ticks. Reset on any successful tick (no per-row error).
	consecutiveFailures := 0

	// Immediate first drain: rows enqueued before Start should not wait
	// a whole tick to land.
	worker.tick(ctx, &consecutiveFailures)

	for {
		waitDuration := worker.cfg.DrainerTick
		if consecutiveFailures > 0 {
			waitDuration = backoffDelay(worker.cfg.DrainerTick, consecutiveFailures)
		}
		select {
		case <-ctx.Done():
			worker.finalDrain(context.Background())
			return
		case <-worker.stopCh:
			worker.finalDrain(context.Background())
			return
		case <-time.After(waitDuration):
			worker.tick(ctx, &consecutiveFailures)
		}
	}
}

// tick executes one drain cycle and updates the consecutive-failure count.
// Any per-row insert error keeps consecutiveFailures growing; a fully
// clean cycle resets it to zero.
func (worker *drainer) tick(ctx context.Context, consecutiveFailures *int) {
	failures, err := worker.drainOnce(ctx)
	if err != nil {
		*consecutiveFailures++
		slog.Error("persistence: drainer tick error",
			"err", err,
			"consecutive_failures", *consecutiveFailures,
		)
		return
	}
	if failures > 0 {
		*consecutiveFailures++
		slog.Warn("persistence: drainer tick had per-row failures",
			"failures", failures,
			"consecutive_failures", *consecutiveFailures,
		)
		return
	}
	if *consecutiveFailures != 0 {
		slog.Info("persistence: drainer tick recovered", "prior_failures", *consecutiveFailures)
	}
	*consecutiveFailures = 0
}

// drainOnce pops up to cfg.DrainerBatch rows and inserts each into
// Postgres. Any row whose insert fails is RPUSHed back onto the buffer
// tail (retries first on the next RPOP). Returns (perRowFailures,
// batchError) — a non-nil batchError means the RPOP itself failed and
// no rows were processed; perRowFailures is the count of rows that were
// popped but whose insert or requeue failed.
func (worker *drainer) drainOnce(ctx context.Context) (int, error) {
	rows, err := worker.store.drainClosedTrades(ctx, worker.cfg.DrainerBatch)
	if err != nil {
		return 0, fmt.Errorf("persistence: drainer drainClosedTrades: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	slog.Debug("persistence: drainer draining", "count", len(rows))
	failures := 0
	for _, row := range rows {
		if err := worker.store.insertClosedTrade(ctx, row); err != nil {
			slog.Error("persistence: drainer insert failed — requeuing",
				"trade_id", row.TradeID,
				"err", err,
			)
			if requeueErr := worker.store.requeueClosedTrade(ctx, row); requeueErr != nil {
				// Requeue failure is the one place we cannot preserve
				// durability. Log loudly; do not panic (a failed Redis
				// RPUSH is a boundary I/O event, not an invariant break).
				slog.Error("persistence: drainer requeue failed — row LOST FROM BUFFER",
					"trade_id", row.TradeID,
					"err", requeueErr,
				)
			}
			failures++
			continue
		}
	}
	slog.Debug("persistence: drainer batch done",
		"processed", len(rows),
		"failures", failures,
	)
	return failures, nil
}

// finalDrain is the shutdown pass. One best-effort drainOnce, using a
// fresh context (the caller's ctx may already be cancelled). Whatever
// remains in the buffer stays there for the next boot — D5.
func (worker *drainer) finalDrain(ctx context.Context) {
	slog.Info("persistence: drainer final drain begin")
	failures, err := worker.drainOnce(ctx)
	if err != nil {
		slog.Warn("persistence: drainer final drain error (rows remain in buffer)", "err", err)
		return
	}
	slog.Info("persistence: drainer final drain end", "failures", failures)
}

// backoffDelay computes an exponential backoff delay from the base tick
// and the consecutive-failure count. Capped at 30 * base so a persistent
// outage still retries within a reasonable interval. Deterministic — no
// jitter (single-drainer, no thundering-herd concern per D4).
func backoffDelay(base time.Duration, consecutiveFailures int) time.Duration {
	// 2^n growth, capped at 32x base.
	multiplier := 1 << consecutiveFailures
	const maxMultiplier = 32
	if multiplier <= 0 || multiplier > maxMultiplier {
		multiplier = maxMultiplier
	}
	return base * time.Duration(multiplier)
}
