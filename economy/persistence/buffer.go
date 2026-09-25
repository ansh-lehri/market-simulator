package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// The write-behind buffer for closed-trade rows. LPUSH at enqueue (fast
// path, called from EnqueueClosedTrade in Stage 3); RPOP at drain (async
// worker in Stage 4). LPUSH+RPOP → FIFO delivery order (D4).
//
// Buffer entries are ephemeral (LPUSHed and RPOPed within a drainer cycle
// or two) and use default time.Time JSON encoding — the snapshot-payload
// codec configurability adds no value here. Keeping this decoupled means
// a mid-life config flip cannot orphan buffer entries.

// pushClosedTrade LPUSHes one TradeInsert onto the write-behind buffer. The
// caller (Component.EnqueueClosedTrade) is expected to have validated the
// insert (via the country adapter) before this call so no poison entry can
// hit the buffer.
func (component *store) pushClosedTrade(ctx context.Context, insert TradeInsert) error {
	slog.Debug("persistence: pushClosedTrade",
		"trade_id", insert.TradeID,
		"account_id", insert.AccountID,
	)

	data, err := json.Marshal(insert)
	if err != nil {
		return fmt.Errorf("persistence: pushClosedTrade marshal: %w", err)
	}
	if err := component.redis.LPush(ctx, bufKey(component.cfg.KeyNamespace), data).Err(); err != nil {
		return fmt.Errorf("persistence: pushClosedTrade lpush: %w", err)
	}

	slog.Info("persistence: pushClosedTrade ok",
		"trade_id", insert.TradeID,
		"buffer_key", bufKey(component.cfg.KeyNamespace),
	)
	return nil
}

// drainClosedTrades RPOPs up to `max` entries from the buffer and returns
// them in FIFO order. An empty buffer returns (nil, nil), not an error.
//
// A malformed JSON entry surfaces as an error; the drainer worker (Stage 4)
// decides how to handle poison messages. In Phase 2 we treat any parse
// failure as a hard error, since well-formed EnqueueClosedTrade never
// produces unparseable data.
func (component *store) drainClosedTrades(ctx context.Context, max int) ([]TradeInsert, error) {
	slog.Debug("persistence: drainClosedTrades", "max", max)

	if max <= 0 {
		return nil, nil
	}

	drained := make([]TradeInsert, 0, max)
	bufferKey := bufKey(component.cfg.KeyNamespace)
	for len(drained) < max {
		raw, err := component.redis.RPop(ctx, bufferKey).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				break // buffer empty
			}
			return drained, fmt.Errorf("persistence: drainClosedTrades rpop: %w", err)
		}

		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber() // preserve int64 precision on nested `any` fields (if any)
		var insert TradeInsert
		if err := decoder.Decode(&insert); err != nil {
			return drained, fmt.Errorf("persistence: drainClosedTrades unmarshal: %w", err)
		}
		drained = append(drained, insert)
	}

	slog.Debug("persistence: drainClosedTrades ok", "drained", len(drained))
	return drained, nil
}

// requeueClosedTrade RPUSHes a TradeInsert back onto the buffer's tail so
// the drainer worker (Stage 4) sees it FIRST on the next RPOP. Used only
// when a Postgres insert fails after a successful drain — the row must not
// be lost. Idempotent-safe with ON CONFLICT.
//
// LIST semantics recap: producers LPUSH (head), consumer RPOPs (tail) →
// FIFO. A row we already popped came from the tail; RPUSH puts it back
// where RPOP is looking, so it retries immediately. Backoff at the drainer
// level prevents a poison row from pinning the CPU.
func (component *store) requeueClosedTrade(ctx context.Context, insert TradeInsert) error {
	slog.Debug("persistence: requeueClosedTrade", "trade_id", insert.TradeID)

	data, err := json.Marshal(insert)
	if err != nil {
		return fmt.Errorf("persistence: requeueClosedTrade marshal: %w", err)
	}
	if err := component.redis.RPush(ctx, bufKey(component.cfg.KeyNamespace), data).Err(); err != nil {
		return fmt.Errorf("persistence: requeueClosedTrade rpush: %w", err)
	}
	return nil
}
