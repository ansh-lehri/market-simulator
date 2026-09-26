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

// The snapshot layer serializes SnapshotPayload to the three Redis key groups
// (cash / pos:* / resv:*) per §7. Time and Side are routed through the
// configurable codec (RFC3339 or UnixNano; string or int) — all other
// numeric fields land as JSON numbers and are decoded via UseNumber() to
// preserve int64 precision.
//
// Buffer entries (buffer.go) do NOT use the codec — they are ephemeral and
// use default time.Time JSON.

// ---- intermediate JSON structs ---------------------------------------------

// cashJSON mirrors CashSnap for Redis. No time or side fields → no codec.
type cashJSON struct {
	Available    int64  `json:"available"` // SIGNED
	Reserved     int64  `json:"reserved"`
	Settled      int64  `json:"settled"`
	BankCash     int64  `json:"bank_cash"`
	DripActive   bool   `json:"drip_active"`
	DripProgress *int64 `json:"drip_progress"`
}

// positionJSON mirrors PositionSnap. InstrumentID is embedded in the Redis
// key, not the value; not repeated in the JSON body.
type positionJSON struct {
	AvailableUnits int64     `json:"available_units"`
	ReservedUnits  int64     `json:"reserved_units"`
	OpenLots       []lotJSON `json:"open_lots"`
}

// lotJSON mirrors LotSnap. EntryAt goes through encodeTime, so its JSON type
// depends on Config.TimeEncoding — string (RFC3339) or number (UnixNano).
type lotJSON struct {
	EntryPrice   int64 `json:"entry_price"`
	EntryAt      any   `json:"entry_at"` // encoded via codec
	RemainingQty int64 `json:"remaining_qty"`
}

// reservationJSON mirrors ReservationSnap. Side goes through encodeSide;
// link-back ids (OrderID/AccountID/SessionID/InstrumentID) are redundantly
// stored so a raw redis-cli read is self-describing.
type reservationJSON struct {
	OrderID          string `json:"order_id"`
	AccountID        string `json:"account_id"`
	SessionID        string `json:"session_id"`
	InstrumentID     string `json:"instrument_id"`
	Side             any    `json:"side"` // encoded via codec
	IsMarket         bool   `json:"is_market"`
	ReservedCash     int64  `json:"reserved_cash"`
	ReservedUnits    int64  `json:"reserved_units"`
	RemainingQty     int64  `json:"remaining_qty"`
	OrigQty          int64  `json:"orig_qty"`
	EverFilled       bool   `json:"ever_filled"`
	BrokerageCharged bool   `json:"brokerage_charged"`
}

// ---- write path ------------------------------------------------------------

// writeSnapshot mirrors ONE account's live state into Redis (cash + all
// positions + all reservations) atomically via MULTI/EXEC. Also cleans up
// stale position/reservation keys for that account so an instrument/order
// dropped from the payload does not linger.
func (component *store) writeSnapshot(ctx context.Context, payload SnapshotPayload) error {
	slog.Debug("persistence: writeSnapshot",
		"session_id", payload.SessionID,
		"account_id", payload.AccountID,
		"positions", len(payload.Positions),
		"reservations", len(payload.Reservations),
	)

	// Build every new key value up-front (marshal errors surface before any
	// Redis write, keeping partial-mutation risk to zero).
	cashData, err := marshalCashSnap(payload.Cash)
	if err != nil {
		return fmt.Errorf("persistence: writeSnapshot marshal cash: %w", err)
	}
	newPosKeyValue := make(map[string][]byte, len(payload.Positions))
	for _, pos := range payload.Positions {
		key := posKey(component.cfg.KeyNamespace, payload.SessionID, payload.AccountID, pos.InstrumentID)
		value, err := marshalPositionSnap(pos, component.cfg.TimeEncoding)
		if err != nil {
			return fmt.Errorf("persistence: writeSnapshot marshal pos %s: %w", pos.InstrumentID, err)
		}
		newPosKeyValue[key] = value
	}
	newResvKeyValue := make(map[string][]byte, len(payload.Reservations))
	for _, resv := range payload.Reservations {
		key := resvKey(component.cfg.KeyNamespace, payload.SessionID, payload.AccountID, resv.OrderID)
		value, err := marshalReservationSnap(resv, component.cfg.TimeEncoding, component.cfg.SideEncoding)
		if err != nil {
			return fmt.Errorf("persistence: writeSnapshot marshal resv %s: %w", resv.OrderID, err)
		}
		newResvKeyValue[key] = value
	}

	// Enumerate existing keys so we can DEL the stale ones.
	existingPosKeys, err := component.scanKeys(ctx, posPrefix(component.cfg.KeyNamespace, payload.SessionID, payload.AccountID))
	if err != nil {
		return fmt.Errorf("persistence: writeSnapshot scan positions: %w", err)
	}
	existingResvKeys, err := component.scanKeys(ctx, resvPrefix(component.cfg.KeyNamespace, payload.SessionID, payload.AccountID))
	if err != nil {
		return fmt.Errorf("persistence: writeSnapshot scan reservations: %w", err)
	}
	staleKeys := diffKeys(existingPosKeys, newPosKeyValue)
	staleKeys = append(staleKeys, diffKeys(existingResvKeys, newResvKeyValue)...)

	// One MULTI/EXEC transaction: DEL stale + SET all new. All-or-nothing
	// so a partial failure never leaves a half-written snapshot.
	_, err = component.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if len(staleKeys) > 0 {
			pipe.Del(ctx, staleKeys...)
		}
		pipe.Set(ctx, cashKey(component.cfg.KeyNamespace, payload.SessionID, payload.AccountID), cashData, 0)
		for key, value := range newPosKeyValue {
			pipe.Set(ctx, key, value, 0)
		}
		for key, value := range newResvKeyValue {
			pipe.Set(ctx, key, value, 0)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("persistence: writeSnapshot pipeline: %w", err)
	}

	slog.Info("persistence: writeSnapshot ok",
		"session_id", payload.SessionID,
		"account_id", payload.AccountID,
		"positions_written", len(newPosKeyValue),
		"reservations_written", len(newResvKeyValue),
		"stale_keys_deleted", len(staleKeys),
	)
	return nil
}

// ---- read path -------------------------------------------------------------

// readSnapshot reads ONE account's snapshot from Redis. Returns
// ErrSnapshotMissing with the ids-populated payload if the cash key is
// absent — the load-bearing seed key. Lifecycle uses that sentinel to
// distinguish "no snapshot" from a zero-valued snapshot (a brand-new
// session with zero capital would otherwise look identical to missing).
//
// Positions and reservations arrive in Redis SCAN order, which is not
// deterministic across runs; callers must not depend on iteration order.
// OpenLots FIFO order INSIDE a position is preserved by the JSON array.
// Mid-scan position/reservation absence (key vanished between SCAN and
// GET) stays a non-fatal race and is skipped internally.
func (component *store) readSnapshot(ctx context.Context, sessionID, accountID string) (SnapshotPayload, error) {
	slog.Debug("persistence: readSnapshot", "session_id", sessionID, "account_id", accountID)

	payload := SnapshotPayload{SessionID: sessionID, AccountID: accountID}

	cashRaw, cashErr := component.redis.Get(ctx, cashKey(component.cfg.KeyNamespace, sessionID, accountID)).Bytes()
	switch {
	case errors.Is(cashErr, redis.Nil):
		return payload, ErrSnapshotMissing
	case cashErr != nil:
		return payload, fmt.Errorf("persistence: readSnapshot cash: %w", cashErr)
	default:
		cash, err := unmarshalCashSnap(cashRaw)
		if err != nil {
			return payload, fmt.Errorf("persistence: readSnapshot cash decode: %w", err)
		}
		payload.Cash = cash
	}

	posKeys, err := component.scanKeys(ctx, posPrefix(component.cfg.KeyNamespace, sessionID, accountID))
	if err != nil {
		return payload, fmt.Errorf("persistence: readSnapshot scan positions: %w", err)
	}
	for _, key := range posKeys {
		instrumentID, ok := trailingSegment(key)
		if !ok {
			return payload, fmt.Errorf("persistence: readSnapshot: malformed position key %q", key)
		}
		raw, err := component.redis.Get(ctx, key).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // key vanished between SCAN and GET; treat as absent
			}
			return payload, fmt.Errorf("persistence: readSnapshot pos %s: %w", key, err)
		}
		pos, err := unmarshalPositionSnap(raw, instrumentID, component.cfg.TimeEncoding)
		if err != nil {
			return payload, fmt.Errorf("persistence: readSnapshot pos %s decode: %w", key, err)
		}
		payload.Positions = append(payload.Positions, pos)
	}

	resvKeys, err := component.scanKeys(ctx, resvPrefix(component.cfg.KeyNamespace, sessionID, accountID))
	if err != nil {
		return payload, fmt.Errorf("persistence: readSnapshot scan reservations: %w", err)
	}
	for _, key := range resvKeys {
		raw, err := component.redis.Get(ctx, key).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return payload, fmt.Errorf("persistence: readSnapshot resv %s: %w", key, err)
		}
		resv, err := unmarshalReservationSnap(raw, component.cfg.TimeEncoding, component.cfg.SideEncoding)
		if err != nil {
			return payload, fmt.Errorf("persistence: readSnapshot resv %s decode: %w", key, err)
		}
		payload.Reservations = append(payload.Reservations, resv)
	}

	slog.Debug("persistence: readSnapshot ok",
		"session_id", sessionID,
		"account_id", accountID,
		"positions", len(payload.Positions),
		"reservations", len(payload.Reservations),
	)
	return payload, nil
}

// deleteSnapshot removes all three key groups for one account. Used on clean
// session close (LAYER_5_PERSISTENCE §7: "Snapshot is discarded on clean
// close").
func (component *store) deleteSnapshot(ctx context.Context, sessionID, accountID string) error {
	slog.Debug("persistence: deleteSnapshot", "session_id", sessionID, "account_id", accountID)

	keysToDelete := []string{cashKey(component.cfg.KeyNamespace, sessionID, accountID)}
	posKeys, err := component.scanKeys(ctx, posPrefix(component.cfg.KeyNamespace, sessionID, accountID))
	if err != nil {
		return fmt.Errorf("persistence: deleteSnapshot scan positions: %w", err)
	}
	resvKeys, err := component.scanKeys(ctx, resvPrefix(component.cfg.KeyNamespace, sessionID, accountID))
	if err != nil {
		return fmt.Errorf("persistence: deleteSnapshot scan reservations: %w", err)
	}
	keysToDelete = append(keysToDelete, posKeys...)
	keysToDelete = append(keysToDelete, resvKeys...)

	if _, err := component.redis.Del(ctx, keysToDelete...).Result(); err != nil {
		return fmt.Errorf("persistence: deleteSnapshot del: %w", err)
	}
	slog.Info("persistence: deleteSnapshot ok",
		"session_id", sessionID,
		"account_id", accountID,
		"keys_deleted", len(keysToDelete),
	)
	return nil
}

// ---- marshal / unmarshal helpers -------------------------------------------

func marshalCashSnap(cash CashSnap) ([]byte, error) {
	return json.Marshal(cashJSON{
		Available:    cash.Available,
		Reserved:     cash.Reserved,
		Settled:      cash.Settled,
		BankCash:     cash.BankCash,
		DripActive:   cash.DripActive,
		DripProgress: cash.DripProgress,
	})
}

func unmarshalCashSnap(raw []byte) (CashSnap, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var j cashJSON
	if err := decoder.Decode(&j); err != nil {
		return CashSnap{}, err
	}
	return CashSnap{
		Available:    j.Available,
		Reserved:     j.Reserved,
		Settled:      j.Settled,
		BankCash:     j.BankCash,
		DripActive:   j.DripActive,
		DripProgress: j.DripProgress,
	}, nil
}

func marshalPositionSnap(pos PositionSnap, timeEncoding TimeEncoding) ([]byte, error) {
	lots := make([]lotJSON, len(pos.OpenLots))
	for i, lot := range pos.OpenLots {
		lots[i] = lotJSON{
			EntryPrice:   lot.EntryPrice,
			EntryAt:      encodeTime(lot.EntryAt, timeEncoding),
			RemainingQty: lot.RemainingQty,
		}
	}
	return json.Marshal(positionJSON{
		AvailableUnits: pos.AvailableUnits,
		ReservedUnits:  pos.ReservedUnits,
		OpenLots:       lots,
	})
}

func unmarshalPositionSnap(raw []byte, instrumentID string, timeEncoding TimeEncoding) (PositionSnap, error) {
	// UseNumber() only affects `any` targets (the EntryAt / Side fields).
	// Typed int64 struct fields decode directly with full precision.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var j positionJSON
	if err := decoder.Decode(&j); err != nil {
		return PositionSnap{}, err
	}
	lots := make([]LotSnap, len(j.OpenLots))
	for i, lot := range j.OpenLots {
		entryAt, err := decodeTime(lot.EntryAt, timeEncoding)
		if err != nil {
			return PositionSnap{}, fmt.Errorf("lot %d entry_at: %w", i, err)
		}
		lots[i] = LotSnap{
			EntryPrice:   lot.EntryPrice,
			EntryAt:      entryAt,
			RemainingQty: lot.RemainingQty,
		}
	}
	return PositionSnap{
		InstrumentID:   instrumentID,
		AvailableUnits: j.AvailableUnits,
		ReservedUnits:  j.ReservedUnits,
		OpenLots:       lots,
	}, nil
}

func marshalReservationSnap(resv ReservationSnap, timeEncoding TimeEncoding, sideEncoding SideEncoding) ([]byte, error) {
	_ = timeEncoding // Reservation has no timestamps in Phase 2; kept in the signature for symmetry with position.
	return json.Marshal(reservationJSON{
		OrderID:          resv.OrderID,
		AccountID:        resv.AccountID,
		SessionID:        resv.SessionID,
		InstrumentID:     resv.InstrumentID,
		Side:             encodeSide(resv.Side, sideEncoding),
		IsMarket:         resv.IsMarket,
		ReservedCash:     resv.ReservedCash,
		ReservedUnits:    resv.ReservedUnits,
		RemainingQty:     resv.RemainingQty,
		OrigQty:          resv.OrigQty,
		EverFilled:       resv.EverFilled,
		BrokerageCharged: resv.BrokerageCharged,
	})
}

func unmarshalReservationSnap(raw []byte, timeEncoding TimeEncoding, sideEncoding SideEncoding) (ReservationSnap, error) {
	_ = timeEncoding
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var j reservationJSON
	if err := decoder.Decode(&j); err != nil {
		return ReservationSnap{}, err
	}
	side, err := decodeSide(j.Side, sideEncoding)
	if err != nil {
		return ReservationSnap{}, fmt.Errorf("side: %w", err)
	}
	return ReservationSnap{
		OrderID:          j.OrderID,
		AccountID:        j.AccountID,
		SessionID:        j.SessionID,
		InstrumentID:     j.InstrumentID,
		Side:             side,
		IsMarket:         j.IsMarket,
		ReservedCash:     j.ReservedCash,
		ReservedUnits:    j.ReservedUnits,
		RemainingQty:     j.RemainingQty,
		OrigQty:          j.OrigQty,
		EverFilled:       j.EverFilled,
		BrokerageCharged: j.BrokerageCharged,
	}, nil
}

// ---- key helpers -----------------------------------------------------------

// scanKeys enumerates every key matching pattern using SCAN. SCAN is safer
// than KEYS on large keyspaces (non-blocking, cursor-based). Iteration order
// is not deterministic and caller must not rely on it.
func (component *store) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var (
		cursor uint64
		keys   []string
	)
	for {
		batch, nextCursor, err := component.redis.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
	return keys, nil
}

// diffKeys returns keys present in `existing` but absent from `keep`.
func diffKeys(existing []string, keep map[string][]byte) []string {
	stale := make([]string, 0)
	for _, key := range existing {
		if _, present := keep[key]; !present {
			stale = append(stale, key)
		}
	}
	return stale
}

// trailingSegment returns the substring after the last ":". Used to recover
// the instrument_id from a Redis position key (which embeds it in the tail).
func trailingSegment(key string) (string, bool) {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == ':' {
			return key[i+1:], true
		}
	}
	return "", false
}
