package persistence

import (
	"encoding/json"
	"fmt"
	"time"

	"simulator/contract"
)

// The codec layer serializes non-primitive JSON tokens for Redis payloads
// (snapshot cash/pos/resv, closed-trade buffer entries). It is symmetric:
// encodeTime with TimeEncoding X round-trips through decodeTime with the
// SAME TimeEncoding X (same for encodeSide/decodeSide).
//
// Return type is `any` so the caller can drop the value straight into a
// map[string]any or a struct field that json.Marshal will serialize.
// Decode functions accept `any` because json.Unmarshal into an interface{}
// produces float64 for numbers and string for strings — the codec handles
// both float64 and json.Number defensively so callers can use either mode.

// encodeTime returns a JSON-marshalable value for a time.Time under encoding.
// TimeRFC3339 → RFC3339Nano string in UTC (redis-cli readable). Zero time
// encodes as "" (Format on a zero time yields "0001-01-01T00:00:00Z" but the
// empty-string sentinel is smaller and unambiguous).
// TimeUnixNano → int64 nanoseconds since Unix epoch. Zero time is special-
// cased to 0 because time.Time{}.UnixNano() is a huge negative number
// (year 1 AD) that does not round-trip cleanly.
func encodeTime(instant time.Time, encoding TimeEncoding) any {
	switch encoding {
	case TimeUnixNano:
		if instant.IsZero() {
			return int64(0)
		}
		return instant.UnixNano()
	case TimeRFC3339:
		fallthrough
	default:
		if instant.IsZero() {
			return ""
		}
		return instant.UTC().Format(time.RFC3339Nano)
	}
}

// decodeTime reverses encodeTime. It accepts the raw values json.Unmarshal
// hands back: string (for RFC3339) or float64/json.Number/int64 (for
// UnixNano). Zero time (all-zeros) round-trips faithfully in both modes.
func decodeTime(raw any, encoding TimeEncoding) (time.Time, error) {
	switch encoding {
	case TimeUnixNano:
		nanos, err := toInt64(raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("persistence: decodeTime unix-nano: %w", err)
		}
		if nanos == 0 {
			return time.Time{}, nil
		}
		return time.Unix(0, nanos).UTC(), nil
	case TimeRFC3339:
		fallthrough
	default:
		asString, ok := raw.(string)
		if !ok {
			return time.Time{}, fmt.Errorf("persistence: decodeTime rfc3339: expected string, got %T", raw)
		}
		if asString == "" {
			return time.Time{}, nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, asString)
		if err != nil {
			return time.Time{}, fmt.Errorf("persistence: decodeTime rfc3339: parse %q: %w", asString, err)
		}
		return parsed.UTC(), nil
	}
}

// encodeSide returns a JSON-marshalable value for a contract.Side under
// encoding. SideString → "buy"/"sell"; SideInt → 0/1 (matching the iota).
func encodeSide(side contract.Side, encoding SideEncoding) any {
	switch encoding {
	case SideInt:
		return int(side)
	case SideString:
		fallthrough
	default:
		return sideToString(side)
	}
}

// decodeSide reverses encodeSide.
func decodeSide(raw any, encoding SideEncoding) (contract.Side, error) {
	switch encoding {
	case SideInt:
		asInt, err := toInt64(raw)
		if err != nil {
			return 0, fmt.Errorf("persistence: decodeSide int: %w", err)
		}
		return sideFromInt(asInt)
	case SideString:
		fallthrough
	default:
		asString, ok := raw.(string)
		if !ok {
			return 0, fmt.Errorf("persistence: decodeSide string: expected string, got %T", raw)
		}
		return sideFromString(asString)
	}
}

// sideToString maps contract.Side → canonical lowercase string. Any value
// outside {Buy, Sell} is a programming error at this seam; return an
// obviously-wrong sentinel so a subsequent decode fails loudly.
func sideToString(side contract.Side) string {
	switch side {
	case contract.Buy:
		return "buy"
	case contract.Sell:
		return "sell"
	default:
		return fmt.Sprintf("side(%d)", int(side))
	}
}

func sideFromString(asString string) (contract.Side, error) {
	switch asString {
	case "buy":
		return contract.Buy, nil
	case "sell":
		return contract.Sell, nil
	default:
		return 0, fmt.Errorf("persistence: unknown side string %q", asString)
	}
}

func sideFromInt(asInt int64) (contract.Side, error) {
	switch contract.Side(asInt) {
	case contract.Buy:
		return contract.Buy, nil
	case contract.Sell:
		return contract.Sell, nil
	default:
		return 0, fmt.Errorf("persistence: unknown side int %d", asInt)
	}
}

// toInt64 coerces json.Unmarshal's numeric shapes (float64, json.Number)
// plus explicit int/int64 into int64. Overflow / non-integer values become
// errors so precision loss surfaces rather than corrupting a nano-timestamp.
func toInt64(raw any) (int64, error) {
	switch typed := raw.(type) {
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case float64:
		asInt := int64(typed)
		if float64(asInt) != typed {
			return 0, fmt.Errorf("non-integer float64 %v", typed)
		}
		return asInt, nil
	case json.Number:
		return typed.Int64()
	default:
		return 0, fmt.Errorf("expected numeric, got %T", raw)
	}
}
