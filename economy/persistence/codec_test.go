package persistence

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"pgregory.net/rapid"

	"simulator/contract"
)

func TestEncodeDecodeTimeRoundTripRFC3339(t *testing.T) {
	instant := time.Date(2026, 3, 15, 14, 32, 17, 123456789, time.UTC)
	encoded := encodeTime(instant, TimeRFC3339)
	if _, ok := encoded.(string); !ok {
		t.Fatalf("TimeRFC3339 encoding: expected string, got %T", encoded)
	}
	decoded, err := decodeTime(encoded, TimeRFC3339)
	if err != nil {
		t.Fatalf("decodeTime: %v", err)
	}
	if !decoded.Equal(instant) {
		t.Errorf("round-trip mismatch: got %v, want %v", decoded, instant)
	}
}

func TestEncodeDecodeTimeRoundTripUnixNano(t *testing.T) {
	instant := time.Date(2026, 3, 15, 14, 32, 17, 123456789, time.UTC)
	encoded := encodeTime(instant, TimeUnixNano)
	if _, ok := encoded.(int64); !ok {
		t.Fatalf("TimeUnixNano encoding: expected int64, got %T", encoded)
	}
	decoded, err := decodeTime(encoded, TimeUnixNano)
	if err != nil {
		t.Fatalf("decodeTime: %v", err)
	}
	if !decoded.Equal(instant) {
		t.Errorf("round-trip mismatch: got %v, want %v", decoded, instant)
	}
}

func TestEncodeDecodeTimeZeroValueRoundTrips(t *testing.T) {
	// Zero time.Time round-trips in both modes. This matters for optional
	// timestamps (e.g., DripActive=false, no last-tap time yet).
	cases := []struct {
		name     string
		encoding TimeEncoding
	}{
		{"rfc3339", TimeRFC3339},
		{"unixnano", TimeUnixNano},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			encoded := encodeTime(time.Time{}, testCase.encoding)
			decoded, err := decodeTime(encoded, testCase.encoding)
			if err != nil {
				subtest.Fatalf("decodeTime: %v", err)
			}
			if !decoded.IsZero() {
				subtest.Errorf("zero time did not round-trip: got %v", decoded)
			}
		})
	}
}

func TestEncodeTimeRoundTripThroughJSON(t *testing.T) {
	// Callers marshal encoded values as JSON and unmarshal them back. The
	// UnixNano mode carries int64 precision, so unmarshaling into an `any`
	// via the default json.Unmarshal (which uses float64 for numbers) would
	// lose precision on large nanos. Stage 2's real Redis code MUST use a
	// json.Decoder with UseNumber() — this test mirrors that.
	instant := time.Date(2026, 6, 21, 12, 0, 0, 500, time.UTC)

	for _, encoding := range []TimeEncoding{TimeRFC3339, TimeUnixNano} {
		encoded := encodeTime(instant, encoding)
		raw, err := json.Marshal(encoded)
		if err != nil {
			t.Fatalf("marshal (encoding=%d): %v", encoding, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var back any
		if err := decoder.Decode(&back); err != nil {
			t.Fatalf("decode (encoding=%d): %v", encoding, err)
		}
		decoded, err := decodeTime(back, encoding)
		if err != nil {
			t.Fatalf("decodeTime (encoding=%d): %v", encoding, err)
		}
		if !decoded.Equal(instant) {
			t.Errorf("json round-trip (encoding=%d): got %v, want %v", encoding, decoded, instant)
		}
	}
}

func TestDecodeTimeRejectsWrongShape(t *testing.T) {
	// RFC3339 mode expects a string; UnixNano mode expects a number. Feeding
	// the wrong shape is a caller bug; surface it.
	if _, err := decodeTime(int64(42), TimeRFC3339); err == nil {
		t.Errorf("expected error decoding int64 as RFC3339")
	}
	if _, err := decodeTime("not-a-number", TimeUnixNano); err == nil {
		t.Errorf("expected error decoding string as UnixNano")
	}
	if _, err := decodeTime("not-an-rfc3339-string", TimeRFC3339); err == nil {
		t.Errorf("expected parse error on malformed RFC3339")
	}
}

func TestDecodeTimeUnixNanoRejectsFractionalFloat(t *testing.T) {
	// Nanosecond timestamps must be integers. A fractional float indicates a
	// caller lost precision on the way in (e.g. very large nano value cast
	// through float64) — surface it, don't silently truncate.
	if _, err := decodeTime(float64(1.5), TimeUnixNano); err == nil {
		t.Errorf("expected error on fractional float64 nanos")
	}
}

func TestEncodeDecodeSideRoundTrip(t *testing.T) {
	for _, side := range []contract.Side{contract.Buy, contract.Sell} {
		for _, encoding := range []SideEncoding{SideString, SideInt} {
			encoded := encodeSide(side, encoding)
			decoded, err := decodeSide(encoded, encoding)
			if err != nil {
				t.Errorf("decodeSide side=%v encoding=%d: %v", side, encoding, err)
				continue
			}
			if decoded != side {
				t.Errorf("side round-trip encoding=%d: got %v, want %v", encoding, decoded, side)
			}
		}
	}
}

func TestEncodeSideStringValues(t *testing.T) {
	if got := encodeSide(contract.Buy, SideString); got != "buy" {
		t.Errorf("encodeSide(Buy, string): got %v, want \"buy\"", got)
	}
	if got := encodeSide(contract.Sell, SideString); got != "sell" {
		t.Errorf("encodeSide(Sell, string): got %v, want \"sell\"", got)
	}
}

func TestDecodeSideRejectsUnknownValues(t *testing.T) {
	if _, err := decodeSide("neutral", SideString); err == nil {
		t.Errorf("expected error decoding unknown side string")
	}
	if _, err := decodeSide(int64(7), SideInt); err == nil {
		t.Errorf("expected error decoding unknown side int")
	}
	if _, err := decodeSide(42, SideString); err == nil {
		t.Errorf("expected error decoding int as side string")
	}
}

// rapid property: any UTC time.Time with nanosecond resolution round-trips
// under BOTH encodings.
func TestPropertyTimeRoundTrip(t *testing.T) {
	rapid.Check(t, func(spec *rapid.T) {
		// Bound the range to well-representable Unix nanoseconds
		// (year 2000..2100), so encode/decode never overflows int64.
		year := rapid.IntRange(2000, 2100).Draw(spec, "year")
		month := rapid.IntRange(1, 12).Draw(spec, "month")
		day := rapid.IntRange(1, 28).Draw(spec, "day")
		hour := rapid.IntRange(0, 23).Draw(spec, "hour")
		minute := rapid.IntRange(0, 59).Draw(spec, "minute")
		second := rapid.IntRange(0, 59).Draw(spec, "second")
		nano := rapid.IntRange(0, 999999999).Draw(spec, "nano")

		instant := time.Date(year, time.Month(month), day, hour, minute, second, nano, time.UTC)

		for _, encoding := range []TimeEncoding{TimeRFC3339, TimeUnixNano} {
			encoded := encodeTime(instant, encoding)
			decoded, err := decodeTime(encoded, encoding)
			if err != nil {
				spec.Fatalf("decodeTime encoding=%d: %v", encoding, err)
			}
			if !decoded.Equal(instant) {
				spec.Errorf("round-trip encoding=%d: got %v, want %v", encoding, decoded, instant)
			}
		}
	})
}
