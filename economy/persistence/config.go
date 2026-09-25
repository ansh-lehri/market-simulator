package persistence

import "time"

// TimeEncoding selects how time.Time is serialized in Redis JSON payloads
// (snapshot cash/pos/resv keys, closed-trade buffer entries). Both directions
// (encode/decode) MUST use the same value — the codec is symmetric.
//
// TimeRFC3339 (default): human-readable, redis-cli-friendly. Written via
// time.Time.UTC().Format(time.RFC3339Nano).
//
// TimeUnixNano: compact, faster to marshal. Written as int64 nanoseconds
// since the Unix epoch.
type TimeEncoding int

const (
	TimeRFC3339 TimeEncoding = iota
	TimeUnixNano
)

// SideEncoding selects how contract.Side is serialized in Redis JSON.
//
// SideString (default): "buy" / "sell" — debuggable via redis-cli.
// SideInt: 0 / 1 — matches the Go iota values.
type SideEncoding int

const (
	SideString SideEncoding = iota
	SideInt
)

// Config carries the persistence component's tunables. All fields are
// optional; withDefaults applies operational defaults on zero-values.
//
// KeyNamespace is the OPTIONAL prefix in front of every Redis key
// (default ""). Non-empty namespaces isolate test runs from each other and
// from prod; in production it stays empty and keys match §7 verbatim
// ("l5:snap:...", "l5:buf:trades").
type Config struct {
	DrainerTick    time.Duration // default 100ms — how often the drainer polls the buffer (Stage 4)
	DrainerBatch   int           // default 64   — max rows drained per tick
	EnqueueTimeout time.Duration // default 1s   — bounds Redis LPUSH from EnqueueClosedTrade
	TimeEncoding   TimeEncoding  // default TimeRFC3339
	SideEncoding   SideEncoding  // default SideString
	KeyNamespace   string        // default ""   — prefix in front of every Redis key
}

// withDefaults returns a copy of cfg with zero-valued fields filled in.
// Callers should pass the result into NewComponent (Stage 3).
func (cfg Config) withDefaults() Config {
	filled := cfg
	if filled.DrainerTick <= 0 {
		filled.DrainerTick = 100 * time.Millisecond
	}
	if filled.DrainerBatch <= 0 {
		filled.DrainerBatch = 64
	}
	if filled.EnqueueTimeout <= 0 {
		filled.EnqueueTimeout = 1 * time.Second
	}
	// TimeEncoding / SideEncoding: zero values ARE the defaults
	// (TimeRFC3339 == 0, SideString == 0), no fill needed.
	// KeyNamespace: zero value ("") is the production default.
	return filled
}
