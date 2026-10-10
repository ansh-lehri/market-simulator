package marketdata

import "errors"

// ErrLoadFailed wraps every CandleStore infra/driver failure — bad DSN,
// dropped connection, ctx cancellation, malformed arguments. A category
// sentinel; callers dispatch via errors.Is, never errors.As on the wrapped
// struct (feedback_error_category_sentinels).
//
// Empty read is NOT this error: RangeRead1m over a window with no stored
// candles returns ([]Candle{}, nil). The HTTP durable-read status map
// (DESIGN §4.5) relies on this split — empty → 200 (incl. candles:[]),
// ErrLoadFailed → 503, never 404 or 504.
var ErrLoadFailed = errors.New("broker/marketdata: durable load failed")
