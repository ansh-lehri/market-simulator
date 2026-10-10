package marketdata

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"simulator/contract"
)

// PathCandles is the one route this handler owns. Exported so bringup
// can register without duplicating the string. Versioned under /v1/ to
// match the broker's existing read routes (account/trades, pnl, etc.).
const PathCandles = "/v1/candles"

// naiveLocalLayout is the ISO-8601 "no timezone" layout the wire uses
// (DESIGN §2.4). Clients send and receive strings in this shape; the
// handler attaches/strips the market tz on the way in/out — the client
// never touches UTC.
const naiveLocalLayout = "2006-01-02T15:04:05"

// BarDTO is the wire shape of one bar on the response envelope. Times
// are naive-local ISO (no timezone suffix); OHLCV are bare int64s (no
// unit/currency field, D-3 / P6-D33).
type BarDTO struct {
	Start  string `json:"start"`
	End    string `json:"end"`
	Open   int64  `json:"open"`
	High   int64  `json:"high"`
	Low    int64  `json:"low"`
	Close  int64  `json:"close"`
	Volume int64  `json:"volume"`
}

// CandleResponse is the HTTP envelope for GET /v1/candles.
//   - Market / Instrument / Timeframe echo the request so a client with
//     multiple in-flight requests can disambiguate without state.
//   - Timezone names the IANA tz every naive-local time in this envelope
//     is expressed in (DESIGN §4.4).
//   - From / To echo the client's requested window strings verbatim.
//   - Candles is the folded page (never nil; empty on an empty window).
//   - NextCursor is a naive-local ISO string or null (last page).
type CandleResponse struct {
	Market     string   `json:"market"`
	Instrument string   `json:"instrument"`
	Timeframe  string   `json:"timeframe"`
	Timezone   string   `json:"timezone"`
	From       string   `json:"from"`
	To         string   `json:"to"`
	Candles    []BarDTO `json:"candles"`
	NextCursor *string  `json:"next_cursor"`
}

// NewCandleHandler constructs the GET /v1/candles handler. engine must
// be non-nil; cfg must validate. Both are checked at construction (fail
// fast) rather than deferred to the first request — same discipline as
// broker/reads/NewTradesHandler.
func NewCandleHandler(engine queryEnginePort, cfg Config) http.Handler {
	if engine == nil {
		panic("broker/marketdata: NewCandleHandler requires a non-nil queryEnginePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/marketdata: NewCandleHandler: " + err.Error())
	}
	return &candleHandler{engine: engine, cfg: cfg}
}

type candleHandler struct {
	engine queryEnginePort
	cfg    Config
}

// ServeHTTP implements the full flow: method guard → recover → parse
// query → ctx with timeout → engine.QueryCandles → status dispatch.
//
// Status map (DESIGN §4.5, durable-read family — same family as
// broker/reads/trades):
//   - 200 on success, including empty candles[] (empty is NEVER 404/409).
//   - 503 on ErrLoadFailed, including a ctx timeout surfaced via the
//     Store's wrap. Durable reads NEVER emit 504.
//   - 400 on bad query params: missing market/instrument/timeframe/
//     from/to; off-allowlist market or timeframe; unparseable times;
//     from>=to; bad cursor; non-positive limit.
//   - 405 on non-GET.
//   - 500 defensive on any non-ErrLoadFailed error (should not fire —
//     the engine's only error source is Store, which always wraps
//     ErrLoadFailed).
func (handlerInstance *candleHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathCandles)

	if !requireGET(responseWriter, request, PathCandles) {
		return
	}
	query, location, status, parseErrMsg := parseCandleQuery(request)
	if status != 0 {
		slog.Debug("broker/marketdata: 400 bad candle query",
			"path", PathCandles,
			"err", parseErrMsg,
		)
		writeError(responseWriter, status, parseErrMsg, PathCandles)
		return
	}

	// Echo back the client's naive-local from/to verbatim. Tz symmetry
	// means "what you sent is what you get back" — simpler than
	// re-formatting from the parsed instants, and matches HTTP convention.
	fromEcho := request.URL.Query().Get("from")
	toEcho := request.URL.Query().Get("to")

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/marketdata: candles begin",
		"market", query.Market,
		"instrument", query.Instrument,
		"timeframe", query.TF,
		"from", query.From,
		"to", query.To,
		"limit", query.Limit,
	)
	page, readErr := handlerInstance.engine.QueryCandles(requestCtx, query)

	switch {
	case errors.Is(readErr, ErrLoadFailed):
		slog.Error("broker/marketdata: 503 candles store unavailable",
			"market", query.Market,
			"instrument", query.Instrument,
			"err", readErr,
		)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable", PathCandles)
	case readErr != nil:
		slog.Error("broker/marketdata: 500 candles unexpected error",
			"market", query.Market,
			"instrument", query.Instrument,
			"err", readErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathCandles)
	default:
		response := toResponse(query, page, location, fromEcho, toEcho)
		slog.Info("broker/marketdata: 200 candles",
			"market", query.Market,
			"instrument", query.Instrument,
			"timeframe", query.TF,
			"bar_count", len(response.Candles),
			"has_more", response.NextCursor != nil,
		)
		writeJSON(responseWriter, http.StatusOK, response, PathCandles)
	}
}

// parseCandleQuery validates and parses the query string. Returns
// (query, location, 0, "") on success or (zero, nil, status, msg) on
// failure; the caller writes status + msg via writeError.
//
// Order of checks is user-friendly (specific messages first, cheap
// validations before expensive ones):
//
//  1. market present + in allowlist (resolves location for later parses)
//  2. instrument present
//  3. timeframe present + on allowlist
//  4. from present + parseable + to present + parseable
//  5. from < to (strict)
//  6. cursor optional + parseable
//  7. limit optional + strictly positive (zero/negative is a client bug,
//     not a shortcut — mirrors broker/reads/parseLimit)
//
// Separated from ServeHTTP so unit tests can drive it without an
// http.ResponseWriter.
func parseCandleQuery(request *http.Request) (CandleQuery, *time.Location, int, string) {
	queryValues := request.URL.Query()

	market := queryValues.Get("market")
	if market == "" {
		return CandleQuery{}, nil, http.StatusBadRequest, "market required"
	}
	location, marketOK := tzFor(market)
	if !marketOK {
		return CandleQuery{}, nil, http.StatusBadRequest, "unknown market"
	}

	instrument := queryValues.Get("instrument")
	if instrument == "" {
		return CandleQuery{}, nil, http.StatusBadRequest, "instrument required"
	}

	timeframeString := queryValues.Get("timeframe")
	if timeframeString == "" {
		return CandleQuery{}, nil, http.StatusBadRequest, "timeframe required"
	}
	timeframe, timeframeOK := ParseTimeframe(timeframeString)
	if !timeframeOK {
		return CandleQuery{}, nil, http.StatusBadRequest, "timeframe must be one of 1m, 5m, 30m, 60m"
	}

	fromRaw := queryValues.Get("from")
	if fromRaw == "" {
		return CandleQuery{}, nil, http.StatusBadRequest, "from required"
	}
	fromTime, fromOK := parseNaiveLocalTime(fromRaw, location)
	if !fromOK {
		return CandleQuery{}, nil, http.StatusBadRequest, "from must be ISO-8601 naive-local (YYYY-MM-DDTHH:MM:SS)"
	}

	toRaw := queryValues.Get("to")
	if toRaw == "" {
		return CandleQuery{}, nil, http.StatusBadRequest, "to required"
	}
	toTime, toOK := parseNaiveLocalTime(toRaw, location)
	if !toOK {
		return CandleQuery{}, nil, http.StatusBadRequest, "to must be ISO-8601 naive-local (YYYY-MM-DDTHH:MM:SS)"
	}

	if !fromTime.Before(toTime) {
		return CandleQuery{}, nil, http.StatusBadRequest, "from must be strictly before to"
	}

	var afterTS *time.Time
	cursorRaw := queryValues.Get("cursor")
	if cursorRaw != "" {
		parsedCursor, cursorOK := parseNaiveLocalTime(cursorRaw, location)
		if !cursorOK {
			return CandleQuery{}, nil, http.StatusBadRequest, "cursor must be ISO-8601 naive-local"
		}
		afterTS = &parsedCursor
	}

	limit := defaultCandleLimit
	limitRaw := queryValues.Get("limit")
	if limitRaw != "" {
		parsedLimit, parseErr := strconv.Atoi(limitRaw)
		if parseErr != nil || parsedLimit <= 0 {
			return CandleQuery{}, nil, http.StatusBadRequest, "limit must be a positive integer"
		}
		limit = parsedLimit
	}

	return CandleQuery{
		Market:     market,
		Instrument: contract.InstrumentID(instrument),
		TF:         timeframe,
		From:       fromTime,
		To:         toTime,
		AfterTS:    afterTS,
		Limit:      limit,
	}, location, 0, ""
}

// parseNaiveLocalTime parses a naive-local ISO-8601 string in the given
// market tz. Returns a time.Time whose instant is the UTC moment for
// that wall-clock in location. An unparseable string returns ok=false
// so the caller writes 400.
func parseNaiveLocalTime(raw string, location *time.Location) (time.Time, bool) {
	parsed, err := time.ParseInLocation(naiveLocalLayout, raw, location)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

// toResponse shapes the engine's output into the wire envelope. Bar
// times render as naive-local in the market tz; NextCursor likewise.
// From / To echo verbatim from the request so the client sees back what
// it sent (no micro-format drift round-tripping).
func toResponse(query CandleQuery, page CandlePage, location *time.Location, fromEcho, toEcho string) CandleResponse {
	bars := make([]BarDTO, len(page.Candles))
	for index, candle := range page.Candles {
		bars[index] = BarDTO{
			Start:  candle.Start.In(location).Format(naiveLocalLayout),
			End:    candle.End.In(location).Format(naiveLocalLayout),
			Open:   candle.Open,
			High:   candle.High,
			Low:    candle.Low,
			Close:  candle.Close,
			Volume: candle.Volume,
		}
	}
	var nextCursorString *string
	if page.NextCursor != nil {
		formatted := page.NextCursor.In(location).Format(naiveLocalLayout)
		nextCursorString = &formatted
	}
	return CandleResponse{
		Market:     query.Market,
		Instrument: string(query.Instrument),
		Timeframe:  timeframeString(query.TF),
		Timezone:   location.String(),
		From:       fromEcho,
		To:         toEcho,
		Candles:    bars,
		NextCursor: nextCursorString,
	}
}

// timeframeString returns the wire string for a Timeframe. Inverse of
// ParseTimeframe. Private — only toResponse uses it. An unknown value
// reaching here means the handler bypassed ParseTimeframe validation
// (programming error, panic per root CLAUDE.md).
func timeframeString(timeframe Timeframe) string {
	switch timeframe {
	case TF1m:
		return "1m"
	case TF5m:
		return "5m"
	case TF30m:
		return "30m"
	case TF60m:
		return "60m"
	}
	panic("broker/marketdata: timeframeString called with unknown Timeframe")
}
