package marketdata

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"simulator/contract"
)

// PathWS is the one WebSocket route this handler owns. Exported so
// bringup (Stage 7) can register without duplicating the string.
// Matches the /v1/candles versioning on the sibling HTTP surface.
const PathWS = "/v1/ws"

// WSConfig carries the dials the WS layer needs. Stage-5 defaults are
// opinionated; bringup (Stage 7) will build this from BrokerConfig.
//
// All five fields are required > 0; a zero value in any field is a
// bringup programming error (invariant per root CLAUDE.md) and panics
// in NewWSHandler.
type WSConfig struct {
	// ConnBuffer sizes each Conn's outbound buffer. Larger = tolerates
	// bigger bursts before dropping; smaller = evicts a slow client
	// sooner. 256 is a reasonable default.
	ConnBuffer int

	// LagEvictionThreshold is the consecutive-drop count that triggers
	// writeLoop to disconnect the client. Mirrors the "slow client"
	// discipline from DESIGN §8.7 — a persistently non-draining client
	// is cut after N drops in a row (NOT a sliding window — the
	// counter resets to 0 on any successful enqueue).
	LagEvictionThreshold int

	// PingInterval is how often writeLoop sends a gorilla PingMessage
	// control frame to keep the connection alive + probe liveness.
	// Pong responses reset the read deadline (see ReadTimeout).
	PingInterval time.Duration

	// ReadTimeout is the read deadline on the socket; the pong handler
	// resets it on every pong. If no pong within this window, the next
	// ReadMessage errors and readLoop disconnects.
	ReadTimeout time.Duration

	// WriteTimeout is the write deadline on each socket write (push +
	// ping). A stuck socket surfaces as a WriteMessage error and
	// writeLoop disconnects.
	WriteTimeout time.Duration
}

// ErrBadWSConfig is the category sentinel every Validate failure
// wraps so callers (bringup, tests) dispatch with a single errors.Is.
var ErrBadWSConfig = errors.New("broker/marketdata: bad WSConfig")

// Validate rejects an unusable WSConfig at construction time.
func (cfg WSConfig) Validate() error {
	if cfg.ConnBuffer <= 0 {
		return fmt.Errorf("ConnBuffer=%d (must be > 0): %w", cfg.ConnBuffer, ErrBadWSConfig)
	}
	if cfg.LagEvictionThreshold <= 0 {
		return fmt.Errorf("LagEvictionThreshold=%d (must be > 0): %w", cfg.LagEvictionThreshold, ErrBadWSConfig)
	}
	if cfg.PingInterval <= 0 {
		return fmt.Errorf("PingInterval=%v (must be > 0): %w", cfg.PingInterval, ErrBadWSConfig)
	}
	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("ReadTimeout=%v (must be > 0): %w", cfg.ReadTimeout, ErrBadWSConfig)
	}
	if cfg.WriteTimeout <= 0 {
		return fmt.Errorf("WriteTimeout=%v (must be > 0): %w", cfg.WriteTimeout, ErrBadWSConfig)
	}
	return nil
}

// upgrader is the shared gorilla upgrader used by every wsHandler.
// CheckOrigin returns true unconditionally — the demo WS surface is
// anonymous and open (DESIGN §8.2); a prod deploy would gate this at
// the ALB / origin check layer.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// NewWSHandler builds the GET /v1/ws upgrade handler. fanOut must be
// non-nil; cfg must validate. Both are checked at construction (fail
// fast) rather than deferred to the first request.
func NewWSHandler(fanOut *FanOut, cfg WSConfig) http.Handler {
	if fanOut == nil {
		panic("broker/marketdata: NewWSHandler requires non-nil FanOut")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/marketdata: NewWSHandler: " + err.Error())
	}
	return &wsHandler{fanOut: fanOut, cfg: cfg}
}

type wsHandler struct {
	fanOut *FanOut
	cfg    WSConfig
}

// ServeHTTP upgrades the request to WebSocket, spawns readLoop and
// writeLoop (two goroutines per connection per BODIES §3.4), and
// returns only after both exit. gorilla's Upgrade hijacks the
// connection; standard HTTP response status is already written by
// Upgrade on failure.
func (handlerInstance *wsHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathWS)

	socket, upgradeErr := upgrader.Upgrade(responseWriter, request, nil)
	if upgradeErr != nil {
		// gorilla.Upgrade already wrote an HTTP error response; nothing
		// to do but log.
		slog.Debug("broker/marketdata: WS upgrade failed", "err", upgradeErr)
		return
	}
	conn := NewConn(socket, handlerInstance.cfg.ConnBuffer)
	slog.Info("broker/marketdata: WS connected",
		"exch", handlerInstance.fanOut.marketSnapshot.Exch(),
		"market", handlerInstance.fanOut.market,
	)

	// Pong resets the read deadline. Initial read deadline is set here
	// so a client that never pongs is cut when the deadline fires.
	socket.SetPongHandler(func(string) error {
		_ = socket.SetReadDeadline(time.Now().Add(handlerInstance.cfg.ReadTimeout))
		return nil
	})
	_ = socket.SetReadDeadline(time.Now().Add(handlerInstance.cfg.ReadTimeout))

	// Spawn the write loop; run the read loop on this goroutine (per
	// BODIES §3.4 "two goroutines per connection — read + sole-writer
	// write"). ServeHTTP returns only after BOTH exit so the hijacked
	// connection isn't released from under them.
	writeLoopDone := make(chan struct{})
	go func() {
		defer close(writeLoopDone)
		handlerInstance.writeLoop(conn)
	}()

	handlerInstance.readLoop(conn)
	<-writeLoopDone
	slog.Info("broker/marketdata: WS disconnected",
		"exch", handlerInstance.fanOut.marketSnapshot.Exch(),
	)
}

// readLoop is the per-Conn inbound-frame reader. Exits only on socket
// error (peer close, read deadline, bad JSON once we read it) — then
// Disconnects. Bad JSON on an individual frame is logged + skipped; a
// single garbage message must not kill the connection (DESIGN §8
// skip-and-log never panic).
func (handlerInstance *wsHandler) readLoop(conn *Conn) {
	defer handlerInstance.fanOut.Disconnect(conn)
	for {
		_, data, readErr := conn.socket.ReadMessage()
		if readErr != nil {
			slog.Debug("broker/marketdata: WS read closed", "err", readErr)
			return
		}
		var frame wsInboundFrame
		if unmarshalErr := json.Unmarshal(data, &frame); unmarshalErr != nil {
			slog.Debug("broker/marketdata: WS bad inbound JSON", "err", unmarshalErr)
			continue
		}
		switch frame.Type {
		case "subscribe":
			handlerInstance.handleSubscribe(conn, frame.Subs)
		case "unsubscribe":
			handlerInstance.handleUnsubscribe(conn, frame.Subs)
		default:
			slog.Debug("broker/marketdata: WS unknown frame type", "type", frame.Type)
		}
	}
}

// writeLoop is the per-Conn SOLE socket writer. It first sends the
// connected frame (establishes tz context) then drains the outbound
// channel + ticks pings. On any WriteMessage error, Disconnect.
//
// Slow-client eviction (DESIGN §8.7 + BODIES §3.6): before each
// successful drain, check laggedCount — if ≥ LagEvictionThreshold the
// client has fallen behind by N consecutive Publish drops, so cut it.
// The client is expected to reconnect + re-fetch history via
// GET /candles then resume live.
func (handlerInstance *wsHandler) writeLoop(conn *Conn) {
	defer handlerInstance.fanOut.Disconnect(conn)

	pingTicker := time.NewTicker(handlerInstance.cfg.PingInterval)
	defer pingTicker.Stop()

	// Connected frame first — establishes the market/timezone context
	// for every naive-local string that follows (WS protocol §1).
	connected := connectedFrame{
		market:   handlerInstance.fanOut.market,
		timezone: handlerInstance.fanOut.location.String(),
	}
	if err := handlerInstance.writeFrame(conn, connected); err != nil {
		return
	}

	for {
		select {
		case frame, ok := <-conn.outbound:
			if !ok {
				// Disconnect closed the channel.
				return
			}
			if laggedCount := conn.laggedCount.Load(); laggedCount >= int32(handlerInstance.cfg.LagEvictionThreshold) {
				slog.Warn("broker/marketdata: WS slow client evicted",
					"lagged_count", laggedCount,
					"threshold", handlerInstance.cfg.LagEvictionThreshold,
				)
				return
			}
			if err := handlerInstance.writeFrame(conn, frame); err != nil {
				return
			}
		case <-pingTicker.C:
			_ = conn.socket.SetWriteDeadline(time.Now().Add(handlerInstance.cfg.WriteTimeout))
			if err := conn.socket.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Debug("broker/marketdata: WS ping failed", "err", err)
				return
			}
		}
	}
}

// writeFrame marshals + writes one frame. Marshal errors are logged
// and swallowed (bad frame → skip, don't kill the socket); write
// errors propagate so writeLoop can Disconnect.
func (handlerInstance *wsHandler) writeFrame(conn *Conn, frame outFrame) error {
	data, marshalErr := frame.marshal(handlerInstance.fanOut.location)
	if marshalErr != nil {
		slog.Error("broker/marketdata: WS marshal failed", "err", marshalErr)
		return nil
	}
	_ = conn.socket.SetWriteDeadline(time.Now().Add(handlerInstance.cfg.WriteTimeout))
	if writeErr := conn.socket.WriteMessage(websocket.TextMessage, data); writeErr != nil {
		slog.Debug("broker/marketdata: WS write failed", "err", writeErr)
		return writeErr
	}
	return nil
}

// handleSubscribe validates each inbound sub item, enqueues an ack
// frame carrying per-sub status, and installs the accepted subs on
// the FanOut (which seeds snapshot subs internally — R2). Ack is
// enqueued BEFORE the seed packets so the client always sees ack
// first in strict order (WS protocol §3.1).
func (handlerInstance *wsHandler) handleSubscribe(conn *Conn, items []wsInboundSubItem) {
	accepted := make([]subRequest, 0, len(items))
	results := make([]wsAckResult, len(items))
	for index, item := range items {
		result := wsAckResult{
			Stream:     item.Stream,
			Instrument: item.Instrument,
			Timeframe:  item.Timeframe,
		}
		req, validationErr := handlerInstance.validateSubItem(item)
		if validationErr != "" {
			result.Status = "error"
			result.Error = validationErr
			results[index] = result
			continue
		}
		result.Status = "ok"
		results[index] = result
		accepted = append(accepted, req)
	}
	// Enqueue ack first.
	conn.Enqueue(ackFrame{results: results})
	// Install accepted subs — seeds are enqueued AFTER ack by Subscribe.
	if len(accepted) > 0 {
		handlerInstance.fanOut.Subscribe(conn, accepted)
	}
}

// handleUnsubscribe validates each inbound sub item (same shape as
// subscribe; the WS protocol doesn't ack unsubscribe per §2.2) and
// removes accepted ones. Invalid items are silently dropped — nothing
// to ack.
func (handlerInstance *wsHandler) handleUnsubscribe(conn *Conn, items []wsInboundSubItem) {
	accepted := make([]subRequest, 0, len(items))
	for _, item := range items {
		req, validationErr := handlerInstance.validateSubItem(item)
		if validationErr != "" {
			continue
		}
		accepted = append(accepted, req)
	}
	if len(accepted) > 0 {
		handlerInstance.fanOut.Unsubscribe(conn, accepted)
	}
}

// validateSubItem checks one inbound sub for: valid stream, non-empty
// instrument on the FanOut's tape-learned list, and (for candle subs)
// valid timeframe. Returns the parsed subRequest + "" on success, or
// zero + a human-readable error string on failure (per WS protocol
// §3.1 error reasons: "unknown instrument", "bad timeframe",
// "bad stream").
func (handlerInstance *wsHandler) validateSubItem(item wsInboundSubItem) (subRequest, string) {
	parsedStream, streamOK := parseStream(item.Stream)
	if !streamOK {
		return subRequest{}, "bad stream"
	}
	if item.Instrument == "" {
		return subRequest{}, "instrument required"
	}
	if !handlerInstance.fanOut.KnownInstrument(contract.InstrumentID(item.Instrument)) {
		return subRequest{}, "unknown instrument"
	}
	if parsedStream == streamSnapshot {
		// Snapshot subs ignore any timeframe field — TF stays 0.
		return subRequest{
			Stream:     streamSnapshot,
			Instrument: contract.InstrumentID(item.Instrument),
		}, ""
	}
	// Candle: timeframe required + on allowlist.
	if item.Timeframe == "" {
		return subRequest{}, "bad timeframe"
	}
	parsedTF, tfOK := ParseTimeframe(item.Timeframe)
	if !tfOK {
		return subRequest{}, "bad timeframe"
	}
	return subRequest{
		Stream:     streamCandle,
		Instrument: contract.InstrumentID(item.Instrument),
		TF:         parsedTF,
	}, ""
}

// --- JSON wire shapes (inbound) ---------------------------------------

type wsInboundFrame struct {
	Type string              `json:"type"`
	Subs []wsInboundSubItem  `json:"subs"`
}

type wsInboundSubItem struct {
	Stream     string `json:"stream"`
	Instrument string `json:"instrument"`
	Timeframe  string `json:"timeframe,omitempty"`
}
