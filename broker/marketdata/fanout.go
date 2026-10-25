package marketdata

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"simulator/contract"
)

// stream names the two live WS feeds the client can subscribe to.
// Values and their wire string forms match the WS protocol doc §2.1.
type stream int

const (
	streamSnapshot stream = iota
	streamCandle
)

func (name stream) String() string {
	switch name {
	case streamSnapshot:
		return "snapshot"
	case streamCandle:
		return "candle"
	}
	return "invalid"
}

// parseStream maps the wire string to a stream. Off-allowlist returns
// (0, false). The WS handler nacks the sub with "bad stream".
func parseStream(wireName string) (stream, bool) {
	switch wireName {
	case "snapshot":
		return streamSnapshot, true
	case "candle":
		return streamCandle, true
	}
	return 0, false
}

// subKey keys the FanOut's subscriber inventory (BODIES §3.1).
// ExchangeID is NOT part of the key because one FanOut lives per
// exchange instance — the ExchangeID is implicit in which FanOut you
// are talking to (BODIES §3.0 [CORRECTS] vs the base IMPL doc).
//
// TF is zero for snapshot subs (BODIES §3.1).
type subKey struct {
	Stream     stream
	Instrument contract.InstrumentID
	TF         Timeframe
}

// subRequest is one unit of a batch subscribe/unsubscribe, after the
// WS handler validated the inbound JSON item. Lives on the Conn's
// request path only; pre-validated: Instrument is tape-known, Stream is
// one of {snapshot, candle}, TF is on the allowlist (and == 0 for
// snapshot).
type subRequest struct {
	Stream     stream
	Instrument contract.InstrumentID
	TF         Timeframe
}

// --- outbound frames -------------------------------------------------
//
// Everything the server writes to a client goes through the per-Conn
// outbound channel, drained by the sole-writer writeLoop. The channel
// element is the outFrame interface so one queue preserves the ordering
// subscribe→ack→seed→live-push without needing multiple channels.

// outFrame is one serializable thing the WS writeLoop will push to the
// socket. Implementers hold their own fields (no refs back to the
// FanOut) so marshaling can proceed outside the FanOut's RLock.
type outFrame interface {
	marshal(location *time.Location) ([]byte, error)
}

// snapshotFrame is one live snapshot push for one instrument (model-a:
// whole picture re-emitted on any field change). phase is stamped by
// the publisher at build time — the writer and seed-build goroutines
// both read MarketSnapshot.Phase (atomic.Int32-backed as of Stage 5).
type snapshotFrame struct {
	instrument     contract.InstrumentID
	instrumentSnap *InstSnap
	phase          SessionPhase
}

func (frame snapshotFrame) marshal(_ *time.Location) ([]byte, error) {
	// Guard: instrumentSnap is nil when a KindSession event fires before
	// the first KindQuote/KindTrade/KindDepth for an instrument (i.e. the
	// instrument has not yet been tape-learned). Clients that subscribe
	// before the first order is placed can receive this frame. Return a
	// minimal snapshot carrying the current phase with all price flags
	// false so the client knows the phase without a nil-deref panic.
	if frame.instrumentSnap == nil {
		return json.Marshal(wsSnapshotFrame{
			Type:       "snapshot",
			Instrument: string(frame.instrument),
			Data:       wsSnapshotData{Phase: frame.phase.String()},
		})
	}

	// Compute change_pct — the ONE float on the wire. Zero when the
	// session base isn't seeded yet (HasOpen=false) OR when SessionOpen
	// is 0 (defensive against div-by-zero if the exchange ever reports
	// a 0 open price).
	var changePct float64
	if frame.instrumentSnap.HasOpen && frame.instrumentSnap.SessionOpen != 0 {
		changePct = float64(frame.instrumentSnap.LastPrice-frame.instrumentSnap.SessionOpen) /
			float64(frame.instrumentSnap.SessionOpen) * 100.0
	}

	bids := make([]wsDepthLevel, len(frame.instrumentSnap.Depth.Bids))
	for index, bid := range frame.instrumentSnap.Depth.Bids {
		bids[index] = wsDepthLevel{Price: int64(bid.Price), Qty: int64(bid.TotalQty)}
	}
	asks := make([]wsDepthLevel, len(frame.instrumentSnap.Depth.Asks))
	for index, ask := range frame.instrumentSnap.Depth.Asks {
		asks[index] = wsDepthLevel{Price: int64(ask.Price), Qty: int64(ask.TotalQty)}
	}

	return json.Marshal(wsSnapshotFrame{
		Type:       "snapshot",
		Instrument: string(frame.instrument),
		Data: wsSnapshotData{
			Phase:     frame.phase.String(),
			LastPrice: int64(frame.instrumentSnap.LastPrice),
			HasLast:   frame.instrumentSnap.HasLast,
			ChangePct: changePct,
			Quote: wsQuote{
				BestBid: int64(frame.instrumentSnap.Quote.BestBid),
				BidQty:  int64(frame.instrumentSnap.Quote.BidQty),
				HasBid:  frame.instrumentSnap.Quote.HasBid,
				BestAsk: int64(frame.instrumentSnap.Quote.BestAsk),
				AskQty:  int64(frame.instrumentSnap.Quote.AskQty),
				HasAsk:  frame.instrumentSnap.Quote.HasAsk,
			},
			Depth: wsDepth{Bids: bids, Asks: asks},
		},
	})
}

// candleFrame is one finalized bar push (model-A: finalized bars only;
// intra-bar live movement rides the snapshot's last_price).
type candleFrame struct {
	bar       Candle
	timeframe Timeframe
}

func (frame candleFrame) marshal(location *time.Location) ([]byte, error) {
	return json.Marshal(wsCandleFrame{
		Type:       "candle",
		Instrument: string(frame.bar.Instrument),
		Timeframe:  timeframeString(frame.timeframe),
		Bar: BarDTO{
			Start:  frame.bar.Start.In(location).Format(naiveLocalLayout),
			End:    frame.bar.End.In(location).Format(naiveLocalLayout),
			Open:   int64(frame.bar.Open),
			High:   int64(frame.bar.High),
			Low:    int64(frame.bar.Low),
			Close:  int64(frame.bar.Close),
			Volume: int64(frame.bar.Volume),
		},
	})
}

// ackFrame carries per-sub validation results for one subscribe batch
// (WS protocol §3.1 — "A batch can partly fail, so the ack reports per-
// sub"). Each result has status "ok" or "error" with a message.
type ackFrame struct {
	results []wsAckResult
}

func (frame ackFrame) marshal(_ *time.Location) ([]byte, error) {
	return json.Marshal(wsAckFrame{Type: "ack", Results: frame.results})
}

// connectedFrame is the first frame the server sends on every new
// connection. Establishes the tz context for every naive-local time in
// the session (WS protocol §1).
type connectedFrame struct {
	market   string
	timezone string
}

func (frame connectedFrame) marshal(_ *time.Location) ([]byte, error) {
	return json.Marshal(wsConnectedFrame{Type: "connected", Market: frame.market, Timezone: frame.timezone})
}

// --- Conn -------------------------------------------------------------

// Conn is one WebSocket client. Owned jointly by the FanOut (which
// references it from its subs/byConn maps) and the ws handler (which
// runs the read/write loops on it). Cleaned up on Disconnect — closes
// outbound, closes the socket, idempotent via CAS.
//
// Zero value is NOT usable — construct via NewConn.
type Conn struct {
	socket       *websocket.Conn
	outbound     chan outFrame // buffered; writeLoop drains, Publish/Subscribe enqueue
	laggedCount  atomic.Int32  // consecutive drops since last successful enqueue
	disconnected atomic.Bool
}

// NewConn wraps a freshly-upgraded gorilla socket with an outbound
// buffer sized per WSConfig.ConnBuffer.
func NewConn(socket *websocket.Conn, bufferSize int) *Conn {
	return &Conn{
		socket:   socket,
		outbound: make(chan outFrame, bufferSize),
	}
}

// Enqueue tries to push one frame into the conn's outbound buffer,
// non-blocking. On success resets the lag counter to 0; on a full
// buffer bumps lag and drops the frame.
//
// Returns true if queued, false if dropped. Callers may ignore the
// return (both Publish and the join-seed do — the lag counter is the
// signal, not the per-call result).
func (conn *Conn) Enqueue(frame outFrame) bool {
	select {
	case conn.outbound <- frame:
		conn.laggedCount.Store(0)
		return true
	default:
		conn.laggedCount.Add(1)
		return false
	}
}

// --- FanOut -----------------------------------------------------------

// FanOut routes outbound frames from the ingestor + the WS handler to
// subscribed Conns. One per exchange instance (BODIES §3.0 — this is
// why subKey no longer carries ExchangeID; it's implicit in the
// FanOut you're calling).
//
// Concurrency: one RWMutex on the inventory. Publish takes the RLock
// on the hot path (many ingestors across many instances publish in
// parallel; within one instance only one ingestor so RLock is really
// just "the lock Subscribe/Unsubscribe/Disconnect wait on"). Writers
// (Subscribe/Unsubscribe/Disconnect) take the write-lock, rarely.
//
// Zero value is NOT usable — construct via NewFanOut.
type FanOut struct {
	mu             sync.RWMutex
	subs           map[subKey]map[*Conn]struct{}
	byConn         map[*Conn][]subKey // reverse index → O(1) disconnect purge
	marketSnapshot *MarketSnapshot
	market         string
	location       *time.Location
}

// NewFanOut constructs a FanOut bound to one exchange instance's
// MarketSnapshot. market + location come from the Stage-7 wire-up
// (WireInstance resolves them from the market registry via tzFor).
func NewFanOut(marketSnapshot *MarketSnapshot, market string, location *time.Location) *FanOut {
	if marketSnapshot == nil {
		panic("broker/marketdata: NewFanOut requires non-nil MarketSnapshot")
	}
	if location == nil {
		panic("broker/marketdata: NewFanOut requires non-nil location")
	}
	return &FanOut{
		subs:           make(map[subKey]map[*Conn]struct{}),
		byConn:         make(map[*Conn][]subKey),
		marketSnapshot: marketSnapshot,
		market:         market,
		location:       location,
	}
}

// Market returns the market label (for the WS connected frame + logs).
func (fanOut *FanOut) Market() string { return fanOut.market }

// Location returns the market tz (for WS wire time formatting).
func (fanOut *FanOut) Location() *time.Location { return fanOut.location }

// Snap returns the backing MarketSnapshot — for the WS handler, which
// needs to call Get for seed-build and Phase for seed-stamp. Internal
// use only; do not mutate the returned snapshot's state outside the
// ingestor goroutine.
func (fanOut *FanOut) Snap() *MarketSnapshot { return fanOut.marketSnapshot }

// KnownInstrument returns true iff the instrument has appeared on the
// tape (i.e. MarketSnapshot has a slot for it). Used by the WS handler
// to nack unknown-instrument subscribe requests (DESIGN §8.3).
//
// Race window: a client that subscribes before the first tape event
// for an instrument sees "unknown instrument" nack. In practice L7
// delays client bindings until the first ticks have flowed.
func (fanOut *FanOut) KnownInstrument(instrument contract.InstrumentID) bool {
	return fanOut.marketSnapshot.Get(instrument) != nil
}

// PublishSnapshot routes one snapshot frame to every subscriber of
// (streamSnapshot, instrument, 0). Non-blocking per Conn — full
// buffers drop + bump lag (BODIES §3.2).
func (fanOut *FanOut) PublishSnapshot(instrument contract.InstrumentID, instrumentSnap *InstSnap, phase SessionPhase) {
	fanOut.publish(
		subKey{Stream: streamSnapshot, Instrument: instrument},
		snapshotFrame{instrument: instrument, instrumentSnap: instrumentSnap, phase: phase},
	)
}

// PublishCandle routes one finalized bar to every subscriber of
// (streamCandle, bar.Instrument, timeframe).
func (fanOut *FanOut) PublishCandle(bar Candle, timeframe Timeframe) {
	fanOut.publish(
		subKey{Stream: streamCandle, Instrument: bar.Instrument, TF: timeframe},
		candleFrame{bar: bar, timeframe: timeframe},
	)
}

// publish is the shared inner path: look up subscribers under RLock,
// non-blocking Enqueue to each. N ingestors across multiple FanOuts
// run this in parallel (one FanOut has one writer, so RLock is really
// just "the thing Subscribe/Disconnect wait on").
func (fanOut *FanOut) publish(key subKey, frame outFrame) {
	fanOut.mu.RLock()
	subscribedConns := fanOut.subs[key]
	for subscribedConn := range subscribedConns {
		subscribedConn.Enqueue(frame)
	}
	fanOut.mu.RUnlock()
}

// Subscribe installs a batch of pre-validated subs under ONE write-
// lock (BODIES §3.3 — the batch win). R2: for each accepted snapshot
// sub, enqueue a seed packet (the current retained InstSnap, stamped
// with the current Phase). Candle subs do NOT seed — WS protocol
// §3.1 "An accepted candle sub is bare (history comes from GET
// /candles)."
//
// All requests are assumed pre-validated by the caller (ws handler
// does instrument/timeframe/stream validation; FanOut just routes).
func (fanOut *FanOut) Subscribe(conn *Conn, requests []subRequest) {
	fanOut.mu.Lock()
	for _, request := range requests {
		key := subKey{Stream: request.Stream, Instrument: request.Instrument, TF: request.TF}
		if fanOut.subs[key] == nil {
			fanOut.subs[key] = make(map[*Conn]struct{})
		}
		fanOut.subs[key][conn] = struct{}{}
		fanOut.byConn[conn] = append(fanOut.byConn[conn], key)
	}
	fanOut.mu.Unlock()

	// Seed each snapshot sub (R2). Done outside the lock so a slow
	// socket queue doesn't block other FanOut writers.
	for _, request := range requests {
		if request.Stream != streamSnapshot {
			continue
		}
		instrumentSnap := fanOut.marketSnapshot.Get(request.Instrument)
		if instrumentSnap == nil {
			// Instrument was validated as known but may have been
			// learned-and-forgotten between validation and here. In
			// practice can't happen (snapshots are retained, never
			// cleared). Defensive skip.
			continue
		}
		conn.Enqueue(snapshotFrame{
			instrument:     request.Instrument,
			instrumentSnap: instrumentSnap,
			phase:          fanOut.marketSnapshot.Phase(),
		})
	}
}

// Unsubscribe removes a batch of subs for one Conn under one write-
// lock. Idempotent per-sub: an unsubscribe for a sub the client never
// subscribed to is a silent no-op (WS protocol §2.2 doesn't require
// nack on this path).
func (fanOut *FanOut) Unsubscribe(conn *Conn, requests []subRequest) {
	fanOut.mu.Lock()
	defer fanOut.mu.Unlock()
	for _, request := range requests {
		key := subKey{Stream: request.Stream, Instrument: request.Instrument, TF: request.TF}
		if subscribedConns, ok := fanOut.subs[key]; ok {
			delete(subscribedConns, conn)
			if len(subscribedConns) == 0 {
				delete(fanOut.subs, key)
			}
		}
		// Prune the one key from byConn[conn]'s slice.
		if subscribedKeys, ok := fanOut.byConn[conn]; ok {
			for index, existingKey := range subscribedKeys {
				if existingKey == key {
					fanOut.byConn[conn] = append(subscribedKeys[:index], subscribedKeys[index+1:]...)
					break
				}
			}
		}
	}
}

// Disconnect purges all of a Conn's subs, closes its outbound buffer,
// and closes its socket. Idempotent via CAS (readLoop + writeLoop
// typically both race to Disconnect on error paths).
//
// Ordering: take the write-lock → remove from subs/byConn → unlock →
// close(conn.outbound) → close(socket). Once we hold the write-lock,
// no Publish can see this Conn (via the subs map); any Publish mid-
// send to this Conn's outbound is holding the RLock and has already
// committed its atomic select (success or default-drop) before we
// acquired the write-lock. So close(conn.outbound) after unlock is
// race-free: no sender remains.
func (fanOut *FanOut) Disconnect(conn *Conn) {
	if !conn.disconnected.CompareAndSwap(false, true) {
		return
	}
	fanOut.mu.Lock()
	for _, key := range fanOut.byConn[conn] {
		delete(fanOut.subs[key], conn)
		if len(fanOut.subs[key]) == 0 {
			delete(fanOut.subs, key)
		}
	}
	delete(fanOut.byConn, conn)
	fanOut.mu.Unlock()

	close(conn.outbound)
	if err := conn.socket.Close(); err != nil {
		slog.Debug("broker/marketdata: WS socket close err (benign)", "err", err)
	}
}

// SubscriptionCount returns the total number of (key, conn) pairs
// currently installed. For tests + ops logging only; not a hot path.
func (fanOut *FanOut) SubscriptionCount() int {
	fanOut.mu.RLock()
	defer fanOut.mu.RUnlock()
	count := 0
	for _, subscribedConns := range fanOut.subs {
		count += len(subscribedConns)
	}
	return count
}

// --- JSON wire shapes (private — the marshal methods above use these) -

type wsConnectedFrame struct {
	Type     string `json:"type"`
	Market   string `json:"market"`
	Timezone string `json:"timezone"`
}

type wsAckFrame struct {
	Type    string        `json:"type"`
	Results []wsAckResult `json:"results"`
}

type wsAckResult struct {
	Stream     string `json:"stream"`
	Instrument string `json:"instrument"`
	Timeframe  string `json:"timeframe,omitempty"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

type wsCandleFrame struct {
	Type       string `json:"type"`
	Instrument string `json:"instrument"`
	Timeframe  string `json:"timeframe"`
	Bar        BarDTO `json:"bar"`
}

type wsSnapshotFrame struct {
	Type       string         `json:"type"`
	Instrument string         `json:"instrument"`
	Data       wsSnapshotData `json:"data"`
}

type wsSnapshotData struct {
	Phase     string  `json:"phase"`
	LastPrice int64   `json:"last_price"`
	HasLast   bool    `json:"has_last"`
	ChangePct float64 `json:"change_pct"`
	Quote     wsQuote `json:"quote"`
	Depth     wsDepth `json:"depth"`
}

type wsQuote struct {
	BestBid int64 `json:"best_bid"`
	BidQty  int64 `json:"bid_qty"`
	HasBid  bool  `json:"has_bid"`
	BestAsk int64 `json:"best_ask"`
	AskQty  int64 `json:"ask_qty"`
	HasAsk  bool  `json:"has_ask"`
}

type wsDepth struct {
	Bids []wsDepthLevel `json:"bids"`
	Asks []wsDepthLevel `json:"asks"`
}

type wsDepthLevel struct {
	Price int64 `json:"price"`
	Qty   int64 `json:"qty"`
}

// Static assertion that all four frame types implement outFrame.
// Keeps a refactor from silently dropping one of them from the
// interface set (would otherwise only surface at the Enqueue call site).
var (
	_ outFrame = snapshotFrame{}
	_ outFrame = candleFrame{}
	_ outFrame = ackFrame{}
	_ outFrame = connectedFrame{}
)
