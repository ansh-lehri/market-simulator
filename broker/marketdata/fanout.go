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

func (s stream) String() string {
	switch s {
	case streamSnapshot:
		return "snapshot"
	case streamCandle:
		return "candle"
	}
	return "invalid"
}

// parseStream maps the wire string to a stream. Off-allowlist returns
// (0, false). The WS handler nacks the sub with "bad stream".
func parseStream(s string) (stream, bool) {
	switch s {
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
	instrument contract.InstrumentID
	snap       *InstSnap
	phase      SessionPhase
}

func (f snapshotFrame) marshal(_ *time.Location) ([]byte, error) {
	// Compute change_pct — the ONE float on the wire. Zero when the
	// session base isn't seeded yet (HasOpen=false) OR when SessionOpen
	// is 0 (defensive against div-by-zero if the exchange ever reports
	// a 0 open price).
	var changePct float64
	if f.snap.HasOpen && f.snap.SessionOpen != 0 {
		changePct = float64(f.snap.LastPrice-f.snap.SessionOpen) / float64(f.snap.SessionOpen) * 100.0
	}

	bids := make([]wsDepthLevel, len(f.snap.Depth.Bids))
	for index, bid := range f.snap.Depth.Bids {
		bids[index] = wsDepthLevel{Price: int64(bid.Price), Qty: int64(bid.TotalQty)}
	}
	asks := make([]wsDepthLevel, len(f.snap.Depth.Asks))
	for index, ask := range f.snap.Depth.Asks {
		asks[index] = wsDepthLevel{Price: int64(ask.Price), Qty: int64(ask.TotalQty)}
	}

	return json.Marshal(wsSnapshotFrame{
		Type:       "snapshot",
		Instrument: string(f.instrument),
		Data: wsSnapshotData{
			Phase:     f.phase.String(),
			LastPrice: int64(f.snap.LastPrice),
			HasLast:   f.snap.HasLast,
			ChangePct: changePct,
			Quote: wsQuote{
				BestBid: int64(f.snap.Quote.BestBid),
				BidQty:  int64(f.snap.Quote.BidQty),
				HasBid:  f.snap.Quote.HasBid,
				BestAsk: int64(f.snap.Quote.BestAsk),
				AskQty:  int64(f.snap.Quote.AskQty),
				HasAsk:  f.snap.Quote.HasAsk,
			},
			Depth: wsDepth{Bids: bids, Asks: asks},
		},
	})
}

// candleFrame is one finalized bar push (model-A: finalized bars only;
// intra-bar live movement rides the snapshot's last_price).
type candleFrame struct {
	bar Candle
	tf  Timeframe
}

func (f candleFrame) marshal(location *time.Location) ([]byte, error) {
	return json.Marshal(wsCandleFrame{
		Type:       "candle",
		Instrument: string(f.bar.Instrument),
		Timeframe:  timeframeString(f.tf),
		Bar: BarDTO{
			Start:  f.bar.Start.In(location).Format(naiveLocalLayout),
			End:    f.bar.End.In(location).Format(naiveLocalLayout),
			Open:   int64(f.bar.Open),
			High:   int64(f.bar.High),
			Low:    int64(f.bar.Low),
			Close:  int64(f.bar.Close),
			Volume: int64(f.bar.Volume),
		},
	})
}

// ackFrame carries per-sub validation results for one subscribe batch
// (WS protocol §3.1 — "A batch can partly fail, so the ack reports per-
// sub"). Each result has status "ok" or "error" with a message.
type ackFrame struct {
	results []wsAckResult
}

func (f ackFrame) marshal(_ *time.Location) ([]byte, error) {
	return json.Marshal(wsAckFrame{Type: "ack", Results: f.results})
}

// connectedFrame is the first frame the server sends on every new
// connection. Establishes the tz context for every naive-local time in
// the session (WS protocol §1).
type connectedFrame struct {
	market   string
	timezone string
}

func (f connectedFrame) marshal(_ *time.Location) ([]byte, error) {
	return json.Marshal(wsConnectedFrame{Type: "connected", Market: f.market, Timezone: f.timezone})
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
func (c *Conn) Enqueue(frame outFrame) bool {
	select {
	case c.outbound <- frame:
		c.laggedCount.Store(0)
		return true
	default:
		c.laggedCount.Add(1)
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
	mu       sync.RWMutex
	subs     map[subKey]map[*Conn]struct{}
	byConn   map[*Conn][]subKey // reverse index → O(1) disconnect purge
	snap     *MarketSnapshot
	market   string
	location *time.Location
}

// NewFanOut constructs a FanOut bound to one exchange instance's
// MarketSnapshot. market + location come from the Stage-7 wire-up
// (WireInstance resolves them from the market registry via tzFor).
func NewFanOut(snap *MarketSnapshot, market string, location *time.Location) *FanOut {
	if snap == nil {
		panic("broker/marketdata: NewFanOut requires non-nil MarketSnapshot")
	}
	if location == nil {
		panic("broker/marketdata: NewFanOut requires non-nil location")
	}
	return &FanOut{
		subs:     make(map[subKey]map[*Conn]struct{}),
		byConn:   make(map[*Conn][]subKey),
		snap:     snap,
		market:   market,
		location: location,
	}
}

// Market returns the market label (for the WS connected frame + logs).
func (f *FanOut) Market() string { return f.market }

// Location returns the market tz (for WS wire time formatting).
func (f *FanOut) Location() *time.Location { return f.location }

// Snap returns the backing MarketSnapshot — for the WS handler, which
// needs to call Get for seed-build and Phase for seed-stamp. Internal
// use only; do not mutate the returned snapshot's state outside the
// ingestor goroutine.
func (f *FanOut) Snap() *MarketSnapshot { return f.snap }

// KnownInstrument returns true iff the instrument has appeared on the
// tape (i.e. MarketSnapshot has a slot for it). Used by the WS handler
// to nack unknown-instrument subscribe requests (DESIGN §8.3).
//
// Race window: a client that subscribes before the first tape event
// for an instrument sees "unknown instrument" nack. In practice L7
// delays client bindings until the first ticks have flowed.
func (f *FanOut) KnownInstrument(instrument contract.InstrumentID) bool {
	return f.snap.Get(instrument) != nil
}

// PublishSnapshot routes one snapshot frame to every subscriber of
// (streamSnapshot, instrument, 0). Non-blocking per Conn — full
// buffers drop + bump lag (BODIES §3.2).
func (f *FanOut) PublishSnapshot(instrument contract.InstrumentID, snap *InstSnap, phase SessionPhase) {
	f.publish(
		subKey{Stream: streamSnapshot, Instrument: instrument},
		snapshotFrame{instrument: instrument, snap: snap, phase: phase},
	)
}

// PublishCandle routes one finalized bar to every subscriber of
// (streamCandle, bar.Instrument, tf).
func (f *FanOut) PublishCandle(bar Candle, tf Timeframe) {
	f.publish(
		subKey{Stream: streamCandle, Instrument: bar.Instrument, TF: tf},
		candleFrame{bar: bar, tf: tf},
	)
}

// publish is the shared inner path: look up subscribers under RLock,
// non-blocking Enqueue to each. N ingestors across multiple FanOuts
// run this in parallel (one FanOut has one writer, so RLock is really
// just "the thing Subscribe/Disconnect wait on").
func (f *FanOut) publish(key subKey, frame outFrame) {
	f.mu.RLock()
	conns := f.subs[key]
	for conn := range conns {
		conn.Enqueue(frame)
	}
	f.mu.RUnlock()
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
func (f *FanOut) Subscribe(conn *Conn, requests []subRequest) {
	f.mu.Lock()
	for _, req := range requests {
		key := subKey{Stream: req.Stream, Instrument: req.Instrument, TF: req.TF}
		if f.subs[key] == nil {
			f.subs[key] = make(map[*Conn]struct{})
		}
		f.subs[key][conn] = struct{}{}
		f.byConn[conn] = append(f.byConn[conn], key)
	}
	f.mu.Unlock()

	// Seed each snapshot sub (R2). Done outside the lock so a slow
	// socket queue doesn't block other FanOut writers.
	for _, req := range requests {
		if req.Stream != streamSnapshot {
			continue
		}
		instrumentSnap := f.snap.Get(req.Instrument)
		if instrumentSnap == nil {
			// Instrument was validated as known but may have been
			// learned-and-forgotten between validation and here. In
			// practice can't happen (snapshots are retained, never
			// cleared). Defensive skip.
			continue
		}
		conn.Enqueue(snapshotFrame{
			instrument: req.Instrument,
			snap:       instrumentSnap,
			phase:      f.snap.Phase(),
		})
	}
}

// Unsubscribe removes a batch of subs for one Conn under one write-
// lock. Idempotent per-sub: an unsubscribe for a sub the client never
// subscribed to is a silent no-op (WS protocol §2.2 doesn't require
// nack on this path).
func (f *FanOut) Unsubscribe(conn *Conn, requests []subRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, req := range requests {
		key := subKey{Stream: req.Stream, Instrument: req.Instrument, TF: req.TF}
		if conns, ok := f.subs[key]; ok {
			delete(conns, conn)
			if len(conns) == 0 {
				delete(f.subs, key)
			}
		}
		// Prune the one key from byConn[conn]'s slice.
		if keys, ok := f.byConn[conn]; ok {
			for index, existing := range keys {
				if existing == key {
					f.byConn[conn] = append(keys[:index], keys[index+1:]...)
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
// close(c.outbound) → close(socket). Once we hold the write-lock, no
// Publish can see this Conn (via the subs map); any Publish mid-send
// to this Conn's outbound is holding the RLock and has already
// committed its atomic select (success or default-drop) before we
// acquired the write-lock. So close(c.outbound) after unlock is race-
// free: no sender remains.
func (f *FanOut) Disconnect(conn *Conn) {
	if !conn.disconnected.CompareAndSwap(false, true) {
		return
	}
	f.mu.Lock()
	for _, key := range f.byConn[conn] {
		delete(f.subs[key], conn)
		if len(f.subs[key]) == 0 {
			delete(f.subs, key)
		}
	}
	delete(f.byConn, conn)
	f.mu.Unlock()

	close(conn.outbound)
	if err := conn.socket.Close(); err != nil {
		slog.Debug("broker/marketdata: WS socket close err (benign)", "err", err)
	}
}

// SubscriptionCount returns the total number of (key, conn) pairs
// currently installed. For tests + ops logging only; not a hot path.
func (f *FanOut) SubscriptionCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	count := 0
	for _, conns := range f.subs {
		count += len(conns)
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
