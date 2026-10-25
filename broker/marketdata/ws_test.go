package marketdata

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"simulator/exchange/book"
	"simulator/exchange/emit"
)

// --- helpers ---------------------------------------------------------

// testWSConfig is a sane WSConfig for integration tests. Timeouts are
// short-ish so a hang surfaces as a test timeout rather than a 10-min
// wait; the lag threshold is intentionally small so eviction tests
// land quickly.
func testWSConfig() WSConfig {
	return WSConfig{
		ConnBuffer:           4,
		LagEvictionThreshold: 3,
		PingInterval:         1 * time.Second,
		ReadTimeout:          3 * time.Second,
		WriteTimeout:         1 * time.Second,
	}
}

// wsTestServer stands up a httptest.Server with a wsHandler bound to a
// fresh FanOut over a MarketSnapshot. Returns the server (for the ws://
// URL), the FanOut (so tests can Publish), and the Snapshot (so tests
// can SetX to tape-learn instruments).
func wsTestServer(t *testing.T) (*httptest.Server, *FanOut, *MarketSnapshot) {
	t.Helper()
	snap := NewMarketSnapshot("E1")
	fanOut := NewFanOut(snap, "IN", time.UTC)
	handler := NewWSHandler(fanOut, testWSConfig())
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, fanOut, snap
}

// wsURL converts an http:// test server URL into ws://.
func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// dialWS opens a WebSocket client against the test server and sets a
// per-read deadline on the connection so a server-side hang surfaces
// as a test error rather than blocking forever.
func dialWS(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	socket, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = socket.Close() })
	_ = socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	return socket
}

// readOneJSON reads one text frame and json-decodes it into a map. The
// map form lets tests probe the "type" discriminator before narrowing.
func readOneJSON(t *testing.T, socket *websocket.Conn) map[string]any {
	t.Helper()
	_, data, err := socket.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode %q: %v", data, err)
	}
	return out
}

// writeJSONFrame sends a text frame carrying the given JSON-encodable
// payload.
func writeJSONFrame(t *testing.T, socket *websocket.Conn, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := socket.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// --- WSConfig validation --------------------------------------------

func TestWSConfig_Validate_ZeroFieldsRejected(t *testing.T) {
	// Every field is required > 0. Each case zeroes one.
	base := testWSConfig()
	cases := map[string]func(cfg *WSConfig){
		"ConnBuffer":           func(cfg *WSConfig) { cfg.ConnBuffer = 0 },
		"LagEvictionThreshold": func(cfg *WSConfig) { cfg.LagEvictionThreshold = 0 },
		"PingInterval":         func(cfg *WSConfig) { cfg.PingInterval = 0 },
		"ReadTimeout":          func(cfg *WSConfig) { cfg.ReadTimeout = 0 },
		"WriteTimeout":         func(cfg *WSConfig) { cfg.WriteTimeout = 0 },
	}
	for name, zero := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := base
			zero(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate: got nil, want ErrBadWSConfig")
			}
		})
	}
}

// --- NewWSHandler panics --------------------------------------------

func TestNewWSHandler_PanicsOnNilFanOut(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil FanOut")
		}
	}()
	NewWSHandler(nil, testWSConfig())
}

func TestNewWSHandler_PanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on invalid Config")
		}
	}()
	NewWSHandler(NewFanOut(NewMarketSnapshot("E1"), "IN", time.UTC), WSConfig{})
}

// --- Connected frame -------------------------------------------------

func TestWS_Connect_SendsConnectedFrameFirst(t *testing.T) {
	server, _, _ := wsTestServer(t)
	socket := dialWS(t, server)

	frame := readOneJSON(t, socket)
	if frame["type"] != "connected" {
		t.Fatalf("first frame type: got %q, want connected", frame["type"])
	}
	if frame["market"] != "IN" {
		t.Errorf("market: got %q, want IN", frame["market"])
	}
	if frame["timezone"] != "UTC" {
		t.Errorf("timezone: got %q, want UTC", frame["timezone"])
	}
}

// --- Subscribe + ack -------------------------------------------------

func TestWS_Subscribe_SnapshotStream_AckOK_ThenSeed(t *testing.T) {
	server, _, snap := wsTestServer(t)
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050)
	snap.SetQuote("ACME", book.QuoteState{BestAsk: 10_060, HasAsk: true})

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket) // drain connected

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})

	// Ack first (WS protocol §3.1 — ack-then-seed order).
	ack := readOneJSON(t, socket)
	if ack["type"] != "ack" {
		t.Fatalf("expected ack, got type=%q", ack["type"])
	}
	results, ok := ack["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("ack.results: got %+v", ack["results"])
	}
	first := results[0].(map[string]any)
	if first["status"] != "ok" {
		t.Errorf("result status: got %q, want ok", first["status"])
	}

	// Then the snapshot seed.
	seed := readOneJSON(t, socket)
	if seed["type"] != "snapshot" {
		t.Fatalf("expected snapshot seed after ack, got type=%q", seed["type"])
	}
	if seed["instrument"] != "ACME" {
		t.Errorf("seed instrument: got %q, want ACME", seed["instrument"])
	}
	data := seed["data"].(map[string]any)
	if data["phase"] != "open" {
		t.Errorf("seed phase: got %q, want open", data["phase"])
	}
	if int(data["last_price"].(float64)) != 10_050 {
		t.Errorf("seed last_price: got %v, want 10050", data["last_price"])
	}
}

func TestWS_Subscribe_CandleStream_AckOK_NoSeed(t *testing.T) {
	// R2: candle subs do NOT seed — the next frame after ack must come
	// from a Publish, not from the server volunteering one.
	//
	// We can't assert "no frame arrives for N ms" cleanly on gorilla
	// (a read-deadline expiry breaks the socket for subsequent reads),
	// so we assert the stronger end-to-end property: after ack, the
	// NEXT frame on the wire is the Publish's candle — never a
	// snapshot (which would be the R2 violation). The FanOut-level
	// TestSubscribe_CandleSub_DoesNOTSeed proves no seed is enqueued
	// server-side; this check verifies the client sees it that way.
	server, fanOut, snap := wsTestServer(t)
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket) // connected

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "candle", "instrument": "ACME", "timeframe": "5m"}},
	})

	ack := readOneJSON(t, socket)
	if ack["type"] != "ack" {
		t.Fatalf("expected ack, got %q", ack["type"])
	}

	start := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	fanOut.PublishCandle(Candle{
		Market: "IN", Instrument: "ACME",
		Start: start, End: start.Add(5 * time.Minute),
		Open: 100, High: 110, Low: 90, Close: 105, Volume: 500,
	}, TF5m)

	frame := readOneJSON(t, socket)
	if frame["type"] == "snapshot" {
		t.Fatal("candle sub leaked a snapshot seed (R2 violation)")
	}
	if frame["type"] != "candle" {
		t.Fatalf("expected candle, got %q", frame["type"])
	}
	if frame["timeframe"] != "5m" {
		t.Errorf("timeframe: got %q, want 5m", frame["timeframe"])
	}
}

// --- Per-sub ack with mixed ok/error --------------------------------

func TestWS_Subscribe_BatchMixedOkAndError_PerSubAck(t *testing.T) {
	server, _, snap := wsTestServer(t)
	snap.SetLast("ACME", 10_050) // known, seeds a snapshot slot
	// BETA not tape-learned — subscriptions are accepted regardless of
	// tape-learn state (the validator no longer rejects unknown instruments
	// to avoid breaking clients that connect before the first tape event).

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket)

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{
			{"stream": "snapshot", "instrument": "ACME"},                    // ok, tape-learned
			{"stream": "snapshot", "instrument": "BETA"},                    // ok, not yet tape-learned but accepted
			{"stream": "candle", "instrument": "ACME", "timeframe": "15m"}, // error: bad timeframe
			{"stream": "trade", "instrument": "ACME"},                       // error: bad stream
		},
	})

	ack := readOneJSON(t, socket)
	results := ack["results"].([]any)
	if len(results) != 4 {
		t.Fatalf("results len: got %d, want 4", len(results))
	}
	want := []struct {
		status string
		errMsg string
	}{
		{"ok", ""},
		{"ok", ""},           // BETA accepted — no "unknown instrument" rejection
		{"error", "bad timeframe"},
		{"error", "bad stream"},
	}
	for index, w := range want {
		r := results[index].(map[string]any)
		if r["status"] != w.status {
			t.Errorf("result[%d].status: got %q, want %q", index, r["status"], w.status)
		}
		if w.errMsg != "" && r["error"] != w.errMsg {
			t.Errorf("result[%d].error: got %q, want %q", index, r["error"], w.errMsg)
		}
	}
}

// --- Snapshot delivery after Publish --------------------------------

func TestWS_PublishSnapshot_DeliveredToSubscriber(t *testing.T) {
	server, fanOut, snap := wsTestServer(t)
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket) // connected

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	_ = readOneJSON(t, socket) // ack
	_ = readOneJSON(t, socket) // seed

	// Ingestor-side mutation + publish.
	snap.SetLast("ACME", 11_000)
	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())

	frame := readOneJSON(t, socket)
	if frame["type"] != "snapshot" {
		t.Fatalf("expected snapshot push, got %q", frame["type"])
	}
	data := frame["data"].(map[string]any)
	if int(data["last_price"].(float64)) != 11_000 {
		t.Errorf("last_price: got %v, want 11000", data["last_price"])
	}
}

// --- Unsubscribe stops delivery -------------------------------------

func TestWS_Unsubscribe_StopsDelivery(t *testing.T) {
	server, fanOut, snap := wsTestServer(t)
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket)

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	_ = readOneJSON(t, socket) // ack
	_ = readOneJSON(t, socket) // seed

	writeJSONFrame(t, socket, map[string]any{
		"type": "unsubscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	// Give the server a moment to process the unsubscribe.
	time.Sleep(50 * time.Millisecond)

	// Publish — must NOT reach the client.
	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())

	_ = socket.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, data, err := socket.ReadMessage()
	if err == nil {
		t.Fatalf("delivery after unsubscribe: unexpected frame %q", data)
	}
}

// --- Change_pct -----------------------------------------------------

// Load-bearing: change_pct is the ONE float on the wire. Formula is
// (last_price − sessionOpen) / sessionOpen × 100 (WS protocol §4).
// Verify end-to-end: session opens at 10000, trade lifts to 10200 →
// change_pct = +2.0.
func TestWS_SnapshotPush_ChangePctFormula(t *testing.T) {
	server, fanOut, snap := wsTestServer(t)
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_000) // seeds sessionOpen=10000
	snap.SetLast("ACME", 10_200) // doesn't re-seed; LastPrice=10200

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket) // connected

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	_ = readOneJSON(t, socket) // ack
	seed := readOneJSON(t, socket)
	data := seed["data"].(map[string]any)
	gotPct := data["change_pct"].(float64)
	if gotPct < 1.99 || gotPct > 2.01 {
		t.Errorf("change_pct: got %v, want ≈ 2.0", gotPct)
	}
	// Publish doesn't move the price — pct stays the same.
	fanOut.PublishSnapshot("ACME", snap.Get("ACME"), snap.Phase())
	live := readOneJSON(t, socket)
	liveData := live["data"].(map[string]any)
	liveGot := liveData["change_pct"].(float64)
	if liveGot < 1.99 || liveGot > 2.01 {
		t.Errorf("live push change_pct: got %v, want ≈ 2.0", liveGot)
	}
}

// --- No-session (HasOpen=false): change_pct is 0 (not NaN / Inf) ----

func TestWS_SnapshotPush_NoSessionBase_ChangePctZero(t *testing.T) {
	server, _, snap := wsTestServer(t)
	// NO ApplySession → phase=Unknown → first trade does NOT seed HasOpen.
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket)

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	_ = readOneJSON(t, socket) // ack
	seed := readOneJSON(t, socket)
	data := seed["data"].(map[string]any)
	if got := data["change_pct"].(float64); got != 0.0 {
		t.Errorf("change_pct w/o session base: got %v, want 0 (HasOpen=false ⇒ no base, no divide-by-zero)", got)
	}
}

// --- Candle bar renders in market tz -------------------------------

func TestWS_CandleFrame_BarTimesInMarketTz(t *testing.T) {
	// Build a FanOut with an IST location so bar.Start = UTC 03:30
	// renders as "2026-10-10T09:00:00" in the wire frame.
	istLocation, ok := tzFor("IN")
	if !ok {
		t.Fatal("tzFor(IN) missed")
	}
	snap := NewMarketSnapshot("E1")
	fanOut := NewFanOut(snap, "IN", istLocation)
	handler := NewWSHandler(fanOut, testWSConfig())
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	connectedFrame := readOneJSON(t, socket)
	if connectedFrame["timezone"] != "Asia/Kolkata" {
		t.Errorf("connected tz: got %q, want Asia/Kolkata", connectedFrame["timezone"])
	}

	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "candle", "instrument": "ACME", "timeframe": "5m"}},
	})
	_ = readOneJSON(t, socket) // ack

	start := time.Date(2026, 10, 10, 3, 30, 0, 0, time.UTC) // == IST 09:00
	fanOut.PublishCandle(Candle{
		Market: "IN", Instrument: "ACME",
		Start: start, End: start.Add(5 * time.Minute),
		Open: 100, High: 110, Low: 90, Close: 105, Volume: 500,
	}, TF5m)

	frame := readOneJSON(t, socket)
	bar := frame["bar"].(map[string]any)
	if bar["start"] != "2026-10-10T09:00:00" {
		t.Errorf("bar.start: got %q, want %q (IST render)", bar["start"], "2026-10-10T09:00:00")
	}
	if bar["end"] != "2026-10-10T09:05:00" {
		t.Errorf("bar.end: got %q, want %q", bar["end"], "2026-10-10T09:05:00")
	}
}

// --- No unit/currency field on wire (D-3 / P6-D33) -----------------

func TestWS_FrameShape_NoUnitField(t *testing.T) {
	server, fanOut, snap := wsTestServer(t)
	snap.ApplySession(emit.SessionOpened)
	snap.SetLast("ACME", 10_050)
	snap.SetQuote("ACME", book.QuoteState{
		BestBid: 10_040, BidQty: 100, HasBid: true,
		BestAsk: 10_060, AskQty: 200, HasAsk: true,
	})
	snap.SetDepth("ACME", emit.DepthSnapshot{
		Bids: []emit.DepthLevel{{Price: 10_040, TotalQty: 100}},
		Asks: []emit.DepthLevel{{Price: 10_060, TotalQty: 200}},
	})

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket)
	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	_ = readOneJSON(t, socket) // ack

	// Read the seed raw so we can grep its bytes.
	_, data, err := socket.ReadMessage()
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	body := string(data)
	for _, forbidden := range []string{`"unit"`, `"currency"`} {
		if strings.Contains(body, forbidden) {
			t.Errorf("snapshot wire leaked forbidden field %s: %s", forbidden, body)
		}
	}

	// Candle frame too.
	start := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	fanOut.PublishCandle(Candle{
		Market: "IN", Instrument: "ACME",
		Start: start, End: start.Add(5 * time.Minute),
		Open: 100, High: 110, Low: 90, Close: 105, Volume: 500,
	}, TF5m)
	// Also need to subscribe to candle.
	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "candle", "instrument": "ACME", "timeframe": "5m"}},
	})
	_ = readOneJSON(t, socket) // ack

	fanOut.PublishCandle(Candle{
		Market: "IN", Instrument: "ACME",
		Start: start, End: start.Add(5 * time.Minute),
		Open: 100, High: 110, Low: 90, Close: 105, Volume: 500,
	}, TF5m)
	_, candleData, err := socket.ReadMessage()
	if err != nil {
		t.Fatalf("read candle: %v", err)
	}
	body = string(candleData)
	for _, forbidden := range []string{`"unit"`, `"currency"`} {
		if strings.Contains(body, forbidden) {
			t.Errorf("candle wire leaked forbidden field %s: %s", forbidden, body)
		}
	}
}

// --- Bad JSON on inbound: ignored, connection stays up --------------

func TestWS_BadInboundJSON_IgnoredNotKilled(t *testing.T) {
	server, _, snap := wsTestServer(t)
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket)

	// Send garbage.
	if err := socket.WriteMessage(websocket.TextMessage, []byte("{not json")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	// Send a valid subscribe — connection should still work.
	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	ack := readOneJSON(t, socket)
	if ack["type"] != "ack" {
		t.Errorf("after garbage: expected ack, got %q", ack["type"])
	}
}

// --- Unknown frame type: ignored ------------------------------------

func TestWS_UnknownFrameType_IgnoredNotKilled(t *testing.T) {
	server, _, snap := wsTestServer(t)
	snap.SetLast("ACME", 10_050)

	socket := dialWS(t, server)
	_ = readOneJSON(t, socket)

	writeJSONFrame(t, socket, map[string]any{
		"type": "pls_do_something",
	})
	// Subsequent subscribe still works.
	writeJSONFrame(t, socket, map[string]any{
		"type": "subscribe",
		"subs": []map[string]any{{"stream": "snapshot", "instrument": "ACME"}},
	})
	ack := readOneJSON(t, socket)
	if ack["type"] != "ack" {
		t.Errorf("after unknown type: expected ack, got %q", ack["type"])
	}
}
