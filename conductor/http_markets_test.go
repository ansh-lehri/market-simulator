package conductor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- helpers specific to the markets catalog -------------------------

// writeMarketConfig drops a minimal valid <tag>.json into the given
// cfgDir. Hand-rolls the JSON so the test fixture does not couple to
// the marketConfigRaw shape — if someone renames a json tag the
// handler responses change observably, which is exactly what we want
// surfaced.
func writeMarketConfig(t *testing.T, cfgDir string, tag string, timezone string, openHHMM string, closeHHMM string) {
	t.Helper()
	content := fmt.Sprintf(`{
  "market": %q,
  "timezone": %q,
  "ohlcv_interval": "1m",
  "num_shards": 1,
  "preopen_lead": "25m",
  "session": [
    { "at": %q, "phase": "continuous" },
    { "at": %q, "phase": "closed" }
  ],
  "ondemand": { "duration": "30m", "warmup": "5m" },
  "instruments": [
    { "id": "ACME", "tick_size": 5, "band_bps": 1000, "seed_price": 1000,
      "liquidity": { "levels": 3, "spacing_pct": 0.5, "near_qty": 100, "taper": 0.9 } },
    { "id": "BETA", "tick_size": 10, "band_bps": 1000, "seed_price": 2000,
      "liquidity": { "levels": 3, "spacing_pct": 0.5, "near_qty": 100, "taper": 0.9 } }
  ]
}`, tag, timezone, openHHMM, closeHHMM)
	path := filepath.Join(cfgDir, tag+".json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// writeBrokenConfig writes an unparseable config file so the catalog
// handler's "skip-and-log" path can be exercised. Any single file
// being malformed must not fail the whole endpoint.
func writeBrokenConfig(t *testing.T, cfgDir string, tag string) {
	t.Helper()
	path := filepath.Join(cfgDir, tag+".json")
	if err := os.WriteFile(path, []byte("{ this is not json"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// newMarketsHarness builds a Conductor with an empty temp cfgDir and
// stub broker/market-data ports — the handler reads only cfgDir and
// the registry, never the broker or market-data. Caller writes
// configs into the returned path with writeMarketConfig.
func newMarketsHarness(t *testing.T) (*Conductor, string) {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	cfgDir := t.TempDir()
	cd, err := NewConductor(Config{
		CfgDir:    cfgDir,
		OHLCVPool: pool,
		Broker:    stubBrokerPort{},
		Market:    stubMarketDataPort{},
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	return cd, cfgDir
}

// getMarkets calls handleMarkets directly (bypasses the mux so each
// test runs standalone) and returns the recorder for assertion.
func getMarkets(t *testing.T, conductor *Conductor, method string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, PathMarkets, nil)
	recorder := httptest.NewRecorder()
	conductor.handleMarkets(recorder, request)
	return recorder
}

// --- nextSessionWindow (pure function; table-driven) -----------------

func TestNextSessionWindow(t *testing.T) {
	// Shared IST fixture: 09:15–15:30, Asia/Kolkata.
	kolkata, locationErr := time.LoadLocation("Asia/Kolkata")
	if locationErr != nil {
		t.Fatalf("LoadLocation: %v", locationErr)
	}
	istConfig := MarketConfig{
		Timezone: "Asia/Kolkata",
		Session: []SessionBoundaryConfig{
			{At: "09:15", Phase: "continuous"},
			{At: "15:30", Phase: "closed"},
		},
	}

	type windowCase struct {
		name           string
		cfg            MarketConfig
		now            time.Time
		wantOpenLocal  time.Time // in market tz
		wantCloseLocal time.Time
		wantErr        bool
	}
	cases := []windowCase{
		{
			name:           "pre-open same weekday -> today's window",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata),
			wantOpenLocal:  time.Date(2026, 10, 13, 9, 15, 0, 0, kolkata),
			wantCloseLocal: time.Date(2026, 10, 13, 15, 30, 0, 0, kolkata),
		},
		{
			name:           "during session -> today's window (Live case)",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 13, 12, 0, 0, 0, kolkata),
			wantOpenLocal:  time.Date(2026, 10, 13, 9, 15, 0, 0, kolkata),
			wantCloseLocal: time.Date(2026, 10, 13, 15, 30, 0, 0, kolkata),
		},
		{
			name:           "exactly at close -> rolls to next weekday",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 13, 15, 30, 0, 0, kolkata),
			wantOpenLocal:  time.Date(2026, 10, 14, 9, 15, 0, 0, kolkata),
			wantCloseLocal: time.Date(2026, 10, 14, 15, 30, 0, 0, kolkata),
		},
		{
			name:           "post-close same weekday -> next weekday",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 13, 20, 0, 0, 0, kolkata),
			wantOpenLocal:  time.Date(2026, 10, 14, 9, 15, 0, 0, kolkata),
			wantCloseLocal: time.Date(2026, 10, 14, 15, 30, 0, 0, kolkata),
		},
		{
			name:           "Friday after close -> Monday",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 16, 20, 0, 0, 0, kolkata), // Fri
			wantOpenLocal:  time.Date(2026, 10, 19, 9, 15, 0, 0, kolkata), // Mon
			wantCloseLocal: time.Date(2026, 10, 19, 15, 30, 0, 0, kolkata),
		},
		{
			name:           "Saturday -> Monday",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 17, 10, 0, 0, 0, kolkata),
			wantOpenLocal:  time.Date(2026, 10, 19, 9, 15, 0, 0, kolkata),
			wantCloseLocal: time.Date(2026, 10, 19, 15, 30, 0, 0, kolkata),
		},
		{
			name:           "Sunday -> Monday",
			cfg:            istConfig,
			now:            time.Date(2026, 10, 18, 23, 59, 0, 0, kolkata),
			wantOpenLocal:  time.Date(2026, 10, 19, 9, 15, 0, 0, kolkata),
			wantCloseLocal: time.Date(2026, 10, 19, 15, 30, 0, 0, kolkata),
		},
		{
			name: "unknown timezone -> error",
			cfg: MarketConfig{
				Timezone: "Not/A_Zone",
				Session:  istConfig.Session,
			},
			now:     time.Date(2026, 10, 13, 7, 0, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name: "empty session -> error",
			cfg: MarketConfig{
				Timezone: "Asia/Kolkata",
				Session:  nil,
			},
			now:     time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata),
			wantErr: true,
		},
		{
			name: "malformed first boundary -> error",
			cfg: MarketConfig{
				Timezone: "Asia/Kolkata",
				Session: []SessionBoundaryConfig{
					{At: "nope", Phase: "continuous"},
					{At: "15:30", Phase: "closed"},
				},
			},
			now:     time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata),
			wantErr: true,
		},
		{
			name: "malformed last boundary -> error",
			cfg: MarketConfig{
				Timezone: "Asia/Kolkata",
				Session: []SessionBoundaryConfig{
					{At: "09:15", Phase: "continuous"},
					{At: "25:99", Phase: "closed"},
				},
			},
			now:     time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata),
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			openInstant, closeInstant, err := nextSessionWindow(tc.cfg, tc.now)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got open=%v close=%v", openInstant, closeInstant)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !openInstant.Equal(tc.wantOpenLocal) {
				t.Errorf("opens_at: got %v, want %v", openInstant, tc.wantOpenLocal)
			}
			if !closeInstant.Equal(tc.wantCloseLocal) {
				t.Errorf("closes_at: got %v, want %v", closeInstant, tc.wantCloseLocal)
			}
		})
	}
}

// --- wallClockLabel (pure formatter) ---------------------------------

func TestWallClockLabel(t *testing.T) {
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	cfg := MarketConfig{
		Timezone: "Asia/Kolkata",
		Session: []SessionBoundaryConfig{
			{At: "09:15", Phase: "continuous"},
			{At: "15:30", Phase: "closed"},
		},
	}
	openInstant := time.Date(2026, 10, 13, 9, 15, 0, 0, kolkata)
	label := wallClockLabel(cfg, openInstant)
	if label != "09:15–15:30 IST" {
		t.Errorf("wallClockLabel = %q, want %q", label, "09:15–15:30 IST")
	}
}

// --- /v1/markets happy path (multi-tag, mixed active) ----------------

func TestHandleMarkets_HappyPathMultiTag(t *testing.T) {
	conductor, cfgDir := newMarketsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	writeMarketConfig(t, cfgDir, "US", "America/New_York", "09:30", "16:00")

	// Pin now so the window computation is deterministic.
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	conductor.now = func() time.Time { return time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata) } // Tuesday pre-open

	// Install a Live canonical market record for IN only; US is not up.
	installLiveCanonicalMarket(t, conductor, "IN")

	recorder := getMarkets(t, conductor, http.MethodGet)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var response marketCatalogResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v (raw=%q)", err, recorder.Body.String())
	}
	if len(response.Markets) != 2 {
		t.Fatalf("got %d markets, want 2. body=%q", len(response.Markets), recorder.Body.String())
	}

	byTag := map[string]marketCatalogEntry{}
	for _, entry := range response.Markets {
		byTag[entry.Tag] = entry
	}

	// IN: active + today's IST window + wall clock + 6h15m duration.
	in, ok := byTag["IN"]
	if !ok {
		t.Fatal("IN tag missing")
	}
	if !in.Active {
		t.Error("IN active = false, want true (Live market installed)")
	}
	if in.Session.WallClock != "09:15–15:30 IST" {
		t.Errorf("IN wall_clock = %q, want %q", in.Session.WallClock, "09:15–15:30 IST")
	}
	if in.Session.Duration != "6h15m0s" {
		t.Errorf("IN duration = %q, want %q", in.Session.Duration, "6h15m0s")
	}
	// 09:15 IST on 2026-10-13 is 03:45 UTC.
	if in.Session.OpensAt != "2026-10-13T03:45:00Z" {
		t.Errorf("IN opens_at = %q, want %q", in.Session.OpensAt, "2026-10-13T03:45:00Z")
	}
	if in.Session.ClosesAt != "2026-10-13T10:00:00Z" {
		t.Errorf("IN closes_at = %q, want %q", in.Session.ClosesAt, "2026-10-13T10:00:00Z")
	}

	// US: not active, window still computed (closed market, UI renders "opens in …").
	us, ok := byTag["US"]
	if !ok {
		t.Fatal("US tag missing")
	}
	if us.Active {
		t.Error("US active = true, want false (no Live market installed)")
	}
	if !strings.Contains(us.Session.WallClock, "09:30–16:00 ") {
		t.Errorf("US wall_clock = %q, want prefix %q", us.Session.WallClock, "09:30–16:00 ")
	}
	if us.Session.Duration != "6h30m0s" {
		t.Errorf("US duration = %q, want %q", us.Session.Duration, "6h30m0s")
	}
}

// --- on-demand instances are excluded (findLiveCanonical filters mode) ---

func TestHandleMarkets_OnDemandMarketsExcluded(t *testing.T) {
	conductor, cfgDir := newMarketsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	conductor.now = func() time.Time { return time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata) }

	// Install only an OnDemand private instance under the IN tag.
	installMarket(t, conductor, OnDemand, Live, "IN", "od-session-xyz")

	recorder := getMarkets(t, conductor, http.MethodGet)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response marketCatalogResponse
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Markets) != 1 {
		t.Fatalf("got %d markets, want 1 (IN config)", len(response.Markets))
	}
	if response.Markets[0].Active {
		t.Error("IN active = true, want false (only on-demand instance exists; canonical filter must exclude it)")
	}
}

// --- Live market's closes_at equals the retained Market.closeAt ------

func TestHandleMarkets_ClosesAtMatchesLiveMarketCloseAt(t *testing.T) {
	conductor, cfgDir := newMarketsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	conductor.now = func() time.Time { return time.Date(2026, 10, 13, 12, 0, 0, 0, kolkata) } // mid-session

	// Install Live and stamp closeAt the way spin-up would — at the
	// terminal boundary on today's date in the market timezone.
	marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")
	expectedCloseAt := time.Date(2026, 10, 13, 15, 30, 0, 0, kolkata)
	conductor.reg.mu.Lock()
	conductor.reg.markets[marketID].closeAt = expectedCloseAt
	conductor.reg.mu.Unlock()

	recorder := getMarkets(t, conductor, http.MethodGet)
	var response marketCatalogResponse
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Markets) != 1 {
		t.Fatalf("got %d markets, want 1", len(response.Markets))
	}

	gotCloseAt, parseErr := time.Parse(time.RFC3339, response.Markets[0].Session.ClosesAt)
	if parseErr != nil {
		t.Fatalf("parse closes_at %q: %v", response.Markets[0].Session.ClosesAt, parseErr)
	}
	if !gotCloseAt.Equal(expectedCloseAt) {
		t.Errorf("closes_at %v != retained Market.closeAt %v", gotCloseAt, expectedCloseAt)
	}
}

// --- one bad config skipped, others returned 200 ---------------------

func TestHandleMarkets_SkipsBadConfigsReturns200(t *testing.T) {
	conductor, cfgDir := newMarketsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	writeBrokenConfig(t, cfgDir, "US")
	writeMarketConfig(t, cfgDir, "SG", "Asia/Singapore", "09:00", "17:00")
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	conductor.now = func() time.Time { return time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata) }

	recorder := getMarkets(t, conductor, http.MethodGet)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (bad config must not fail the endpoint). body=%q",
			recorder.Code, recorder.Body.String())
	}

	var response marketCatalogResponse
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Markets) != 2 {
		t.Fatalf("got %d markets, want 2 (US skipped). body=%q",
			len(response.Markets), recorder.Body.String())
	}
	for _, entry := range response.Markets {
		if entry.Tag == "US" {
			t.Errorf("US should have been skipped, got entry %+v", entry)
		}
	}
}

// --- empty catalog is a valid 200, not 404 ---------------------------

func TestHandleMarkets_EmptyCatalogReturns200(t *testing.T) {
	conductor, _ := newMarketsHarness(t)
	// No configs written.

	recorder := getMarkets(t, conductor, http.MethodGet)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (empty catalog is valid)", recorder.Code)
	}
	var response marketCatalogResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.Markets == nil {
		t.Error("markets = nil, want empty slice (JSON []) for stable client parsing")
	}
	if len(response.Markets) != 0 {
		t.Errorf("got %d markets, want 0", len(response.Markets))
	}
}

// --- method / listing-failure error paths ----------------------------

func TestHandleMarkets_MethodNotGET(t *testing.T) {
	conductor, _ := newMarketsHarness(t)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			recorder := getMarkets(t, conductor, method)
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s status = %d, want 405", method, recorder.Code)
			}
			assertErrorBody(t, recorder)
		})
	}
}

func TestHandleMarkets_CfgDirListingFailureReturns500(t *testing.T) {
	conductor, cfgDir := newMarketsHarness(t)
	// Point cfgDir at a path that does not exist — listConfiguredMarkets
	// fails at the ReadDir call, which is a whole-endpoint failure
	// (distinct from a per-market bad config, which skips).
	conductor.cfgDir = filepath.Join(cfgDir, "does-not-exist")

	recorder := getMarkets(t, conductor, http.MethodGet)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	assertErrorBody(t, recorder)
}

// --- concurrent reads under -race -----------------------------------

// TestHandleMarkets_ConcurrentScansRace releases a swarm of GET callers
// against the registry while a background writer flips a Market
// record's state through the registry's one mutex. The isCanonicalLive
// scan must take reg.mu correctly and never race.
func TestHandleMarkets_ConcurrentScansRace(t *testing.T) {
	conductor, cfgDir := newMarketsHarness(t)
	writeMarketConfig(t, cfgDir, "IN", "Asia/Kolkata", "09:15", "15:30")
	kolkata, _ := time.LoadLocation("Asia/Kolkata")
	conductor.now = func() time.Time { return time.Date(2026, 10, 13, 7, 0, 0, 0, kolkata) }

	marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")

	const readers = 32
	const flips = 128
	startBarrier := make(chan struct{})
	var waitGroup sync.WaitGroup

	// Writer: flip State between Live and Provisioning through the
	// registry mutex. This stays within the data types the reader
	// scans, so a race would surface deterministically under -race.
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		<-startBarrier
		for i := 0; i < flips; i++ {
			conductor.reg.mu.Lock()
			if conductor.reg.markets[marketID].State == Live {
				conductor.reg.markets[marketID].State = Provisioning
			} else {
				conductor.reg.markets[marketID].State = Live
			}
			conductor.reg.mu.Unlock()
		}
	}()

	// Readers: fire the handler.
	for i := 0; i < readers; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-startBarrier
			recorder := getMarkets(t, conductor, http.MethodGet)
			if recorder.Code != http.StatusOK {
				t.Errorf("concurrent reader got status %d", recorder.Code)
			}
		}()
	}

	close(startBarrier)
	waitGroup.Wait()
}
