package conductor

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, dir, tag, body string) {
	t.Helper()
	path := filepath.Join(dir, tag+".json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

const validINConfig = `{
  "market": "IN",
  "timezone": "Asia/Kolkata",
  "ohlcv_interval": "1m",
  "num_shards": 1,
  "preopen_lead": "25m",
  "session": [
    {"at": "09:15", "phase": "continuous"},
    {"at": "15:30", "phase": "closed"}
  ],
  "ondemand": {"duration": "60m", "warmup": "2m"},
  "instruments": [
    {"id": "ACME", "tick_size": 5, "band_bps": 1000, "seed_price": 10000,
     "liquidity": {"levels": 5, "spacing_pct": 0.1, "near_qty": 100, "taper": 0.5}}
  ]
}`

func TestLoadConfig_Valid(t *testing.T) {
	dir := t.TempDir()
	writeTempConfig(t, dir, "IN", validINConfig)

	cfg, err := loadConfig(dir, "IN")
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Market != "IN" {
		t.Errorf("Market = %q, want IN", cfg.Market)
	}
	if cfg.Timezone != "Asia/Kolkata" {
		t.Errorf("Timezone = %q", cfg.Timezone)
	}
	if cfg.OHLCVInterval != time.Minute {
		t.Errorf("OHLCVInterval = %v, want 1m", cfg.OHLCVInterval)
	}
	if cfg.NumShards != 1 {
		t.Errorf("NumShards = %d, want 1", cfg.NumShards)
	}
	if cfg.PreopenLead != 25*time.Minute {
		t.Errorf("PreopenLead = %v, want 25m", cfg.PreopenLead)
	}
	if cfg.OnDemand.Duration != 60*time.Minute {
		t.Errorf("OnDemand.Duration = %v, want 60m", cfg.OnDemand.Duration)
	}
	if cfg.OnDemand.Warmup != 2*time.Minute {
		t.Errorf("OnDemand.Warmup = %v, want 2m", cfg.OnDemand.Warmup)
	}
	if len(cfg.Session) != 2 {
		t.Fatalf("Session len = %d, want 2", len(cfg.Session))
	}
	if cfg.Session[0].At != "09:15" || cfg.Session[0].Phase != "continuous" {
		t.Errorf("Session[0] = %+v", cfg.Session[0])
	}
	if cfg.Session[1].At != "15:30" || cfg.Session[1].Phase != "closed" {
		t.Errorf("Session[1] = %+v", cfg.Session[1])
	}
	if len(cfg.Instruments) != 1 {
		t.Fatalf("Instruments len = %d, want 1", len(cfg.Instruments))
	}
	instrument := cfg.Instruments[0]
	if instrument.ID != "ACME" {
		t.Errorf("Instrument.ID = %q", instrument.ID)
	}
	if instrument.TickSize != 5 {
		t.Errorf("Instrument.TickSize = %d", instrument.TickSize)
	}
	if instrument.SeedPrice != 10000 {
		t.Errorf("Instrument.SeedPrice = %d", instrument.SeedPrice)
	}
	if instrument.Liquidity.Levels != 5 || instrument.Liquidity.NearQty != 100 {
		t.Errorf("Instrument.Liquidity = %+v", instrument.Liquidity)
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := loadConfig(dir, "IN")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadConfig_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	writeTempConfig(t, dir, "IN", "{ not valid json")
	_, err := loadConfig(dir, "IN")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLoadConfig_BadDurations(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "bad ohlcv_interval",
			body: `{"market":"IN","timezone":"Asia/Kolkata","ohlcv_interval":"nope","num_shards":1,"preopen_lead":"25m","session":[],"ondemand":{"duration":"60m","warmup":"2m"},"instruments":[]}`,
		},
		{
			name: "bad preopen_lead",
			body: `{"market":"IN","timezone":"Asia/Kolkata","ohlcv_interval":"1m","num_shards":1,"preopen_lead":"ten minutes","session":[],"ondemand":{"duration":"60m","warmup":"2m"},"instruments":[]}`,
		},
		{
			name: "bad ondemand.duration",
			body: `{"market":"IN","timezone":"Asia/Kolkata","ohlcv_interval":"1m","num_shards":1,"preopen_lead":"25m","session":[],"ondemand":{"duration":"long","warmup":"2m"},"instruments":[]}`,
		},
		{
			name: "bad ondemand.warmup",
			body: `{"market":"IN","timezone":"Asia/Kolkata","ohlcv_interval":"1m","num_shards":1,"preopen_lead":"25m","session":[],"ondemand":{"duration":"60m","warmup":"soon"},"instruments":[]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeTempConfig(t, dir, "IN", tc.body)
			_, err := loadConfig(dir, "IN")
			if err == nil {
				t.Fatal("expected duration parse error")
			}
		})
	}
}

func TestListConfiguredMarkets_Empty(t *testing.T) {
	dir := t.TempDir()
	tags, err := listConfiguredMarkets(dir)
	if err != nil {
		t.Fatalf("listConfiguredMarkets: %v", err)
	}
	if len(tags) != 0 {
		t.Errorf("tags = %v, want empty slice", tags)
	}
}

func TestListConfiguredMarkets_MixedEntries(t *testing.T) {
	dir := t.TempDir()
	writeTempConfig(t, dir, "US", validINConfig)
	writeTempConfig(t, dir, "IN", validINConfig)
	if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("ignore me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	tags, err := listConfiguredMarkets(dir)
	if err != nil {
		t.Fatalf("listConfiguredMarkets: %v", err)
	}
	if len(tags) != 2 {
		t.Fatalf("tags = %v, want 2 (IN, US)", tags)
	}
	if tags[0] != "IN" || tags[1] != "US" {
		t.Errorf("tags = %v, want sorted [IN US]", tags)
	}
}

func TestListConfiguredMarkets_MissingDir(t *testing.T) {
	_, err := listConfiguredMarkets(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("expected error for missing dir")
	}
}
