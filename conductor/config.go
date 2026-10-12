package conductor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"simulator/contract"
)

// MarketConfig is one on-disk configuration for one market tag — the
// shape the canonical bring-up phase will read to build each market's
// schedule, instruments, and shard count. Durations on-disk are strings
// ("25m", "1m") — human editable — and parsed into time.Duration at
// load time via the shadow struct marketConfigRaw.
type MarketConfig struct {
	Market        MarketTag
	Timezone      string
	OHLCVInterval time.Duration
	NumShards     int
	PreopenLead   time.Duration
	Session       []SessionBoundaryConfig
	OnDemand      OnDemandConfig
	Instruments   []InstrumentConfig
}

type SessionBoundaryConfig struct {
	At    string `json:"at"`
	Phase string `json:"phase"`
}

// OnDemandConfig carries both the user-chosen session-duration bounds
// and the warm-up length — the window after construct during which
// agents trade before the user is admitted.
type OnDemandConfig struct {
	Duration time.Duration
	Warmup   time.Duration
}

type InstrumentConfig struct {
	ID        contract.InstrumentID `json:"id"`
	TickSize  contract.Ticks        `json:"tick_size"`
	BandBps   int                   `json:"band_bps"`
	SeedPrice contract.Ticks        `json:"seed_price"`
	Liquidity LiquidityConfig       `json:"liquidity"`
}

type LiquidityConfig struct {
	Levels     int     `json:"levels"`
	SpacingPct float64 `json:"spacing_pct"`
	NearQty    int64   `json:"near_qty"`
	Taper      float64 `json:"taper"`
}

// marketConfigRaw is the on-disk shadow with string durations. Kept
// unexported so the public API stays time.Duration.
type marketConfigRaw struct {
	Market        MarketTag               `json:"market"`
	Timezone      string                  `json:"timezone"`
	OHLCVInterval string                  `json:"ohlcv_interval"`
	NumShards     int                     `json:"num_shards"`
	PreopenLead   string                  `json:"preopen_lead"`
	Session       []SessionBoundaryConfig `json:"session"`
	OnDemand      onDemandConfigRaw       `json:"ondemand"`
	Instruments   []InstrumentConfig      `json:"instruments"`
}

type onDemandConfigRaw struct {
	Duration string `json:"duration"`
	Warmup   string `json:"warmup"`
}

func (raw marketConfigRaw) toMarketConfig() (MarketConfig, error) {
	ohlcvInterval, err := time.ParseDuration(raw.OHLCVInterval)
	if err != nil {
		return MarketConfig{}, fmt.Errorf("ohlcv_interval %q: %w", raw.OHLCVInterval, err)
	}
	preopenLead, err := time.ParseDuration(raw.PreopenLead)
	if err != nil {
		return MarketConfig{}, fmt.Errorf("preopen_lead %q: %w", raw.PreopenLead, err)
	}
	ondemandDuration, err := time.ParseDuration(raw.OnDemand.Duration)
	if err != nil {
		return MarketConfig{}, fmt.Errorf("ondemand.duration %q: %w", raw.OnDemand.Duration, err)
	}
	ondemandWarmup, err := time.ParseDuration(raw.OnDemand.Warmup)
	if err != nil {
		return MarketConfig{}, fmt.Errorf("ondemand.warmup %q: %w", raw.OnDemand.Warmup, err)
	}
	return MarketConfig{
		Market:        raw.Market,
		Timezone:      raw.Timezone,
		OHLCVInterval: ohlcvInterval,
		NumShards:     raw.NumShards,
		PreopenLead:   preopenLead,
		Session:       raw.Session,
		OnDemand: OnDemandConfig{
			Duration: ondemandDuration,
			Warmup:   ondemandWarmup,
		},
		Instruments: raw.Instruments,
	}, nil
}

// loadConfig reads <cfgDir>/<tag>.json and returns the parsed
// MarketConfig. Missing file, malformed JSON, and bad duration strings
// each return a wrapped error.
func loadConfig(cfgDir string, tag MarketTag) (MarketConfig, error) {
	path := filepath.Join(cfgDir, string(tag)+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return MarketConfig{}, fmt.Errorf("conductor: loadConfig %q: %w", tag, err)
	}
	var raw marketConfigRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return MarketConfig{}, fmt.Errorf("conductor: loadConfig %q: parse: %w", tag, err)
	}
	cfg, err := raw.toMarketConfig()
	if err != nil {
		return MarketConfig{}, fmt.Errorf("conductor: loadConfig %q: %w", tag, err)
	}
	return cfg, nil
}

// listConfiguredMarkets returns every market tag found under cfgDir
// (one *.json file per market tag). Subdirectories and non-json files
// are ignored. Sorted ascending for deterministic iteration.
func listConfiguredMarkets(cfgDir string) ([]MarketTag, error) {
	entries, err := os.ReadDir(cfgDir)
	if err != nil {
		return nil, fmt.Errorf("conductor: listConfiguredMarkets: %w", err)
	}
	tags := make([]MarketTag, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		tags = append(tags, MarketTag(strings.TrimSuffix(name, ".json")))
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags, nil
}
