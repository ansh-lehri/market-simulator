package marketdata

import (
	"log/slog"
	"time"
)

// marketTimezones maps the broker's market label to its IANA timezone.
// Same allowlist shape as bringup/country.go — adding a market = adding a
// line here and nothing else at this layer. Two readers consume it: the
// rollup engine (for bucket alignment + response rendering) and the HTTP
// handler (for request parse local→UTC + response "timezone" field).
//
// Values pre-loaded at package init so tzFor is a plain map read with no
// LoadLocation overhead on the hot path. An entry whose LoadLocation
// errors is omitted — tzFor misses, and the caller treats it as an
// unknown market (400 at the HTTP edge). zoneinfo missing from the
// binary/OS is a deployment issue, not a per-request failure.
var marketTimezones = loadMarketTimezones()

// loadMarketTimezones resolves each allowlisted market to its *time.Location
// via time.LoadLocation. Called once at package init. Failures are logged
// at error-level (deployment problem) but do not panic — the component
// stays available for the markets that did resolve.
func loadMarketTimezones() map[string]*time.Location {
	specs := map[string]string{
		"IN": "Asia/Kolkata",
		"US": "America/New_York",
	}
	out := make(map[string]*time.Location, len(specs))
	for market, ianaName := range specs {
		location, loadErr := time.LoadLocation(ianaName)
		if loadErr != nil {
			slog.Error("broker/marketdata: tz load failed at init",
				"market", market,
				"iana", ianaName,
				"err", loadErr,
			)
			continue
		}
		out[market] = location
	}
	return out
}

// tzFor returns the market's IANA *time.Location or (nil, false) if the
// market is not in the allowlist. Pure read, safe from any goroutine.
// Readers: the rollup engine (bucket-align + serialize), the HTTP handler
// (parse inbound local→UTC + emit "timezone" on the response envelope).
func tzFor(market string) (*time.Location, bool) {
	location, ok := marketTimezones[market]
	return location, ok
}
