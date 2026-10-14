package conductor

import (
	"fmt"
	"net/http"
	"time"
)

// marketCatalogEntry is one row of the GET /v1/markets response. One
// entry per configured market tag; array membership is itself the
// "supported" signal (no redundant supported field).
type marketCatalogEntry struct {
	Tag     string            `json:"tag"`
	Active  bool              `json:"active"`
	Session marketSessionInfo `json:"session"`
}

// marketSessionInfo is the canonical session window. Instants are
// RFC3339 UTC so clients parse them with their native Instant/Date
// parser; wall_clock is a human string in the market's own timezone so
// the UI can label the window without re-converting.
type marketSessionInfo struct {
	OpensAt   string `json:"opens_at"`
	ClosesAt  string `json:"closes_at"`
	Duration  string `json:"duration"`
	WallClock string `json:"wall_clock"`
}

// marketCatalogResponse is the top-level envelope. markets is never
// nil (empty catalog returns [], not null).
type marketCatalogResponse struct {
	Markets []marketCatalogEntry `json:"markets"`
}

// handleMarkets serves the catalog + liveness + session-window read.
// For every configured market tag:
//
//   - loadConfig reads the on-disk JSON. A per-market load or window
//     error is logged and the tag is skipped; a bad config for one
//     market must never fail the whole endpoint.
//   - isCanonicalLive asks the registry whether a canonical Live
//     market with that tag currently exists (brief lock for the scan,
//     no call-out inside — THE RULE).
//   - nextSessionWindow derives the current-or-next session window
//     purely from config (opens-at + closes-at + duration + wall-clock).
//
// Empty catalog → 200 with an empty list. The only failure that fails
// the whole endpoint is listConfiguredMarkets itself erroring (the
// cfgDir listing surface).
func (conductor *Conductor) handleMarkets(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathMarkets)

	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathMarkets)
		return
	}

	tags, listErr := listConfiguredMarkets(conductor.cfgDir)
	if listErr != nil {
		conductor.log.Error("conductor: handleMarkets listConfiguredMarkets failed",
			"cfg_dir", conductor.cfgDir,
			"err", listErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "catalog unavailable", PathMarkets)
		return
	}

	entries := make([]marketCatalogEntry, 0, len(tags))
	for _, tag := range tags {
		marketConfig, loadErr := loadConfig(conductor.cfgDir, tag)
		if loadErr != nil {
			conductor.log.Warn("conductor: handleMarkets skipping bad config",
				"tag", tag,
				"err", loadErr,
			)
			continue
		}
		openInstant, closeInstant, windowErr := nextSessionWindow(marketConfig, conductor.now())
		if windowErr != nil {
			conductor.log.Warn("conductor: handleMarkets skipping unschedulable market",
				"tag", tag,
				"err", windowErr,
			)
			continue
		}
		entries = append(entries, marketCatalogEntry{
			Tag:    string(tag),
			Active: conductor.isCanonicalLive(tag),
			Session: marketSessionInfo{
				OpensAt:   openInstant.UTC().Format(time.RFC3339),
				ClosesAt:  closeInstant.UTC().Format(time.RFC3339),
				Duration:  closeInstant.Sub(openInstant).String(),
				WallClock: wallClockLabel(marketConfig, openInstant),
			},
		})
	}

	writeJSON(responseWriter, http.StatusOK, marketCatalogResponse{Markets: entries}, PathMarkets)
}

// isCanonicalLive briefly takes the registry mutex to answer "is there
// a Live canonical market record for this tag right now?". Lock-only-
// for-the-scan: findLiveCanonical is a pure in-memory walk, no I/O, no
// call-out. Reuses the same scan the join flow uses so liveness here
// matches admissibility there exactly.
func (conductor *Conductor) isCanonicalLive(tag MarketTag) bool {
	conductor.reg.mu.Lock()
	defer conductor.reg.mu.Unlock()
	return findLiveCanonical(conductor.reg, tag) != nil
}

// nextSessionWindow returns the canonical session window the UI should
// show for this market right now. The window is purely config-derived
// (no registry read), so liveness and window stay orthogonal — a Live
// market and a closed one both get a well-defined window.
//
// Semantics (current-or-next, weekdays-only):
//
//   - If today's session has not yet closed, return today's open and
//     close instants (even if we are in the pre-open gap, when the
//     market is not yet active).
//   - Otherwise walk forward one calendar day at a time, skipping
//     Saturday and Sunday, and return the first upcoming weekday's
//     open/close.
//
// The open boundary is anchored directly (NOT open − preopen lead):
// the preopen lead is a bring-up timing concern invisible to the user.
// Reuses the canonical-timer's HH:MM format and timezone lookup.
//
// Errors on malformed config:
//
//   - zero session boundaries,
//   - unknown IANA timezone,
//   - unparseable first or last boundary HH:MM.
//
// Pure: reads only its arguments; writes nothing.
func nextSessionWindow(marketConfig MarketConfig, nowInstant time.Time) (openInstant, closeInstant time.Time, err error) {
	if len(marketConfig.Session) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("conductor: nextSessionWindow: no session boundaries configured")
	}
	location, locationErr := time.LoadLocation(marketConfig.Timezone)
	if locationErr != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("conductor: nextSessionWindow: timezone %q: %w", marketConfig.Timezone, locationErr)
	}
	firstBoundary := marketConfig.Session[0]
	lastBoundary := marketConfig.Session[len(marketConfig.Session)-1]
	parsedOpen, openParseErr := time.ParseInLocation(sessionBoundaryTimeFormat, firstBoundary.At, location)
	if openParseErr != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("conductor: nextSessionWindow: session[0].at %q: %w", firstBoundary.At, openParseErr)
	}
	parsedClose, closeParseErr := time.ParseInLocation(sessionBoundaryTimeFormat, lastBoundary.At, location)
	if closeParseErr != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("conductor: nextSessionWindow: last session boundary at %q: %w", lastBoundary.At, closeParseErr)
	}

	nowInMarket := nowInstant.In(location)
	for dayOffset := 0; dayOffset <= weekdayLookAheadDays; dayOffset++ {
		candidateDate := nowInMarket.AddDate(0, 0, dayOffset)
		weekday := candidateDate.Weekday()
		if weekday == time.Saturday || weekday == time.Sunday {
			continue
		}
		candidateOpen := time.Date(
			candidateDate.Year(), candidateDate.Month(), candidateDate.Day(),
			parsedOpen.Hour(), parsedOpen.Minute(), 0, 0,
			location,
		)
		candidateClose := time.Date(
			candidateDate.Year(), candidateDate.Month(), candidateDate.Day(),
			parsedClose.Hour(), parsedClose.Minute(), 0, 0,
			location,
		)
		// If this is today and today's session has already closed, roll
		// forward. Any day in the future is accepted on first weekday
		// hit, including the pre-open gap (opens_at > now, closes_at >
		// now — the UI renders "opens in …").
		if dayOffset == 0 && !candidateClose.After(nowInMarket) {
			continue
		}
		return candidateOpen, candidateClose, nil
	}
	return time.Time{}, time.Time{}, fmt.Errorf("conductor: nextSessionWindow: no session window within %d days", weekdayLookAheadDays)
}

// wallClockLabel builds the human session-window string for the UI,
// e.g. "09:15–15:30 IST". Takes the raw HH:MM strings straight out of
// the config (so the on-disk source of truth is what the user sees),
// and pulls the zone abbreviation from the first instant in the market
// timezone — zone() returns "IST", "EDT", "EST", … matching what the
// instant resolved to on that date (handles DST without a hardcoded
// map).
//
// Config shape is already validated by nextSessionWindow before this
// is called, so an empty Session slice or an unparseable timezone are
// not possible at this call site.
func wallClockLabel(marketConfig MarketConfig, openInstantInMarketTZ time.Time) string {
	firstBoundary := marketConfig.Session[0]
	lastBoundary := marketConfig.Session[len(marketConfig.Session)-1]
	zoneAbbrev, _ := openInstantInMarketTZ.Zone()
	return fmt.Sprintf("%s–%s %s", firstBoundary.At, lastBoundary.At, zoneAbbrev)
}
