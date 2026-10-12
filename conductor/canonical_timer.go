package conductor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultBadConfigRetryDelay is how long the per-market timer sleeps
// between retries when its config fails to load or compute a next fire
// time. Picked large so a persistent config error never spins the loop
// — correctness over reactivity. Overridable via Config.BadConfigRetryDelay.
const DefaultBadConfigRetryDelay = time.Hour

// weekdayLookAheadDays bounds how far nextCanonicalFireTime scans
// forward for the next weekday. Seven covers "today is Friday after
// close, next weekday is Monday" (3-day gap) with slack; a higher
// bound would only accommodate a holiday calendar we do not have.
const weekdayLookAheadDays = 7

// nextCanonicalFireTime returns the next absolute instant the per-market
// timer must fire — the first boundary's wall-clock (session open)
// minus the configured preopen lead, in the market timezone, on the
// next weekday strictly after now. Weekends roll to Monday; a process
// that starts after today's fire has already passed rolls to the next
// weekday (the market stays down today).
//
// Errors on malformed config:
//   - no session boundaries,
//   - unknown timezone,
//   - unparseable first-boundary HH:MM.
//
// Pure function: reads only its arguments; writes nothing.
func nextCanonicalFireTime(marketConfig MarketConfig, nowFn func() time.Time) (time.Time, error) {
	if len(marketConfig.Session) == 0 {
		return time.Time{}, fmt.Errorf("conductor: nextCanonicalFireTime: no session boundaries configured")
	}
	location, locationErr := time.LoadLocation(marketConfig.Timezone)
	if locationErr != nil {
		return time.Time{}, fmt.Errorf("conductor: nextCanonicalFireTime: timezone %q: %w", marketConfig.Timezone, locationErr)
	}
	firstBoundary := marketConfig.Session[0]
	parsedWallClock, parseErr := time.ParseInLocation(sessionBoundaryTimeFormat, firstBoundary.At, location)
	if parseErr != nil {
		return time.Time{}, fmt.Errorf("conductor: nextCanonicalFireTime: session[0].at %q: %w", firstBoundary.At, parseErr)
	}

	nowInMarket := nowFn().In(location)
	for dayOffset := 0; dayOffset <= weekdayLookAheadDays; dayOffset++ {
		candidateDate := nowInMarket.AddDate(0, 0, dayOffset)
		weekday := candidateDate.Weekday()
		if weekday == time.Saturday || weekday == time.Sunday {
			continue
		}
		openInstant := time.Date(
			candidateDate.Year(), candidateDate.Month(), candidateDate.Day(),
			parsedWallClock.Hour(), parsedWallClock.Minute(), 0, 0,
			location,
		)
		fireInstant := openInstant.Add(-marketConfig.PreopenLead)
		if fireInstant.After(nowInMarket) {
			return fireInstant, nil
		}
	}
	// A degenerate clock or truly bizarre config could theoretically
	// miss every weekday in the window; defensive.
	return time.Time{}, fmt.Errorf("conductor: nextCanonicalFireTime: no fire time within %d days", weekdayLookAheadDays)
}

// runCanonicalTimer is the per-market timer loop. One goroutine per
// configured market; spawned by Conductor.Start. Flow:
//
//  1. Load the market config.
//  2. Compute the next fire instant (nextCanonicalFireTime).
//  3. Sleep until that instant OR exit if timerQuit closes.
//  4. On wake, call spinUpCanonical inline. Serializes same-market
//     bring-up by construction — this goroutine is the only trigger
//     for its tag. Spin-up errors other than ErrAlreadyUp are logged;
//     the market stays down until the next arm.
//  5. Loop. Re-arming recomputes fireAt from the current clock, so a
//     market naturally rolls to the next weekday.
//
// Config load errors and compute-fire-time errors each sleep for the
// configured bad-config retry delay before retrying — never spin.
//
// Exits only when timerQuit closes (shutdown).
func (conductor *Conductor) runCanonicalTimer(marketTag MarketTag) {
	defer conductor.wg.Done()
	conductor.log.Info("conductor: canonical timer started", "tag", marketTag)
	for {
		marketConfig, loadErr := loadConfig(conductor.cfgDir, marketTag)
		if loadErr != nil {
			conductor.log.Error("conductor: canonical timer load config failed; sleeping before retry",
				"tag", marketTag,
				"retry_delay", conductor.badConfigRetryDelay,
				"err", loadErr,
			)
			if !conductor.sleepOrQuit(conductor.badConfigRetryDelay) {
				conductor.log.Info("conductor: canonical timer exiting", "tag", marketTag)
				return
			}
			continue
		}
		fireAt, fireErr := nextCanonicalFireTime(marketConfig, conductor.now)
		if fireErr != nil {
			conductor.log.Error("conductor: canonical timer compute next fire time failed; sleeping before retry",
				"tag", marketTag,
				"retry_delay", conductor.badConfigRetryDelay,
				"err", fireErr,
			)
			if !conductor.sleepOrQuit(conductor.badConfigRetryDelay) {
				conductor.log.Info("conductor: canonical timer exiting", "tag", marketTag)
				return
			}
			continue
		}
		delay := fireAt.Sub(conductor.now())
		conductor.log.Info("conductor: canonical timer armed",
			"tag", marketTag,
			"fire_at", fireAt,
			"delay", delay,
		)
		if !conductor.sleepOrQuit(delay) {
			conductor.log.Info("conductor: canonical timer exiting", "tag", marketTag)
			return
		}
		marketID, spinUpErr := conductor.spinUpCanonical(context.Background(), marketTag)
		switch {
		case spinUpErr == nil:
			conductor.log.Info("conductor: canonical timer spin-up succeeded; re-arming for next weekday",
				"tag", marketTag,
				"market_id", marketID,
			)
		case errors.Is(spinUpErr, ErrAlreadyUp):
			conductor.log.Info("conductor: canonical timer spin-up found existing record; re-arming",
				"tag", marketTag,
				"market_id", marketID,
			)
		default:
			conductor.log.Error("conductor: canonical timer spin-up failed; market stays down until next arm",
				"tag", marketTag,
				"err", spinUpErr,
			)
		}
	}
}

// sleepOrQuit blocks for delay OR returns false if timerQuit closes
// before the delay elapses. A non-positive delay returns true after a
// non-blocking check for shutdown — so a stale fireAt (already in the
// past) still fires promptly but a Stop in flight still wins.
func (conductor *Conductor) sleepOrQuit(delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-conductor.timerQuit:
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-conductor.timerQuit:
		return false
	}
}
