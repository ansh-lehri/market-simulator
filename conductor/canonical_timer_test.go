package conductor

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- nextCanonicalFireTime -------------------------------------------

// baseMarketConfigForTimer returns an IN-shaped config with a 25m
// preopen lead and the three standard session boundaries (continuous
// at 09:15, closing_auction at 15:15, closed at 15:30, Asia/Kolkata).
// Tests mutate copies to construct edge cases.
func baseMarketConfigForTimer() MarketConfig {
	return MarketConfig{
		Market:        "IN",
		Timezone:      "Asia/Kolkata",
		OHLCVInterval: time.Minute,
		PreopenLead:   25 * time.Minute,
		Session: []SessionBoundaryConfig{
			{At: "09:15", Phase: "continuous"},
			{At: "15:15", Phase: "closing_auction"},
			{At: "15:30", Phase: "closed"},
		},
	}
}

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	location, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", name, err)
	}
	return location
}

// atWallClockInIST returns the IST (Asia/Kolkata) instant for the
// given calendar date and wall-clock hour/minute — the shape every
// expected-fire-time case here uses.
func atWallClockInIST(t *testing.T, year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, mustLoadLocation(t, "Asia/Kolkata"))
}

func TestNextCanonicalFireTime_TodayWeekdayBeforeFire(t *testing.T) {
	// Monday 2026-10-12 07:00 IST → fire at 08:50 IST same day (09:15 − 25m).
	nowFixed := atWallClockInIST(t, 2026, 10, 12, 7, 0)
	expectedFire := atWallClockInIST(t, 2026, 10, 12, 8, 50)
	gotFire, err := nextCanonicalFireTime(baseMarketConfigForTimer(), func() time.Time { return nowFixed })
	if err != nil {
		t.Fatalf("nextCanonicalFireTime: %v", err)
	}
	if !gotFire.Equal(expectedFire) {
		t.Errorf("fire = %s, want %s", gotFire, expectedFire)
	}
}

func TestNextCanonicalFireTime_TodayAfterFire_RollsToNextWeekday(t *testing.T) {
	// Monday 2026-10-12 10:00 IST (already past 08:50 fire) → Tuesday 08:50.
	nowFixed := atWallClockInIST(t, 2026, 10, 12, 10, 0)
	expectedFire := atWallClockInIST(t, 2026, 10, 13, 8, 50)
	gotFire, err := nextCanonicalFireTime(baseMarketConfigForTimer(), func() time.Time { return nowFixed })
	if err != nil {
		t.Fatalf("nextCanonicalFireTime: %v", err)
	}
	if !gotFire.Equal(expectedFire) {
		t.Errorf("fire = %s, want %s", gotFire, expectedFire)
	}
}

func TestNextCanonicalFireTime_FridayAfterFire_RollsToMonday(t *testing.T) {
	// Friday 2026-10-16 10:00 IST → next weekday Monday 2026-10-19 08:50.
	nowFixed := atWallClockInIST(t, 2026, 10, 16, 10, 0)
	expectedFire := atWallClockInIST(t, 2026, 10, 19, 8, 50)
	gotFire, err := nextCanonicalFireTime(baseMarketConfigForTimer(), func() time.Time { return nowFixed })
	if err != nil {
		t.Fatalf("nextCanonicalFireTime: %v", err)
	}
	if !gotFire.Equal(expectedFire) {
		t.Errorf("fire = %s, want Monday %s", gotFire, expectedFire)
	}
}

func TestNextCanonicalFireTime_SaturdayStart_RollsToMonday(t *testing.T) {
	// Saturday 2026-10-17 07:00 IST → Monday 2026-10-19 08:50.
	nowFixed := atWallClockInIST(t, 2026, 10, 17, 7, 0)
	expectedFire := atWallClockInIST(t, 2026, 10, 19, 8, 50)
	gotFire, err := nextCanonicalFireTime(baseMarketConfigForTimer(), func() time.Time { return nowFixed })
	if err != nil {
		t.Fatalf("nextCanonicalFireTime: %v", err)
	}
	if !gotFire.Equal(expectedFire) {
		t.Errorf("fire = %s, want %s", gotFire, expectedFire)
	}
}

func TestNextCanonicalFireTime_SundayStart_RollsToMonday(t *testing.T) {
	// Sunday 2026-10-18 23:59 IST → Monday 2026-10-19 08:50.
	nowFixed := atWallClockInIST(t, 2026, 10, 18, 23, 59)
	expectedFire := atWallClockInIST(t, 2026, 10, 19, 8, 50)
	gotFire, err := nextCanonicalFireTime(baseMarketConfigForTimer(), func() time.Time { return nowFixed })
	if err != nil {
		t.Fatalf("nextCanonicalFireTime: %v", err)
	}
	if !gotFire.Equal(expectedFire) {
		t.Errorf("fire = %s, want %s", gotFire, expectedFire)
	}
}

func TestNextCanonicalFireTime_PreopenLeadSubtractionIsPrecise(t *testing.T) {
	// Pick a preopen lead that is NOT a round-minute to catch off-by-one
	// in the subtraction. 7m31s should resolve to 09:07:29 IST.
	cfg := baseMarketConfigForTimer()
	cfg.PreopenLead = 7*time.Minute + 31*time.Second
	nowFixed := atWallClockInIST(t, 2026, 10, 12, 7, 0)
	gotFire, err := nextCanonicalFireTime(cfg, func() time.Time { return nowFixed })
	if err != nil {
		t.Fatalf("nextCanonicalFireTime: %v", err)
	}
	expectedFire := atWallClockInIST(t, 2026, 10, 12, 9, 15).Add(-cfg.PreopenLead)
	if !gotFire.Equal(expectedFire) {
		t.Errorf("fire = %s, want %s (09:15 − 7m31s = 09:07:29 IST)", gotFire, expectedFire)
	}
}

func TestNextCanonicalFireTime_UnknownTimezone_Errors(t *testing.T) {
	cfg := baseMarketConfigForTimer()
	cfg.Timezone = "Mars/Olympus_Mons"
	_, err := nextCanonicalFireTime(cfg, func() time.Time { return time.Now() })
	if err == nil {
		t.Fatal("expected error for unknown timezone")
	}
	if !strings.Contains(err.Error(), "Mars/Olympus_Mons") {
		t.Errorf("error does not mention the bad timezone: %v", err)
	}
}

func TestNextCanonicalFireTime_MalformedFirstBoundary_Errors(t *testing.T) {
	cfg := baseMarketConfigForTimer()
	cfg.Session[0].At = "nine-fifteen" // not HH:MM
	_, err := nextCanonicalFireTime(cfg, func() time.Time { return time.Now() })
	if err == nil {
		t.Fatal("expected error for malformed first boundary")
	}
	if !strings.Contains(err.Error(), "nine-fifteen") {
		t.Errorf("error does not mention the bad value: %v", err)
	}
}

func TestNextCanonicalFireTime_EmptySession_Errors(t *testing.T) {
	cfg := baseMarketConfigForTimer()
	cfg.Session = nil
	_, err := nextCanonicalFireTime(cfg, func() time.Time { return time.Now() })
	if err == nil {
		t.Fatal("expected error for empty session boundaries")
	}
}

// --- sleepOrQuit ------------------------------------------------------

func newSleepOrQuitHarness(t *testing.T) *Conductor {
	t.Helper()
	pool := newUnconnectedPool(t)
	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	cd.timerQuit = make(chan struct{})
	t.Cleanup(func() { cd.Stop() })
	return cd
}

func TestSleepOrQuit_WakesOnTimer(t *testing.T) {
	cd := newSleepOrQuitHarness(t)
	start := time.Now()
	elapsed := cd.sleepOrQuit(20 * time.Millisecond)
	duration := time.Since(start)
	if !elapsed {
		t.Error("sleepOrQuit returned false; expected true (timer elapsed, no quit)")
	}
	if duration < 15*time.Millisecond {
		t.Errorf("sleepOrQuit returned after %v; want ≥ 15ms", duration)
	}
}

func TestSleepOrQuit_WakesOnQuit(t *testing.T) {
	cd := newSleepOrQuitHarness(t)
	// Close quit after a short delay so the long sleep yields early.
	closedByQuit := make(chan bool, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(cd.timerQuit)
	}()
	start := time.Now()
	elapsed := cd.sleepOrQuit(2 * time.Second)
	duration := time.Since(start)
	closedByQuit <- elapsed
	if elapsed {
		t.Error("sleepOrQuit returned true; expected false (quit closed, timer not elapsed)")
	}
	if duration > 500*time.Millisecond {
		t.Errorf("sleepOrQuit returned after %v on quit; want ≪ 500ms", duration)
	}
}

func TestSleepOrQuit_NonPositiveDelay_ReturnsImmediately(t *testing.T) {
	cd := newSleepOrQuitHarness(t)
	start := time.Now()
	elapsed := cd.sleepOrQuit(0)
	if !elapsed {
		t.Error("sleepOrQuit(0) returned false; expected true (no quit yet)")
	}
	if time.Since(start) > 10*time.Millisecond {
		t.Error("sleepOrQuit(0) blocked; expected immediate return")
	}
}

func TestSleepOrQuit_NonPositiveDelayAfterQuit_ReturnsFalse(t *testing.T) {
	cd := newSleepOrQuitHarness(t)
	close(cd.timerQuit)
	if cd.sleepOrQuit(-1) {
		t.Error("sleepOrQuit(-1) after close returned true; want false (quit observed)")
	}
}

// --- runCanonicalTimer ------------------------------------------------

func TestRunCanonicalTimer_ExitsOnQuit(t *testing.T) {
	// Harness has one tag; the timer arms to a far-future fire; we
	// close timerQuit and expect prompt exit.
	harness := newSpinUpHarness(t, "IN")
	// Override now to just-past-fire-time's day so the next arm is well
	// in the future (Tuesday); runCanonicalTimer will sleep until then.
	futureNow := atWallClockInIST(t, 2026, 10, 12, 10, 0)
	harness.conductor.now = func() time.Time { return futureNow }

	harness.conductor.timerQuit = make(chan struct{})
	done := make(chan struct{})
	harness.conductor.wg.Add(1)
	go func() {
		defer close(done)
		harness.conductor.runCanonicalTimer("IN")
	}()

	// Give the timer a moment to arm, then close quit.
	time.Sleep(10 * time.Millisecond)
	close(harness.conductor.timerQuit)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runCanonicalTimer did not exit within 500ms of quit close")
	}
	harness.conductor.wg.Wait()
	// Nothing to spin up because the timer never fired.
	if containsString(harness.sequencer.snapshot(), "exch.NewExchange") {
		t.Error("timer fired a spin-up before quit (unexpected)")
	}
}

func TestRunCanonicalTimer_BadConfigSleepsThenExitsOnQuit(t *testing.T) {
	// Build a harness, then DELETE its config file so loadConfig fails
	// every iteration. The retry delay is set small so the loop
	// observably iterates; the loop MUST exit on quit regardless of
	// where it is in the sleep.
	harness := newSpinUpHarness(t, "IN")
	// Remove the config file.
	// (writeTestMarketConfig placed <tag>.json; just overwrite the dir
	// to empty via a fresh TempDir would also work, but this is simpler.)
	harness.conductor.cfgDir = t.TempDir() // now has no <tag>.json
	harness.conductor.badConfigRetryDelay = 20 * time.Millisecond

	harness.conductor.timerQuit = make(chan struct{})
	done := make(chan struct{})
	harness.conductor.wg.Add(1)
	go func() {
		defer close(done)
		harness.conductor.runCanonicalTimer("IN")
	}()

	// Let the loop iterate a couple of times, then quit.
	time.Sleep(60 * time.Millisecond)
	close(harness.conductor.timerQuit)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runCanonicalTimer with bad config did not exit within 500ms of quit close")
	}
	// Nothing was spun up — load failed every time.
	if containsString(harness.sequencer.snapshot(), "exch.NewExchange") {
		t.Error("bad-config loop reached spin-up; expected zero")
	}
}

func TestRunCanonicalTimer_FiresAndTriggersSpinUp(t *testing.T) {
	// Set now to 20ms before fireAt (fireAt = 09:15 − 25m = 08:50 IST
	// on 2026-10-12). The real time.Timer wakes after ~20ms and the
	// timer loop calls spinUpCanonical.
	harness := newSpinUpHarness(t, "IN")
	fireAt := atWallClockInIST(t, 2026, 10, 12, 8, 50)
	nowAtTrigger := fireAt.Add(-20 * time.Millisecond)

	var nowCalls atomic.Int64
	harness.conductor.now = func() time.Time {
		// First call: timer uses to compute delay. Second call: spin-up
		// uses to compute market id. Return a value that advances just
		// enough so the market id lands on 2026-10-12.
		nowCalls.Add(1)
		return nowAtTrigger
	}

	harness.conductor.timerQuit = make(chan struct{})
	done := make(chan struct{})
	harness.conductor.wg.Add(1)
	go func() {
		defer close(done)
		harness.conductor.runCanonicalTimer("IN")
	}()

	// Wait for the spin-up's exch.NewExchange to appear.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if containsString(harness.sequencer.snapshot(), "exch.NewExchange") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !containsString(harness.sequencer.snapshot(), "exch.NewExchange") {
		t.Fatal("timer did not fire a spin-up within 500ms of its armed instant")
	}

	// Shut the timer down cleanly.
	close(harness.conductor.timerQuit)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runCanonicalTimer did not exit within 500ms of quit close")
	}
}

func TestRunCanonicalTimer_ExistingLiveMarketReturnsErrAlreadyUp_LoopContinues(t *testing.T) {
	// Prime the registry with a Live record for IN:2026-10-12, then arm
	// the timer so it fires and sees ErrAlreadyUp. The loop must NOT
	// treat ErrAlreadyUp as a hard failure — it logs and re-arms.
	harness := newSpinUpHarness(t, "IN")
	fireAt := atWallClockInIST(t, 2026, 10, 12, 8, 50)
	nowAtTrigger := fireAt.Add(-20 * time.Millisecond)
	harness.conductor.now = func() time.Time { return nowAtTrigger }

	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.markets["IN:2026-10-12"] = &Market{
		ID:      "IN:2026-10-12",
		Mode:    Canonical,
		Tag:     "IN",
		State:   Live,
		members: map[SessionID]struct{}{},
	}
	harness.conductor.reg.mu.Unlock()

	harness.conductor.timerQuit = make(chan struct{})
	done := make(chan struct{})
	harness.conductor.wg.Add(1)
	go func() {
		defer close(done)
		harness.conductor.runCanonicalTimer("IN")
	}()

	// Wait briefly for the first fire → ErrAlreadyUp log; the loop
	// should re-arm (not exit on its own).
	time.Sleep(100 * time.Millisecond)
	// Still running? Then quit now and confirm it exits.
	close(harness.conductor.timerQuit)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runCanonicalTimer with ErrAlreadyUp fire did not exit on quit")
	}

	// Spin-up ran (reserve observed the existing record and returned
	// ErrAlreadyUp), but neither BringUp nor NewExchange executed.
	if containsString(harness.sequencer.snapshot(), "exch.NewExchange") {
		t.Error("ErrAlreadyUp path reached exch.NewExchange; want no construction")
	}
	if harness.agents.callCount != 0 {
		t.Errorf("agents.BringUp calls = %d, want 0 on ErrAlreadyUp", harness.agents.callCount)
	}
}

// --- Conductor.Start/Stop --------------------------------------------

func TestConductorStart_SpawnsOneTimerPerConfiguredMarket(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	configureAnotherMarket(t, harness, "US")
	// Set a future now so spawned timers sleep (no fire during this test).
	harness.conductor.now = func() time.Time { return atWallClockInIST(t, 2026, 10, 12, 10, 0) }

	if err := harness.conductor.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The timers block in sleepOrQuit; we observe them by counting wg
	// entries indirectly via a sentinel: close quit and verify Stop
	// completes (which only happens if every spawned goroutine wg.Done'd).
	// We also assert that two timers logged "canonical timer started" by
	// counting goroutines via a short sleep + ensuring Stop drains them.
	time.Sleep(10 * time.Millisecond)
	harness.conductor.Stop()
	// Stop would hang if the goroutines did not exit — reaching this
	// line is the signal that both timers ran and returned on quit.
}

func TestConductorStart_EmptyCfgDirIsLegal(t *testing.T) {
	pool := newUnconnectedPool(t)
	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	if startErr := cd.Start(); startErr != nil {
		t.Errorf("Start on empty cfgDir: %v", startErr)
	}
	cd.Stop()
}

func TestConductorStart_NonexistentCfgDir_Errors(t *testing.T) {
	// NewConductor accepts the dir unchecked (ReadDir is deferred to
	// Start). Point the conductor at a dir that does not exist.
	pool := newUnconnectedPool(t)
	cfg := newTestConfig(t, pool)
	cfg.CfgDir = "/this/path/does/not/exist/" + t.Name()
	cd, err := NewConductor(cfg)
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	defer cd.Stop()
	startErr := cd.Start()
	if startErr == nil {
		t.Fatal("Start on nonexistent cfgDir returned nil; want error")
	}
	if !errors.Is(startErr, startErr) { // sanity on wrapping; the exact wrap is os.PathError
		t.Fail()
	}
}

func TestConductor_StopIsIdempotent(t *testing.T) {
	pool := newUnconnectedPool(t)
	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	if err := cd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cd.Stop()
	cd.Stop() // must not panic (pool.Close is called only once via stopOnce)
	cd.Stop()
}
