package conductor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"simulator/contract"
	"simulator/exchange/clock"
)

// Fixed "now" for every schedule assembly test — a Monday so the
// weekday-roll logic the timer stage adds later never has to be
// considered here. India timezone keeps the wall-clock parsing on the
// IST +5:30 offset, which is the hardest practical case.
var scheduleTestNow = time.Date(2026, 10, 12, 7, 0, 0, 0, time.UTC) // Monday 07:00 UTC = 12:30 IST

func istLocation(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("LoadLocation(Asia/Kolkata): %v", err)
	}
	return loc
}

// marketConfigForTest returns an IN-shaped canonical config with the
// three standard phase boundaries (continuous open → closing auction →
// closed) and one instrument. Tests mutate copies of this to construct
// edge cases.
func marketConfigForTest() MarketConfig {
	return MarketConfig{
		Market:        "IN",
		Timezone:      "Asia/Kolkata",
		OHLCVInterval: time.Minute,
		NumShards:     0, // default-exercised
		PreopenLead:   25 * time.Minute,
		Session: []SessionBoundaryConfig{
			{At: "09:15", Phase: "continuous"},
			{At: "15:15", Phase: "closing_auction"},
			{At: "15:30", Phase: "closed"},
		},
		Instruments: []InstrumentConfig{
			{
				ID:        contract.InstrumentID("RELIANCE"),
				TickSize:  contract.Ticks(5),
				BandBps:   1000,
				SeedPrice: contract.Ticks(250000),
			},
		},
	}
}

func fixedNowFn() func() time.Time {
	return func() time.Time { return scheduleTestNow }
}

func readerReturningClose(closePrice int64) *fakeCandleReader {
	return &fakeCandleReader{returnRow: fakeCandleRow{closePrice: closePrice}}
}

func readerReturningNoRow() *fakeCandleReader {
	return &fakeCandleReader{returnRow: fakeCandleRow{scanErr: pgx.ErrNoRows}}
}

func TestBuildExchangeArgs_NumShardsDefaultsToOneWhenZero(t *testing.T) {
	cfg := marketConfigForTest()
	cfg.NumShards = 0
	numShards, _, _, _, _, err := buildExchangeArgs(context.Background(), readerReturningClose(251000), fixedNowFn(), cfg, istLocation(t))
	if err != nil {
		t.Fatalf("buildExchangeArgs: %v", err)
	}
	if numShards != defaultNumShards {
		t.Errorf("numShards = %d, want %d (default)", numShards, defaultNumShards)
	}
}

func TestBuildExchangeArgs_NumShardsPreservesExplicitValue(t *testing.T) {
	cfg := marketConfigForTest()
	cfg.NumShards = 4
	numShards, _, _, _, _, err := buildExchangeArgs(context.Background(), readerReturningClose(251000), fixedNowFn(), cfg, istLocation(t))
	if err != nil {
		t.Fatalf("buildExchangeArgs: %v", err)
	}
	if numShards != 4 {
		t.Errorf("numShards = %d, want 4 (explicit)", numShards)
	}
}

func TestBuildExchangeArgs_ScheduleAnchoredToTodayInMarketTimezone(t *testing.T) {
	location := istLocation(t)
	numShards, schedule, _, openInstant, closeInstant, err := buildExchangeArgs(
		context.Background(), readerReturningClose(251000), fixedNowFn(), marketConfigForTest(), location,
	)
	if err != nil {
		t.Fatalf("buildExchangeArgs: %v", err)
	}
	if numShards < 1 {
		t.Fatalf("numShards = %d", numShards)
	}
	// scheduleTestNow is Monday 2026-10-12 07:00 UTC; in IST that's the
	// same date 12:30 IST. All three boundaries anchor on 2026-10-12 IST.
	expectedOpen := time.Date(2026, 10, 12, 9, 15, 0, 0, location)
	expectedAuction := time.Date(2026, 10, 12, 15, 15, 0, 0, location)
	expectedClose := time.Date(2026, 10, 12, 15, 30, 0, 0, location)
	if !schedule.Boundaries[0].At.Equal(expectedOpen) {
		t.Errorf("boundary[0].At = %s, want %s", schedule.Boundaries[0].At, expectedOpen)
	}
	if !schedule.Boundaries[1].At.Equal(expectedAuction) {
		t.Errorf("boundary[1].At = %s, want %s", schedule.Boundaries[1].At, expectedAuction)
	}
	if !schedule.Boundaries[2].At.Equal(expectedClose) {
		t.Errorf("boundary[2].At = %s, want %s", schedule.Boundaries[2].At, expectedClose)
	}
	if schedule.Boundaries[0].Phase != clock.PhaseContinuous {
		t.Errorf("boundary[0].Phase = %v, want PhaseContinuous", schedule.Boundaries[0].Phase)
	}
	if schedule.Boundaries[1].Phase != clock.PhaseClosingAuction {
		t.Errorf("boundary[1].Phase = %v, want PhaseClosingAuction", schedule.Boundaries[1].Phase)
	}
	if schedule.Boundaries[2].Phase != clock.PhaseClosed {
		t.Errorf("boundary[2].Phase = %v, want PhaseClosed", schedule.Boundaries[2].Phase)
	}
	if !openInstant.Equal(expectedOpen) {
		t.Errorf("openInstant = %s, want %s", openInstant, expectedOpen)
	}
	if !closeInstant.Equal(expectedClose) {
		t.Errorf("closeInstant = %s, want %s", closeInstant, expectedClose)
	}
}

func TestBuildExchangeArgs_SpecsCarryReferenceAndRules(t *testing.T) {
	const storedClose int64 = 251575
	_, _, specs, openInstant, _, err := buildExchangeArgs(
		context.Background(), readerReturningClose(storedClose), fixedNowFn(), marketConfigForTest(), istLocation(t),
	)
	if err != nil {
		t.Fatalf("buildExchangeArgs: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("specs count = %d, want 1", len(specs))
	}
	spec := specs[0]
	if spec.Instrument != contract.InstrumentID("RELIANCE") {
		t.Errorf("spec.Instrument = %q, want %q", spec.Instrument, "RELIANCE")
	}
	if spec.Reference != contract.Ticks(storedClose) {
		t.Errorf("spec.Reference = %d, want %d (the stored close)", spec.Reference, storedClose)
	}
	if spec.BandBps != 1000 {
		t.Errorf("spec.BandBps = %d, want 1000", spec.BandBps)
	}
	if spec.Rules.TickSize != contract.Ticks(5) {
		t.Errorf("spec.Rules.TickSize = %d, want 5", spec.Rules.TickSize)
	}
	if spec.OHLCVInterval != time.Minute {
		t.Errorf("spec.OHLCVInterval = %v, want 1m", spec.OHLCVInterval)
	}
	if !spec.OHLCVStart.Equal(openInstant) {
		t.Errorf("spec.OHLCVStart = %s, want openInstant %s", spec.OHLCVStart, openInstant)
	}
}

func TestBuildExchangeArgs_SpecsFallBackToSeedPriceOnNoRow(t *testing.T) {
	_, _, specs, _, _, err := buildExchangeArgs(
		context.Background(), readerReturningNoRow(), fixedNowFn(), marketConfigForTest(), istLocation(t),
	)
	if err != nil {
		t.Fatalf("buildExchangeArgs with no prior candles: %v", err)
	}
	if specs[0].Reference != contract.Ticks(250000) {
		t.Errorf("spec.Reference = %d on no-row, want seed price 250000", specs[0].Reference)
	}
}

func TestBuildExchangeArgs_ReferenceReadError_Bubbles(t *testing.T) {
	driverFailure := errors.New("connection reset by peer")
	reader := &fakeCandleReader{returnRow: fakeCandleRow{scanErr: driverFailure}}

	_, _, specs, _, _, err := buildExchangeArgs(
		context.Background(), reader, fixedNowFn(), marketConfigForTest(), istLocation(t),
	)
	if err == nil {
		t.Fatal("expected error on reference read failure")
	}
	if !errors.Is(err, driverFailure) {
		t.Errorf("error does not wrap driver failure: %v", err)
	}
	if specs != nil {
		t.Errorf("specs non-nil on error: %v", specs)
	}
}

// --- Schedule validation table -----------------------------------------

func TestBuildExchangeArgs_RejectsBadSchedules(t *testing.T) {
	cases := []struct {
		name       string
		session    []SessionBoundaryConfig
		wantSubstr string
	}{
		{
			name: "unknown_phase_label",
			session: []SessionBoundaryConfig{
				{At: "09:15", Phase: "warmup"}, // not a known label
				{At: "15:30", Phase: "closed"},
			},
			wantSubstr: "unknown phase label",
		},
		{
			name: "malformed_wall_clock",
			session: []SessionBoundaryConfig{
				{At: "9h15m", Phase: "continuous"}, // not HH:MM
				{At: "15:30", Phase: "closed"},
			},
			wantSubstr: "session[0].at",
		},
		{
			name: "single_boundary",
			session: []SessionBoundaryConfig{
				{At: "09:15", Phase: "continuous"},
			},
			wantSubstr: "want at least 2",
		},
		{
			name: "first_boundary_closed",
			session: []SessionBoundaryConfig{
				{At: "09:15", Phase: "closed"},
				{At: "15:30", Phase: "closed"},
			},
			wantSubstr: "first boundary phase is PhaseClosed",
		},
		{
			name: "equal_timestamps",
			session: []SessionBoundaryConfig{
				{At: "09:15", Phase: "continuous"},
				{At: "09:15", Phase: "closed"}, // same instant → not strictly ascending
			},
			wantSubstr: "not strictly ascending",
		},
		{
			name: "descending_timestamps",
			session: []SessionBoundaryConfig{
				{At: "15:30", Phase: "continuous"},
				{At: "09:15", Phase: "closed"},
			},
			wantSubstr: "not strictly ascending",
		},
		{
			name: "terminal_not_closed",
			session: []SessionBoundaryConfig{
				{At: "09:15", Phase: "continuous"},
				{At: "15:30", Phase: "continuous"}, // ends in continuous, not closed
			},
			wantSubstr: "terminal boundary phase",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := marketConfigForTest()
			cfg.Session = testCase.session
			_, _, _, _, _, err := buildExchangeArgs(
				context.Background(), readerReturningClose(251000), fixedNowFn(), cfg, istLocation(t),
			)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", testCase.wantSubstr)
			}
			if !containsSubstring(err.Error(), testCase.wantSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), testCase.wantSubstr)
			}
		})
	}
}

func TestBuildExchangeArgs_ScheduleValidationPrecedesReferenceRead(t *testing.T) {
	// Bad schedule + reader that would fail: the schedule validator must
	// reject before any reference lookup fires, so the reader records
	// zero calls.
	reader := &fakeCandleReader{returnRow: fakeCandleRow{scanErr: errors.New("should not be called")}}
	cfg := marketConfigForTest()
	cfg.Session = []SessionBoundaryConfig{
		{At: "09:15", Phase: "closed"}, // first phase closed → validation error
		{At: "15:30", Phase: "closed"},
	}
	_, _, _, _, _, err := buildExchangeArgs(context.Background(), reader, fixedNowFn(), cfg, istLocation(t))
	if err == nil {
		t.Fatal("expected schedule validation error")
	}
	if reader.calls != 0 {
		t.Errorf("reader was called %d times before schedule validation; want 0", reader.calls)
	}
}

// --- buildInstrumentSpecs (standalone helper) --------------------------

// TestBuildInstrumentSpecs_StandAlone exercises the extracted helper
// directly — the shape the on-demand bring-up calls (passing a
// construct-time openInstant that is NOT a wall-clock session open).
// The canonical caller already exercises the helper transitively; this
// test proves the helper is callable in isolation with any openInstant
// a future caller supplies, and that every spec field is wired through
// the same mapping the canonical path uses.
func TestBuildInstrumentSpecs_StandAlone(t *testing.T) {
	instruments := []InstrumentConfig{
		{
			ID:        contract.InstrumentID("RELIANCE"),
			TickSize:  contract.Ticks(5),
			BandBps:   1000,
			SeedPrice: contract.Ticks(250000),
		},
		{
			ID:        contract.InstrumentID("INFY"),
			TickSize:  contract.Ticks(10),
			BandBps:   750,
			SeedPrice: contract.Ticks(150000),
		},
	}
	// Construct-time anchor NOT aligned to any session open — proves the
	// helper does not reach back to a schedule.
	openInstant := time.Date(2026, 10, 12, 11, 23, 47, 0, time.UTC)
	const storedCloseReliance int64 = 251575
	const storedCloseInfy int64 = 151200
	// Two sequential QueryRow calls — first returns reliance close, second
	// returns infy close. fakeCandleReader has one canned row; stage it to
	// return each in order.
	type stagedReader struct {
		fakeCandleReader
		queue []fakeCandleRow
	}
	reader := &stagedReader{queue: []fakeCandleRow{
		{closePrice: storedCloseReliance},
		{closePrice: storedCloseInfy},
	}}
	queryRow := func(ctx context.Context, sql string, args ...any) pgx.Row {
		row := reader.queue[0]
		reader.queue = reader.queue[1:]
		reader.calls++
		reader.lastSQL = sql
		reader.lastArgs = args
		return row
	}
	wrapper := candleReaderFn(queryRow)

	specs, err := buildInstrumentSpecs(
		context.Background(), wrapper, instruments, MarketTag("IN"), time.Minute, openInstant,
	)
	if err != nil {
		t.Fatalf("buildInstrumentSpecs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("specs count = %d, want 2", len(specs))
	}

	expected := []struct {
		instrument contract.InstrumentID
		reference  contract.Ticks
		band       int
		tickSize   contract.Ticks
	}{
		{"RELIANCE", contract.Ticks(storedCloseReliance), 1000, contract.Ticks(5)},
		{"INFY", contract.Ticks(storedCloseInfy), 750, contract.Ticks(10)},
	}
	for specIndex, want := range expected {
		got := specs[specIndex]
		if got.Instrument != want.instrument {
			t.Errorf("specs[%d].Instrument = %q, want %q", specIndex, got.Instrument, want.instrument)
		}
		if got.Reference != want.reference {
			t.Errorf("specs[%d].Reference = %d, want %d", specIndex, got.Reference, want.reference)
		}
		if got.BandBps != want.band {
			t.Errorf("specs[%d].BandBps = %d, want %d", specIndex, got.BandBps, want.band)
		}
		if got.Rules.TickSize != want.tickSize {
			t.Errorf("specs[%d].Rules.TickSize = %d, want %d", specIndex, got.Rules.TickSize, want.tickSize)
		}
		if got.OHLCVInterval != time.Minute {
			t.Errorf("specs[%d].OHLCVInterval = %v, want 1m", specIndex, got.OHLCVInterval)
		}
		if !got.OHLCVStart.Equal(openInstant) {
			t.Errorf("specs[%d].OHLCVStart = %s, want openInstant %s", specIndex, got.OHLCVStart, openInstant)
		}
	}
}

// TestBuildInstrumentSpecs_SeedPriceFallbackOnNoRow proves the helper
// alone (no surrounding schedule) treats a missing prior candle as a
// seed-price fallback, matching the behavior canonical bring-up
// depends on for a brand-new instrument's first-ever session.
func TestBuildInstrumentSpecs_SeedPriceFallbackOnNoRow(t *testing.T) {
	instruments := []InstrumentConfig{
		{ID: contract.InstrumentID("NEWLISTING"), TickSize: 1, BandBps: 500, SeedPrice: contract.Ticks(99999)},
	}
	specs, err := buildInstrumentSpecs(
		context.Background(),
		readerReturningNoRow(),
		instruments, MarketTag("IN"), time.Minute,
		time.Date(2026, 10, 12, 11, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("buildInstrumentSpecs on no-row: %v", err)
	}
	if specs[0].Reference != contract.Ticks(99999) {
		t.Errorf("Reference = %d, want seed price 99999", specs[0].Reference)
	}
}

// TestBuildInstrumentSpecs_ReferenceReadError_Bubbles proves the
// helper short-circuits on the first database error and wraps it
// enough to identify the offending instrument.
func TestBuildInstrumentSpecs_ReferenceReadError_Bubbles(t *testing.T) {
	driverFailure := errors.New("connection reset by peer")
	reader := &fakeCandleReader{returnRow: fakeCandleRow{scanErr: driverFailure}}
	instruments := []InstrumentConfig{
		{ID: contract.InstrumentID("RELIANCE"), TickSize: 5, BandBps: 1000, SeedPrice: 250000},
		{ID: contract.InstrumentID("INFY"), TickSize: 10, BandBps: 750, SeedPrice: 150000},
	}
	specs, err := buildInstrumentSpecs(
		context.Background(), reader, instruments, MarketTag("IN"), time.Minute,
		time.Date(2026, 10, 12, 11, 0, 0, 0, time.UTC),
	)
	if err == nil {
		t.Fatal("expected error on reference read failure")
	}
	if !errors.Is(err, driverFailure) {
		t.Errorf("error does not wrap driver failure: %v", err)
	}
	if specs != nil {
		t.Errorf("specs non-nil on error: %v", specs)
	}
	// Short-circuit: reader was called exactly once (for RELIANCE), not
	// twice. The second instrument's lookup must not have fired.
	if reader.calls != 1 {
		t.Errorf("reader.calls = %d, want 1 (short-circuit on first error)", reader.calls)
	}
}

// candleReaderFn adapts a bare function value to the candleReader
// interface — lets the staged-reader test above write its QueryRow
// behavior inline without defining a whole fake type.
type candleReaderFn func(ctx context.Context, sql string, args ...any) pgx.Row

func (fn candleReaderFn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return fn(ctx, sql, args...)
}

// --- phase label helper -----------------------------------------------

func TestPhaseFromConfigLabel_AcceptsThreeLabels(t *testing.T) {
	cases := map[string]clock.Phase{
		phaseLabelContinuous:     clock.PhaseContinuous,
		phaseLabelClosingAuction: clock.PhaseClosingAuction,
		phaseLabelClosed:         clock.PhaseClosed,
	}
	for label, want := range cases {
		got, err := phaseFromConfigLabel(label)
		if err != nil {
			t.Errorf("phaseFromConfigLabel(%q): %v", label, err)
			continue
		}
		if got != want {
			t.Errorf("phaseFromConfigLabel(%q) = %v, want %v", label, got, want)
		}
	}
}

func TestPhaseFromConfigLabel_RejectsUnknown(t *testing.T) {
	_, err := phaseFromConfigLabel("preopen")
	if err == nil {
		t.Fatal("phaseFromConfigLabel(\"preopen\"): want error")
	}
}

// containsSubstring is a tiny stdlib-free substring helper so tests do
// not pull in strings just for this.
func containsSubstring(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for startIndex := 0; startIndex+len(needle) <= len(haystack); startIndex++ {
		if haystack[startIndex:startIndex+len(needle)] == needle {
			return true
		}
	}
	return false
}
