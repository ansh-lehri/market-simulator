package conductor

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"testing"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/contract"
	"simulator/economy/lifecycle"
)

// --- fake broker for the join flow ------------------------------------

// ensureAccountInvocation records a single EnsureAccount call for later
// assertion.
type ensureAccountInvocation struct {
	OwnerType lifecycle.OwnerType
	OwnerID   string
}

// fakeJoinBroker is the join-flow-focused BrokerPort fake. It records
// every EnsureAccount and StartSession call and lets each test program
// a return function (hooks for the finalize-market-gone test fire inside
// startSessionFunc; this is how we simulate teardown-between-reserve-
// and-finalize without putting a test hook into production code).
//
// Embeds stubBrokerPort so WireInstance + QualifyOnDemand get their
// scaffold defaults — this fake only cares about the two methods join
// drives.
type fakeJoinBroker struct {
	stubBrokerPort

	callLock sync.Mutex

	ensureAccountInvocations []ensureAccountInvocation
	ensureAccountFunc        func(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)

	startSessionInvocations []funnel.SessionStartCmd
	startSessionFunc        func(ctx context.Context, cmd funnel.SessionStartCmd) error
}

func (fake *fakeJoinBroker) EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error) {
	fake.callLock.Lock()
	fake.ensureAccountInvocations = append(fake.ensureAccountInvocations, ensureAccountInvocation{
		OwnerType: ownerType,
		OwnerID:   ownerID,
	})
	fake.callLock.Unlock()
	if fake.ensureAccountFunc == nil {
		return funnel.AccountID("fake-account:" + ownerID), nil
	}
	return fake.ensureAccountFunc(ctx, ownerType, ownerID)
}

func (fake *fakeJoinBroker) StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error {
	fake.callLock.Lock()
	fake.startSessionInvocations = append(fake.startSessionInvocations, cmd)
	fake.callLock.Unlock()
	if fake.startSessionFunc == nil {
		return nil
	}
	return fake.startSessionFunc(ctx, cmd)
}

func (fake *fakeJoinBroker) ensureAccountCount() int {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	return len(fake.ensureAccountInvocations)
}

func (fake *fakeJoinBroker) startSessionCount() int {
	fake.callLock.Lock()
	defer fake.callLock.Unlock()
	return len(fake.startSessionInvocations)
}

var _ BrokerPort = (*fakeJoinBroker)(nil)

// --- harness ----------------------------------------------------------

// newJoinHarness builds a Conductor backed by the fake broker, with the
// scaffold's stub market-data port and a never-connected pgx pool
// (JoinCanonical never touches the pool).
func newJoinHarness(t *testing.T) (*Conductor, *fakeJoinBroker) {
	t.Helper()
	pool := newUnconnectedPool(t)
	t.Cleanup(pool.Close)
	broker := &fakeJoinBroker{}
	conductor, err := NewConductor(Config{
		CfgDir:    t.TempDir(),
		OHLCVPool: pool,
		Broker:    broker,
		Market:    stubMarketDataPort{},
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	return conductor, broker
}

// installMarket places a bare Market record directly in the registry.
// Bypasses spin-up because join tests only care about the Market's
// {Mode, State, Tag, inbound, members} fields; everything else (exch,
// fanout, closeAt) is irrelevant to admission.
func installMarket(t *testing.T, conductor *Conductor, mode Mode, state MarketState, tag MarketTag, suffix string) (MarketID, chan contract.InboundMessage) {
	t.Helper()
	marketID := MarketID(string(tag) + ":" + suffix)
	inbound := make(chan contract.InboundMessage, 1)
	market := &Market{
		ID:      marketID,
		Mode:    mode,
		Tag:     tag,
		State:   state,
		inbound: inbound,
		members: map[SessionID]struct{}{},
	}
	conductor.reg.mu.Lock()
	conductor.reg.markets[marketID] = market
	conductor.reg.mu.Unlock()
	return marketID, inbound
}

// installLiveCanonicalMarket is the common case: Canonical + Live.
func installLiveCanonicalMarket(t *testing.T, conductor *Conductor, tag MarketTag) (MarketID, chan contract.InboundMessage) {
	t.Helper()
	return installMarket(t, conductor, Canonical, Live, tag, "test")
}

// --- UUID shape check -------------------------------------------------

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func isUUIDv4(candidate string) bool {
	return uuidV4Pattern.MatchString(candidate)
}

func TestNewUUIDv4_ShapeAndUniqueness(t *testing.T) {
	seen := map[string]struct{}{}
	for range 128 {
		id := newUUIDv4()
		if !isUUIDv4(id) {
			t.Fatalf("newUUIDv4 returned %q, not a UUID v4", id)
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("newUUIDv4 collision on %q", id)
		}
		seen[id] = struct{}{}
	}
}

// --- JoinCanonical: happy path ---------------------------------------

func TestJoinCanonical_HappyPath(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")
	userID := UserID("human-42")

	result, err := conductor.JoinCanonical(context.Background(), userID, "IN")
	if err != nil {
		t.Fatalf("JoinCanonical: %v", err)
	}

	// Identifiers must be opaque UUID v4s and distinct.
	if !isUUIDv4(string(result.SessionID)) {
		t.Errorf("SessionID %q is not a UUID v4", result.SessionID)
	}
	if !isUUIDv4(string(result.ParticipantID)) {
		t.Errorf("ParticipantID %q is not a UUID v4", result.ParticipantID)
	}
	if string(result.SessionID) == string(result.ParticipantID) {
		t.Error("SessionID and ParticipantID must be distinct")
	}

	// Session record: Active, Durable, points at the user + market, has
	// Account + Participant stamped.
	conductor.reg.mu.Lock()
	session, present := conductor.reg.sessions[result.SessionID]
	byUserSession := conductor.reg.byUser[userID]
	_, memberPresent := conductor.reg.markets[marketID].members[result.SessionID]
	conductor.reg.mu.Unlock()
	if !present {
		t.Fatal("session record missing from registry")
	}
	if session.State != Active {
		t.Errorf("session.State = %v, want Active", session.State)
	}
	if session.AccountType != Durable {
		t.Errorf("session.AccountType = %v, want Durable", session.AccountType)
	}
	if session.User != userID {
		t.Errorf("session.User = %q, want %q", session.User, userID)
	}
	if session.Market != marketID {
		t.Errorf("session.Market = %q, want %q", session.Market, marketID)
	}
	if session.Account == "" {
		t.Error("session.Account not stamped")
	}
	if session.Participant != result.ParticipantID {
		t.Errorf("session.Participant = %q, want %q", session.Participant, result.ParticipantID)
	}
	if byUserSession != session {
		t.Error("byUser slot does not point at the installed session")
	}
	if !memberPresent {
		t.Error("session not added to market.members")
	}

	// Broker call shape.
	if broker.ensureAccountCount() != 1 {
		t.Fatalf("EnsureAccount calls = %d, want 1", broker.ensureAccountCount())
	}
	ensureCall := broker.ensureAccountInvocations[0]
	if ensureCall.OwnerType != lifecycle.OwnerHuman {
		t.Errorf("EnsureAccount OwnerType = %v, want OwnerHuman", ensureCall.OwnerType)
	}
	if ensureCall.OwnerID != string(userID) {
		t.Errorf("EnsureAccount OwnerID = %q, want %q", ensureCall.OwnerID, userID)
	}

	if broker.startSessionCount() != 1 {
		t.Fatalf("StartSession calls = %d, want 1", broker.startSessionCount())
	}
	startCmd := broker.startSessionInvocations[0]
	if startCmd.Mode != lifecycle.Canonical {
		t.Errorf("StartSession Mode = %v, want Canonical", startCmd.Mode)
	}
	if startCmd.AccountID == "" {
		t.Error("StartSession AccountID empty")
	}
	if string(startCmd.SessionID) != string(result.SessionID) {
		t.Errorf("StartSession SessionID = %q, want %q", startCmd.SessionID, result.SessionID)
	}
	if string(startCmd.ExchangeID) != string(marketID) {
		t.Errorf("StartSession ExchangeID = %q, want %q", startCmd.ExchangeID, marketID)
	}
	if startCmd.ExchangeInbound == nil {
		t.Error("StartSession ExchangeInbound nil")
	}
	if string(startCmd.Participant) != string(result.ParticipantID) {
		t.Errorf("StartSession Participant = %q, want %q", startCmd.Participant, result.ParticipantID)
	}
}

// --- JoinCanonical: per-user XOR front-gate --------------------------

func TestJoinCanonical_XORFrontGate(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")
	userID := UserID("human-42")

	if _, err := conductor.JoinCanonical(context.Background(), userID, "IN"); err != nil {
		t.Fatalf("first JoinCanonical: %v", err)
	}
	_, secondErr := conductor.JoinCanonical(context.Background(), userID, "IN")
	if !errors.Is(secondErr, ErrAlreadyInSession) {
		t.Fatalf("second JoinCanonical err = %v, want ErrAlreadyInSession", secondErr)
	}

	// Front-gate must short-circuit before any broker I/O; the broker
	// got exactly the one pair of calls from the first join.
	if broker.ensureAccountCount() != 1 {
		t.Errorf("EnsureAccount calls = %d, want 1", broker.ensureAccountCount())
	}
	if broker.startSessionCount() != 1 {
		t.Errorf("StartSession calls = %d, want 1", broker.startSessionCount())
	}
}

// --- JoinCanonical: no Live canonical market -------------------------

func TestJoinCanonical_NoLiveMarket(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	// Deliberately no market installed.

	_, err := conductor.JoinCanonical(context.Background(), UserID("human-42"), "IN")
	if !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("err = %v, want ErrMarketNotLive", err)
	}
	if broker.ensureAccountCount() != 0 || broker.startSessionCount() != 0 {
		t.Errorf("broker should have received zero calls, got ensure=%d start=%d",
			broker.ensureAccountCount(), broker.startSessionCount())
	}
	conductor.reg.mu.Lock()
	sessionsCount := len(conductor.reg.sessions)
	byUserCount := len(conductor.reg.byUser)
	conductor.reg.mu.Unlock()
	if sessionsCount != 0 || byUserCount != 0 {
		t.Errorf("registry residue: sessions=%d byUser=%d", sessionsCount, byUserCount)
	}
}

// --- findLiveCanonical selectivity -----------------------------------

func TestJoinCanonical_FindLiveCanonicalSelectivity(t *testing.T) {
	cases := []struct {
		name       string
		mode       Mode
		state      MarketState
		wantAccept bool
	}{
		{"canonical live", Canonical, Live, true},
		{"canonical provisioning", Canonical, Provisioning, false},
		{"canonical tearing down", Canonical, TearingDown, false},
		{"on-demand live (wrong mode)", OnDemand, Live, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conductor, _ := newJoinHarness(t)
			installMarket(t, conductor, tc.mode, tc.state, "IN", "candidate")

			_, err := conductor.JoinCanonical(context.Background(), UserID("human-x"), "IN")
			if tc.wantAccept {
				if err != nil {
					t.Fatalf("expected success, got %v", err)
				}
			} else {
				if !errors.Is(err, ErrMarketNotLive) {
					t.Fatalf("expected ErrMarketNotLive, got %v", err)
				}
			}
		})
	}
}

// --- EnsureAccount failures roll back + skip StartSession -----------

func TestJoinCanonical_EnsureAccountFailureRollsBack(t *testing.T) {
	cases := []error{
		lifecycle.ErrBadOwnerType,
		lifecycle.ErrBadOwnerID,
		lifecycle.ErrCreateFailed,
		lifecycle.ErrNegativeStartingCapital,
		store.ErrNoAccount,
		store.ErrLoadFailed,
	}
	for _, forcedErr := range cases {
		t.Run(forcedErr.Error(), func(t *testing.T) {
			conductor, broker := newJoinHarness(t)
			installLiveCanonicalMarket(t, conductor, "IN")
			broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
				return "", forcedErr
			}

			_, err := conductor.JoinCanonical(context.Background(), UserID("human-x"), "IN")
			if !errors.Is(err, forcedErr) {
				t.Fatalf("err = %v, want wrap of %v", err, forcedErr)
			}
			if broker.startSessionCount() != 0 {
				t.Error("StartSession must not be called when EnsureAccount fails")
			}
			conductor.reg.mu.Lock()
			sessionsCount := len(conductor.reg.sessions)
			byUserCount := len(conductor.reg.byUser)
			conductor.reg.mu.Unlock()
			if sessionsCount != 0 || byUserCount != 0 {
				t.Errorf("rollback incomplete: sessions=%d byUser=%d", sessionsCount, byUserCount)
			}
		})
	}
}

// --- StartSession failures roll back; EnsureAccount already ran -----

func TestJoinCanonical_StartSessionFailureRollsBack(t *testing.T) {
	cases := []error{
		funnel.ErrBadBinding,
		funnel.ErrBadParticipant,
		funnel.ErrSessionAlreadyLive,
		funnel.ErrBrokerTimeout,
		lifecycle.ErrNoAccount,
		lifecycle.ErrLoadFailed,
		lifecycle.ErrSnapshotWriteFailed,
	}
	for _, forcedErr := range cases {
		t.Run(forcedErr.Error(), func(t *testing.T) {
			conductor, broker := newJoinHarness(t)
			marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")
			broker.startSessionFunc = func(context.Context, funnel.SessionStartCmd) error {
				return forcedErr
			}

			_, err := conductor.JoinCanonical(context.Background(), UserID("human-x"), "IN")
			if !errors.Is(err, forcedErr) {
				t.Fatalf("err = %v, want wrap of %v", err, forcedErr)
			}
			if broker.ensureAccountCount() != 1 {
				t.Errorf("EnsureAccount should still have run; got %d calls", broker.ensureAccountCount())
			}
			conductor.reg.mu.Lock()
			sessionsCount := len(conductor.reg.sessions)
			byUserCount := len(conductor.reg.byUser)
			memberCount := len(conductor.reg.markets[marketID].members)
			conductor.reg.mu.Unlock()
			if sessionsCount != 0 || byUserCount != 0 {
				t.Errorf("rollback incomplete: sessions=%d byUser=%d", sessionsCount, byUserCount)
			}
			if memberCount != 0 {
				t.Errorf("members must not be touched when finalize never runs; got %d", memberCount)
			}
		})
	}
}

// --- Finalize market-gone guard --------------------------------------

func TestJoinCanonical_FinalizeMarketGoneGuard(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")
	userID := UserID("human-42")

	// Simulate teardown landing between reserve-unlock and finalize-lock:
	// the fake StartSession deletes the market record just before
	// returning nil. JoinCanonical must still succeed, not panic, and
	// must not write to a non-existent members map.
	broker.startSessionFunc = func(context.Context, funnel.SessionStartCmd) error {
		conductor.reg.mu.Lock()
		delete(conductor.reg.markets, marketID)
		conductor.reg.mu.Unlock()
		return nil
	}

	result, err := conductor.JoinCanonical(context.Background(), userID, "IN")
	if err != nil {
		t.Fatalf("JoinCanonical: %v", err)
	}
	conductor.reg.mu.Lock()
	installedSession, sessionPresent := conductor.reg.sessions[result.SessionID]
	byUserSession, byUserPresent := conductor.reg.byUser[userID]
	_, marketPresent := conductor.reg.markets[marketID]
	conductor.reg.mu.Unlock()

	if !sessionPresent {
		t.Error("session must be installed even when market is gone at finalize")
	}
	if installedSession.State != Active {
		t.Errorf("session.State = %v, want Active", installedSession.State)
	}
	if !byUserPresent || byUserSession != installedSession {
		t.Error("byUser must still point at the installed session")
	}
	if marketPresent {
		t.Error("market record must stay deleted (test hook removed it)")
	}
}

// --- Concurrent joins by distinct users (race surface) --------------

func TestJoinCanonical_ConcurrentJoinsDistinctUsers(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")

	const numUsers = 32
	errorsChan := make(chan error, numUsers)
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	for userIndex := range numUsers {
		userID := UserID(fmt.Sprintf("human-%d", userIndex))
		go func() {
			startBarrier.Wait()
			_, joinErr := conductor.JoinCanonical(context.Background(), userID, "IN")
			errorsChan <- joinErr
		}()
	}
	startBarrier.Done()
	for range numUsers {
		if err := <-errorsChan; err != nil {
			t.Errorf("concurrent join failed: %v", err)
		}
	}
	conductor.reg.mu.Lock()
	sessionsCount := len(conductor.reg.sessions)
	byUserCount := len(conductor.reg.byUser)
	conductor.reg.mu.Unlock()
	if sessionsCount != numUsers || byUserCount != numUsers {
		t.Errorf("sessions=%d byUser=%d, want both %d", sessionsCount, byUserCount, numUsers)
	}
}

// --- Concurrent joins by the SAME user: one wins, N-1 see front-gate.

func TestJoinCanonical_ConcurrentJoinsSameUser(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")
	userID := UserID("human-concurrent")

	const numAttempts = 16
	errorsChan := make(chan error, numAttempts)
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	for range numAttempts {
		go func() {
			startBarrier.Wait()
			_, joinErr := conductor.JoinCanonical(context.Background(), userID, "IN")
			errorsChan <- joinErr
		}()
	}
	startBarrier.Done()

	var wins, frontGateHits int
	for range numAttempts {
		switch err := <-errorsChan; {
		case err == nil:
			wins++
		case errors.Is(err, ErrAlreadyInSession):
			frontGateHits++
		default:
			t.Errorf("unexpected err: %v", err)
		}
	}
	if wins != 1 {
		t.Errorf("wins = %d, want exactly 1 (per-user XOR)", wins)
	}
	if frontGateHits != numAttempts-1 {
		t.Errorf("frontGateHits = %d, want %d", frontGateHits, numAttempts-1)
	}
	// Only the one winner reached the broker.
	if broker.ensureAccountCount() != 1 || broker.startSessionCount() != 1 {
		t.Errorf("broker call counts: ensure=%d start=%d, want 1 each",
			broker.ensureAccountCount(), broker.startSessionCount())
	}
}
