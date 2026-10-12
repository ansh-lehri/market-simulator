package conductor

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewRegistry_MapsInitialized(t *testing.T) {
	reg := newRegistry()
	if reg == nil {
		t.Fatal("newRegistry returned nil")
	}
	if reg.markets == nil || reg.sessions == nil || reg.byUser == nil {
		t.Fatal("maps must be initialized")
	}
	if len(reg.markets) != 0 || len(reg.sessions) != 0 || len(reg.byUser) != 0 {
		t.Fatal("maps must start empty")
	}
}

// TestRegistry_ReserveFinalizePattern simulates the lock-only-for-µs
// reserve → off-lock → finalize dance (the one-mutex rule) without any
// flow logic. The scaffold ships the registry + mutex; later phases
// add the flow helpers.
func TestRegistry_ReserveFinalizePattern(t *testing.T) {
	reg := newRegistry()
	marketID := MarketID("IN:2026-10-12")

	reg.mu.Lock()
	if _, duplicate := reg.markets[marketID]; duplicate {
		reg.mu.Unlock()
		t.Fatal("unexpected duplicate on empty registry")
	}
	reg.markets[marketID] = &Market{
		ID:      marketID,
		Mode:    Canonical,
		Tag:     "IN",
		State:   Provisioning,
		members: map[SessionID]struct{}{},
	}
	reg.mu.Unlock()

	// Imagine slow off-lock work here (BRING_UP, NewExchange, WireInstance,
	// Start) — the mutex must be released across those call-outs.

	reg.mu.Lock()
	market := reg.markets[marketID]
	if market == nil || market.State != Provisioning {
		reg.mu.Unlock()
		t.Fatalf("provisioning record missing or wrong state: %+v", market)
	}
	market.State = Live
	reg.mu.Unlock()

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.markets[marketID].State != Live {
		t.Errorf("state = %v, want Live", reg.markets[marketID].State)
	}
}

// TestRegistry_RollbackDeletesProvisioning covers the
// abort-provisioning pattern — any fallible step before the exchange
// is wired-and-started causes the Provisioning record to be deleted
// (no goroutines were spawned, nothing to tear down).
func TestRegistry_RollbackDeletesProvisioning(t *testing.T) {
	reg := newRegistry()
	marketID := MarketID("IN:2026-10-12")

	reg.mu.Lock()
	reg.markets[marketID] = &Market{
		ID:      marketID,
		State:   Provisioning,
		members: map[SessionID]struct{}{},
	}
	reg.mu.Unlock()

	reg.mu.Lock()
	delete(reg.markets, marketID)
	reg.mu.Unlock()

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, exists := reg.markets[marketID]; exists {
		t.Error("provisioning record not deleted")
	}
}

// TestRegistry_SessionReserveWithXOR covers the join flow's reservation
// pattern — the byUser slot IS the reservation. A second Starting
// session for the same user must see the slot occupied (the per-user
// one-session XOR check).
func TestRegistry_SessionReserveWithXOR(t *testing.T) {
	reg := newRegistry()
	userID := UserID("user-1")
	sessionID := SessionID("sess-1")

	reg.mu.Lock()
	if _, occupied := reg.byUser[userID]; occupied {
		reg.mu.Unlock()
		t.Fatal("byUser should be empty")
	}
	session := &Session{
		ID:          sessionID,
		User:        userID,
		AccountType: Durable,
		State:       Starting,
	}
	reg.sessions[sessionID] = session
	reg.byUser[userID] = session
	reg.mu.Unlock()

	// Second reservation attempt for same user — must see the slot.
	reg.mu.Lock()
	if existing, occupied := reg.byUser[userID]; !occupied || existing.ID != sessionID {
		reg.mu.Unlock()
		t.Fatalf("byUser slot not held: existing=%+v, occupied=%v", existing, occupied)
	}
	reg.mu.Unlock()
}

// TestRegistry_ConcurrentWritesAndReads is the -race surface for the
// scaffold. Many goroutines insert distinct markets + sessions under
// the mutex; concurrent readers scan the maps. The lock is the whole
// discipline.
func TestRegistry_ConcurrentWritesAndReads(t *testing.T) {
	reg := newRegistry()
	const (
		writers   = 8
		perWriter = 50
		readers   = 4
		reads     = 500
	)
	var waitGroup sync.WaitGroup
	for writerIndex := 0; writerIndex < writers; writerIndex++ {
		waitGroup.Add(1)
		go func(wi int) {
			defer waitGroup.Done()
			for j := 0; j < perWriter; j++ {
				marketID := MarketID(fmt.Sprintf("IN:%d:%d", wi, j))
				sessionID := SessionID(fmt.Sprintf("sess:%d:%d", wi, j))
				userID := UserID(fmt.Sprintf("user:%d:%d", wi, j))
				reg.mu.Lock()
				reg.markets[marketID] = &Market{ID: marketID, State: Provisioning}
				session := &Session{ID: sessionID, User: userID, State: Starting}
				reg.sessions[sessionID] = session
				reg.byUser[userID] = session
				reg.mu.Unlock()
			}
		}(writerIndex)
	}
	for readerIndex := 0; readerIndex < readers; readerIndex++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for i := 0; i < reads; i++ {
				reg.mu.Lock()
				_ = len(reg.markets) + len(reg.sessions) + len(reg.byUser)
				reg.mu.Unlock()
			}
		}()
	}
	waitGroup.Wait()

	wantCount := writers * perWriter
	if got := len(reg.markets); got != wantCount {
		t.Errorf("markets count = %d, want %d", got, wantCount)
	}
	if got := len(reg.sessions); got != wantCount {
		t.Errorf("sessions count = %d, want %d", got, wantCount)
	}
	if got := len(reg.byUser); got != wantCount {
		t.Errorf("byUser count = %d, want %d", got, wantCount)
	}
}

func TestEnumStrings(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"Canonical", Canonical.String(), "canonical"},
		{"OnDemand", OnDemand.String(), "ondemand"},
		{"Provisioning", Provisioning.String(), "provisioning"},
		{"Live", Live.String(), "live"},
		{"TearingDown", TearingDown.String(), "tearing_down"},
		{"Starting", Starting.String(), "starting"},
		{"Active", Active.String(), "active"},
		{"Stopping", Stopping.String(), "stopping"},
		{"Ended", Ended.String(), "ended"},
		{"Failed", Failed.String(), "failed"},
		{"Durable", Durable.String(), "durable"},
		{"Ephemeral", Ephemeral.String(), "ephemeral"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if Mode(99).String() != "invalid" {
		t.Error("invalid Mode should stringify as invalid")
	}
	if MarketState(99).String() != "invalid" {
		t.Error("invalid MarketState should stringify as invalid")
	}
	if SessionState(99).String() != "invalid" {
		t.Error("invalid SessionState should stringify as invalid")
	}
	if AccountType(99).String() != "invalid" {
		t.Error("invalid AccountType should stringify as invalid")
	}
}
