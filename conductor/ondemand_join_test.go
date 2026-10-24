package conductor

import (
	"context"
	"errors"
	"testing"

	"simulator/economy/lifecycle"
)

// TestJoinOnDemand_OwnerTypePassedThroughToEnsureAccount is the direct
// regression test for the real bug: ownerType must reach EnsureAccount
// unchanged, not get hardcoded to OwnerAgent. EnsureAccount is keyed by
// (owner_type, owner_id), so a caller that opened its account as
// OwnerAgentOperator but joins under a hardcoded OwnerAgent would get
// its session installed on a DIFFERENT account row than the one it
// actually holds — every subsequent order/seed-inventory call against
// its real account would then 409 "no session", even though *a*
// session was installed (just not on that account).
func TestJoinOnDemand_OwnerTypePassedThroughToEnsureAccount(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	marketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")
	operatorUserID := UserID("seeder-sell-0")

	if _, err := conductor.JoinOnDemand(context.Background(), operatorUserID, marketID, lifecycle.OwnerAgentOperator); err != nil {
		t.Fatalf("JoinOnDemand: %v", err)
	}
	if broker.ensureAccountCount() != 1 {
		t.Fatalf("EnsureAccount calls = %d, want 1", broker.ensureAccountCount())
	}
	if inv := broker.ensureAccountInvocations[0]; inv.OwnerType != lifecycle.OwnerAgentOperator {
		t.Errorf("EnsureAccount owner_type = %v, want OwnerAgentOperator", inv.OwnerType)
	}
}

// --- JoinOnDemand: happy path ------------------------------------------

func TestJoinOnDemand_HappyPath(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	marketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")
	agentUserID := UserID("agent-42")

	result, err := conductor.JoinOnDemand(context.Background(), agentUserID, marketID, lifecycle.OwnerAgent)
	if err != nil {
		t.Fatalf("JoinOnDemand: %v", err)
	}
	if !isUUIDv4(string(result.SessionID)) || !isUUIDv4(string(result.ParticipantID)) {
		t.Fatalf("result = %+v, want UUID v4 identifiers", result)
	}

	conductor.reg.mu.Lock()
	session, present := conductor.reg.sessions[result.SessionID]
	_, memberPresent := conductor.reg.markets[marketID].members[result.SessionID]
	conductor.reg.mu.Unlock()
	if !present {
		t.Fatal("session record missing from registry")
	}
	if session.State != Active {
		t.Errorf("session.State = %v, want Active", session.State)
	}
	if session.AccountType != Ephemeral {
		t.Errorf("session.AccountType = %v, want Ephemeral", session.AccountType)
	}
	if session.Market != marketID {
		t.Errorf("session.Market = %q, want %q", session.Market, marketID)
	}
	if !memberPresent {
		t.Error("session not added to market.members")
	}

	if broker.ensureAccountCount() != 1 {
		t.Fatalf("EnsureAccount calls = %d, want 1", broker.ensureAccountCount())
	}
	ensureCall := broker.ensureAccountInvocations[0]
	if ensureCall.OwnerType != lifecycle.OwnerAgent {
		t.Errorf("EnsureAccount OwnerType = %v, want OwnerAgent", ensureCall.OwnerType)
	}
	if ensureCall.OwnerID != string(agentUserID) {
		t.Errorf("EnsureAccount OwnerID = %q, want %q", ensureCall.OwnerID, agentUserID)
	}

	if broker.startSessionCount() != 1 {
		t.Fatalf("StartSession calls = %d, want 1", broker.startSessionCount())
	}
	startCmd := broker.startSessionInvocations[0]
	if startCmd.Mode != lifecycle.OnDemand {
		t.Errorf("StartSession Mode = %v, want OnDemand", startCmd.Mode)
	}
	if string(startCmd.ExchangeID) != string(marketID) {
		t.Errorf("StartSession ExchangeID = %q, want %q", startCmd.ExchangeID, marketID)
	}
}

// --- JoinOnDemand: reconnect idempotence -------------------------------

func TestJoinOnDemand_SameAgentSameMarket_Reconnects(t *testing.T) {
	conductor, broker := newJoinHarness(t)
	marketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")
	agentUserID := UserID("agent-42")

	firstResult, firstErr := conductor.JoinOnDemand(context.Background(), agentUserID, marketID, lifecycle.OwnerAgent)
	if firstErr != nil {
		t.Fatalf("first JoinOnDemand: %v", firstErr)
	}
	secondResult, secondErr := conductor.JoinOnDemand(context.Background(), agentUserID, marketID, lifecycle.OwnerAgent)
	if secondErr != nil {
		t.Fatalf("second JoinOnDemand: %v (want reconnect, not an error)", secondErr)
	}
	if secondResult != firstResult {
		t.Errorf("reconnect result = %+v, want %+v (identical to first join)", secondResult, firstResult)
	}
	if broker.ensureAccountCount() != 1 || broker.startSessionCount() != 1 {
		t.Errorf("broker calls after reconnect: ensure=%d start=%d, want 1,1 (reconnect must not call the broker)",
			broker.ensureAccountCount(), broker.startSessionCount())
	}
}

// --- JoinOnDemand: XOR against a different market ----------------------

func TestJoinOnDemand_LiveElsewhere_DifferentMarket(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	firstMarketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")
	secondMarketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-2")
	agentUserID := UserID("agent-42")

	if _, err := conductor.JoinOnDemand(context.Background(), agentUserID, firstMarketID, lifecycle.OwnerAgent); err != nil {
		t.Fatalf("first JoinOnDemand: %v", err)
	}
	if _, err := conductor.JoinOnDemand(context.Background(), agentUserID, secondMarketID, lifecycle.OwnerAgent); !errors.Is(err, ErrLiveElsewhere) {
		t.Fatalf("second JoinOnDemand err = %v, want ErrLiveElsewhere", err)
	}
}

// An agent already bound to a Live canonical session must also be
// refused when joining an on-demand market — byUser is one slot per
// UserID shared across both market modes, not a per-mode gate.
func TestJoinOnDemand_LiveElsewhere_ExistingCanonicalSession(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	installLiveCanonicalMarket(t, conductor, "IN")
	onDemandMarketID, _ := installMarket(t, conductor, OnDemand, Live, "IN", "od-1")
	agentUserID := UserID("agent-42")

	if _, err := conductor.JoinCanonical(context.Background(), agentUserID, "IN", lifecycle.OwnerAgent); err != nil {
		t.Fatalf("JoinCanonical: %v", err)
	}
	if _, err := conductor.JoinOnDemand(context.Background(), agentUserID, onDemandMarketID, lifecycle.OwnerAgent); !errors.Is(err, ErrLiveElsewhere) {
		t.Fatalf("JoinOnDemand err = %v, want ErrLiveElsewhere", err)
	}
}

// --- JoinOnDemand: not-live rejections ---------------------------------

func TestJoinOnDemand_RejectsWhenMarketMissing(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	if _, err := conductor.JoinOnDemand(context.Background(), UserID("agent-1"), MarketID("od:nonexistent"), lifecycle.OwnerAgent); !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("err = %v, want ErrMarketNotLive", err)
	}
}

func TestJoinOnDemand_RejectsCanonicalMarket(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	marketID, _ := installLiveCanonicalMarket(t, conductor, "IN")
	if _, err := conductor.JoinOnDemand(context.Background(), UserID("agent-1"), marketID, lifecycle.OwnerAgent); !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("err = %v, want ErrMarketNotLive (wrong Mode)", err)
	}
}

func TestJoinOnDemand_RejectsProvisioningMarket(t *testing.T) {
	conductor, _ := newJoinHarness(t)
	marketID, _ := installMarket(t, conductor, OnDemand, Provisioning, "IN", "od-1")
	if _, err := conductor.JoinOnDemand(context.Background(), UserID("agent-1"), marketID, lifecycle.OwnerAgent); !errors.Is(err, ErrMarketNotLive) {
		t.Fatalf("err = %v, want ErrMarketNotLive (wrong State)", err)
	}
}
