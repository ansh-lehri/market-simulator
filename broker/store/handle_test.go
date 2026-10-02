package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestAccountHandle_HappyPath_AgentOwner — a seeded (agent, owner_id)
// resolves to its account_id with no error.
func TestAccountHandle_HappyPath_AgentOwner(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	ownerID := prefix + "-owner"
	seedAccount(t, pool, accountID, "agent", ownerID, 100_000, 50_000)

	store := newPoolOnlyStore(pool)
	got, err := store.AccountHandle(context.Background(), "agent", ownerID)
	if err != nil {
		t.Fatalf("AccountHandle: got err %v, want nil", err)
	}
	if got != accountID {
		t.Errorf("AccountHandle: got %q, want %q", got, accountID)
	}
}

// TestAccountHandle_HappyPath_HumanOwner — the "human" branch, same
// shape. Covers both branches of the dual-branched owner_type API
// (feedback_tests_probe_dont_assume).
func TestAccountHandle_HappyPath_HumanOwner(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	ownerID := prefix + "-owner"
	seedAccount(t, pool, accountID, "human", ownerID, 200_000, 0)

	store := newPoolOnlyStore(pool)
	got, err := store.AccountHandle(context.Background(), "human", ownerID)
	if err != nil {
		t.Fatalf("AccountHandle: got err %v, want nil", err)
	}
	if got != accountID {
		t.Errorf("AccountHandle: got %q, want %q", got, accountID)
	}
}

// TestAccountHandle_UnknownOwnerID — an owner_type that exists in the
// seed but a different owner_id surfaces as ErrNoAccount (the signal,
// not a wrapped failure).
func TestAccountHandle_UnknownOwnerID(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	seedAccount(t, pool, prefix+"-acc", "agent", prefix+"-owner", 100_000, 0)

	store := newPoolOnlyStore(pool)
	_, err := store.AccountHandle(context.Background(), "agent", prefix+"-nope")
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("AccountHandle(agent, <missing>): got %v, want ErrNoAccount", err)
	}
}

// TestAccountHandle_WrongOwnerType — seeded as "agent", queried as
// "human" with the same owner_id. Must miss (both filters bind — this
// is the uq_accounts_owner UNIQUE pair). Confirms (human,"x") and
// (agent,"x") are distinct accounts (BROKER_PHASE1 §2.3).
func TestAccountHandle_WrongOwnerType(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	ownerID := prefix + "-owner"
	seedAccount(t, pool, prefix+"-acc", "agent", ownerID, 100_000, 0)

	store := newPoolOnlyStore(pool)
	_, err := store.AccountHandle(context.Background(), "human", ownerID)
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("AccountHandle(human, <agent-owner>): got %v, want ErrNoAccount", err)
	}
}

// TestAccountHandle_OwnerTypeDisambiguation — seed two accounts with
// the SAME owner_id but different owner_types. Each resolves to its
// distinct account_id. This is the invariant uq_accounts_owner UNIQUE
// (owner_type, owner_id) encodes.
func TestAccountHandle_OwnerTypeDisambiguation(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	sharedOwnerID := prefix + "-owner"
	agentAccountID := prefix + "-agent-acc"
	humanAccountID := prefix + "-human-acc"
	seedAccount(t, pool, agentAccountID, "agent", sharedOwnerID, 100_000, 0)
	seedAccount(t, pool, humanAccountID, "human", sharedOwnerID, 200_000, 0)

	store := newPoolOnlyStore(pool)
	gotAgent, err := store.AccountHandle(context.Background(), "agent", sharedOwnerID)
	if err != nil {
		t.Fatalf("AccountHandle(agent): %v", err)
	}
	if gotAgent != agentAccountID {
		t.Errorf("agent branch: got %q, want %q", gotAgent, agentAccountID)
	}
	gotHuman, err := store.AccountHandle(context.Background(), "human", sharedOwnerID)
	if err != nil {
		t.Fatalf("AccountHandle(human): %v", err)
	}
	if gotHuman != humanAccountID {
		t.Errorf("human branch: got %q, want %q", gotHuman, humanAccountID)
	}
}

// TestAccountHandle_EmptyOwnerType — empty owner_type is an argument
// error, surfaced as ErrLoadFailed-wrapped. No SQL is issued.
func TestAccountHandle_EmptyOwnerType(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, err := store.AccountHandle(context.Background(), "", "some-owner")
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("AccountHandle(''): got %v, want ErrLoadFailed", err)
	}
}

// TestAccountHandle_EmptyOwnerID — the symmetric case: empty owner_id
// also fails as ErrLoadFailed (feedback_tests_probe_dont_assume: cover
// both branches of a symmetric guard).
func TestAccountHandle_EmptyOwnerID(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, err := store.AccountHandle(context.Background(), "agent", "")
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("AccountHandle('', ''): got %v, want ErrLoadFailed", err)
	}
}

// TestAccountHandle_CancelledContext — a pre-cancelled context is
// surfaced as an infra failure (ErrLoadFailed-wrapped). We don't assert
// context.Canceled specifically because pgx may map it to a driver
// error; the behavioural claim is "not a signal, not a success".
func TestAccountHandle_CancelledContext(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)
	seedAccount(t, pool, prefix+"-acc", "agent", prefix+"-owner", 0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := newPoolOnlyStore(pool)
	_, err := store.AccountHandle(ctx, "agent", prefix+"-owner")
	if err == nil {
		t.Fatal("AccountHandle(cancelled): got nil err, want infra failure")
	}
	if errors.Is(err, ErrNoAccount) {
		t.Fatalf("AccountHandle(cancelled): got ErrNoAccount, want infra failure")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("AccountHandle(cancelled): got %v, want ErrLoadFailed-wrapped", err)
	}
}

// TestAccountHandle_SkipsIfNotConnected is a sanity probe: without
// setting TEST_DATABASE_URL the helpers skip cleanly. Explicit here so
// a developer running the suite without the env sees a Skip, not a
// panic. Short timeout to keep the test fast when DB IS set — a happy
// Postgres responds in milliseconds.
func TestAccountHandle_FastPathTimeout(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)
	seedAccount(t, pool, prefix+"-acc", "agent", prefix+"-owner", 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := newPoolOnlyStore(pool)
	if _, err := store.AccountHandle(ctx, "agent", prefix+"-owner"); err != nil {
		t.Fatalf("AccountHandle under 5s budget: %v", err)
	}
}
