package funnel

import "testing"

// permissiveInstruments always answers true, regardless of what gets asked.
// A sealed test stub is exercised in place_test.go via sess.instruments
// swap; this file only pins the default.
func TestPermissiveInstruments_AlwaysContains(t *testing.T) {
	set := permissiveInstruments{}
	for _, instrument := range []string{"ACME", "WIDGET", "", "with spaces"} {
		if !set.Contains(instrument) {
			t.Errorf("permissiveInstruments.Contains(%q) = false, want true", instrument)
		}
	}
}

// A nil ownedSession.instruments falls through to the "fail open" branch of
// knownInstrument — same posture as the permissive stub. Guards against a
// mis-wired session dropping orders.
func TestOwnedSession_KnownInstrument_NilIsPermissive(t *testing.T) {
	session := &ownedSession{instruments: nil}
	if !session.knownInstrument("ACME") {
		t.Errorf("knownInstrument on nil InstrumentSet = false, want true (fail-open)")
	}
}

// A sealed InstrumentSet is honoured — returns false for an instrument
// outside the allowlist. This is the branch validateSpec will reach for
// ReasonUnknownInstrument when session-install injects a real set.
func TestOwnedSession_KnownInstrument_SealedSet(t *testing.T) {
	session := &ownedSession{instruments: sealedInstrumentSet("ACME")}
	if !session.knownInstrument("ACME") {
		t.Errorf("knownInstrument(allowed) = false, want true")
	}
	if session.knownInstrument("UNLISTED") {
		t.Errorf("knownInstrument(not-allowed) = true, want false")
	}
}

// sealedInstrumentSet is a test-only closed InstrumentSet; the single allowed
// id is matched by exact equality.
type sealedInstrumentSet string

func (allowed sealedInstrumentSet) Contains(instrument string) bool {
	return string(allowed) == instrument
}
