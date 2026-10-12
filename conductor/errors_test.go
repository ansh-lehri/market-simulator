package conductor

import (
	"errors"
	"testing"
)

// Sentinels must be distinct comparable values — callers dispatch on
// them with errors.Is, so a silent aliasing (e.g. two sentinels pointing
// at the same errors.New) would make dispatch ambiguous.
func TestSentinels_AreDistinct(t *testing.T) {
	sentinels := map[string]error{
		"ErrAlreadyUp":               ErrAlreadyUp,
		"ErrOrchestratorUnavailable": ErrOrchestratorUnavailable,
		"ErrOrchestratorTimeout":     ErrOrchestratorTimeout,
		"ErrAgentBringUpRejected":    ErrAgentBringUpRejected,
	}
	for firstName, firstErr := range sentinels {
		for secondName, secondErr := range sentinels {
			if firstName == secondName {
				continue
			}
			if errors.Is(firstErr, secondErr) {
				t.Errorf("%s and %s are aliased: errors.Is matches both ways", firstName, secondName)
			}
		}
	}
}

// Each sentinel's message is non-empty and starts with the "conductor:"
// prefix so log scraping can find them. Loose contract, not a format
// test.
func TestSentinels_HaveConductorPrefixedMessages(t *testing.T) {
	for _, sentinel := range []error{
		ErrAlreadyUp,
		ErrOrchestratorUnavailable,
		ErrOrchestratorTimeout,
		ErrAgentBringUpRejected,
	} {
		if sentinel.Error() == "" {
			t.Errorf("sentinel has empty message: %v", sentinel)
		}
		if len(sentinel.Error()) < len("conductor: ") || sentinel.Error()[:len("conductor: ")] != "conductor: " {
			t.Errorf("sentinel message %q does not begin with %q", sentinel.Error(), "conductor: ")
		}
	}
}
