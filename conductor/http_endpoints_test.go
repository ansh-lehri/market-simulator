package conductor

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// TestJoinStatusFor exercises every row of the canonical-join status
// table (and a representative unknown error), both bare and %w-wrapped,
// to lock the mapper against regressions as the flow grows.
func TestJoinStatusFor(t *testing.T) {
	type statusCase struct {
		name string
		err  error
		want int
	}
	cases := []statusCase{
		// Success.
		{"nil to 200", nil, http.StatusOK},

		// Conflicts.
		{"ErrMarketNotLive to 409", ErrMarketNotLive, http.StatusConflict},
		{"ErrAlreadyInSession to 409", ErrAlreadyInSession, http.StatusConflict},
		{"funnel.ErrSessionAlreadyLive to 409", funnel.ErrSessionAlreadyLive, http.StatusConflict},

		// Shard-reply timeout.
		{"funnel.ErrBrokerTimeout to 504", funnel.ErrBrokerTimeout, http.StatusGatewayTimeout},

		// Durable load / snapshot failures from StartSession's loader.
		{"lifecycle.ErrNoAccount to 503", lifecycle.ErrNoAccount, http.StatusServiceUnavailable},
		{"lifecycle.ErrLoadFailed to 503", lifecycle.ErrLoadFailed, http.StatusServiceUnavailable},
		{"lifecycle.ErrSnapshotWriteFailed to 503", lifecycle.ErrSnapshotWriteFailed, http.StatusServiceUnavailable},

		// Conductor-bug seams: both fields are filled by the conductor,
		// so a client never produces these.
		{"funnel.ErrBadBinding to 500", funnel.ErrBadBinding, http.StatusInternalServerError},
		{"funnel.ErrBadParticipant to 500", funnel.ErrBadParticipant, http.StatusInternalServerError},

		// EnsureAccount failure paths. store.ErrNoAccount is distinct
		// from lifecycle.ErrNoAccount (same name, different package); the
		// mapper discriminates by sentinel identity.
		{"store.ErrNoAccount to 500", store.ErrNoAccount, http.StatusInternalServerError},
		{"store.ErrLoadFailed to 500", store.ErrLoadFailed, http.StatusInternalServerError},
		{"lifecycle.ErrBadOwnerType to 500", lifecycle.ErrBadOwnerType, http.StatusInternalServerError},
		{"lifecycle.ErrBadOwnerID to 500", lifecycle.ErrBadOwnerID, http.StatusInternalServerError},
		{"lifecycle.ErrNegativeStartingCapital to 500", lifecycle.ErrNegativeStartingCapital, http.StatusInternalServerError},
		{"lifecycle.ErrNegativeStartingBank to 500", lifecycle.ErrNegativeStartingBank, http.StatusInternalServerError},
		{"lifecycle.ErrCreateFailed to 500", lifecycle.ErrCreateFailed, http.StatusInternalServerError},

		// Anything the switch doesn't recognize must fall through to
		// internal — a leaked unknown error must never be reported as a
		// client-fixable condition.
		{"unknown error to 500", errors.New("something unexpected"), http.StatusInternalServerError},
	}

	// Wrap every non-nil case with %w and assert the mapper still
	// classifies — JoinCanonical surfaces broker errors wrapped with
	// contextual messages (e.g. fmt.Errorf("start session: %w", err)),
	// so the mapper must see through the wrap.
	originals := append([]statusCase(nil), cases...)
	for _, original := range originals {
		if original.err == nil {
			continue
		}
		cases = append(cases, statusCase{
			name: original.name + " (wrapped)",
			err:  fmt.Errorf("join flow: %w", original.err),
			want: original.want,
		})
	}

	for _, singleCase := range cases {
		t.Run(singleCase.name, func(t *testing.T) {
			got := joinStatusFor(singleCase.err)
			if got != singleCase.want {
				t.Fatalf("joinStatusFor(%v) = %d, want %d", singleCase.err, got, singleCase.want)
			}
		})
	}
}
