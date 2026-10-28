package conductor

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// PathGuestQuota is the landing-infrastructure endpoint that returns how many free
// practice sessions a guest identity has used and how many remain.
const PathGuestQuota = "/api/guest/quota"

// PathGuestConsume is the landing-infrastructure endpoint that atomically increments
// the guest's used-session count and returns the updated quota. Returns 409 when the
// limit has already been reached.
const PathGuestConsume = "/api/guest/consume"

// freeSessionLimit is the number of free practice sessions each guest may start before
// being asked to register.
const freeSessionLimit = 3

// guestQuotaResponse is the shared response body for quota and consume.
type guestQuotaResponse struct {
	Used      int `json:"used"`
	Limit     int `json:"limit"`
	Remaining int `json:"remaining"`
}

// handleGuestQuota returns the current free-session quota for a guest_id.
// GET /api/guest/quota?guest_id=<id>
func (conductor *Conductor) handleGuestQuota(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathGuestQuota)

	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathGuestQuota)
		return
	}

	guestID := strings.TrimSpace(request.URL.Query().Get("guest_id"))
	if guestID == "" {
		writeError(responseWriter, http.StatusBadRequest, "missing required query parameter: guest_id", PathGuestQuota)
		return
	}

	if !isValidGuestID(guestID) {
		writeError(responseWriter, http.StatusBadRequest, "invalid guest_id format", PathGuestQuota)
		return
	}

	const selectSQL = `SELECT used FROM guest_free_sessions WHERE guest_id = $1`

	ctx, cancel := context.WithTimeout(request.Context(), conductor.accountOpenTimeout)
	defer cancel()

	var used int
	err := conductor.ohlcv.QueryRow(ctx, selectSQL, guestID).Scan(&used)
	if err != nil {
		// pgx returns pgx.ErrNoRows when no row found — treat as zero usage
		used = 0
	}

	remaining := freeSessionLimit - used
	if remaining < 0 {
		remaining = 0
	}

	conductor.log.Debug("conductor: guest quota read",
		"guest_id", guestID,
		"used", used,
		"remaining", remaining,
	)
	writeJSON(responseWriter, http.StatusOK, guestQuotaResponse{
		Used:      used,
		Limit:     freeSessionLimit,
		Remaining: remaining,
	}, PathGuestQuota)
}

// guestConsumeRequest is the POST body for the consume endpoint.
type guestConsumeRequest struct {
	GuestID string `json:"guest_id"`
}

// handleGuestConsume atomically increments the used count for a guest identity if the
// limit has not been reached. Returns 409 when all free sessions are consumed.
// POST /api/guest/consume
func (conductor *Conductor) handleGuestConsume(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathGuestConsume)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathGuestConsume)
		return
	}

	var body guestConsumeRequest
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathGuestConsume)
		return
	}

	body.GuestID = strings.TrimSpace(body.GuestID)
	if body.GuestID == "" {
		writeError(responseWriter, http.StatusBadRequest, "guest_id is required", PathGuestConsume)
		return
	}

	if !isValidGuestID(body.GuestID) {
		writeError(responseWriter, http.StatusBadRequest, "invalid guest_id format", PathGuestConsume)
		return
	}

	// Atomic upsert: insert or increment used only when used < limit.
	// The WHERE clause on the DO UPDATE means the row is not updated (and no RETURNING row
	// is produced) when the limit has already been reached — that is the 409 signal.
	const consumeSQL = `
INSERT INTO guest_free_sessions (guest_id, used)
VALUES ($1, 1)
ON CONFLICT (guest_id) DO UPDATE
  SET used = guest_free_sessions.used + 1
  WHERE guest_free_sessions.used < $2
RETURNING used`

	ctx, cancel := context.WithTimeout(request.Context(), conductor.accountOpenTimeout)
	defer cancel()

	var used int
	err := conductor.ohlcv.QueryRow(ctx, consumeSQL, body.GuestID, freeSessionLimit).Scan(&used)
	if err != nil {
		// No row returned means the conditional WHERE failed — limit already reached.
		// Check whether this is a pgx "no rows" or a real database error by trying a
		// plain read. If the row exists with used >= limit → 409; on a real DB error → 500.
		const checkSQL = `SELECT used FROM guest_free_sessions WHERE guest_id = $1`
		var currentUsed int
		checkErr := conductor.ohlcv.QueryRow(ctx, checkSQL, body.GuestID).Scan(&currentUsed)
		if checkErr == nil && currentUsed >= freeSessionLimit {
			conductor.log.Info("conductor: guest session limit reached",
				"guest_id", body.GuestID,
				"used", currentUsed,
				"limit", freeSessionLimit,
			)
			writeError(responseWriter, http.StatusConflict, "free_session_limit_reached", PathGuestConsume)
			return
		}
		// Real database error (or the row vanished between queries, extremely unlikely).
		conductor.log.Error("conductor: handleGuestConsume db error",
			"guest_id", body.GuestID,
			"err", err,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathGuestConsume)
		return
	}

	remaining := freeSessionLimit - used
	if remaining < 0 {
		remaining = 0
	}

	conductor.log.Info("conductor: guest session consumed",
		"guest_id", body.GuestID,
		"used", used,
		"remaining", remaining,
	)
	writeJSON(responseWriter, http.StatusOK, guestQuotaResponse{
		Used:      used,
		Limit:     freeSessionLimit,
		Remaining: remaining,
	}, PathGuestConsume)
}

// isValidGuestID checks that the guest_id looks like our minted format
// "guest_<uuid>" — a loose check that prevents trivially malformed IDs reaching
// the database.
func isValidGuestID(guestID string) bool {
	return strings.HasPrefix(guestID, "guest_") && len(guestID) > 6
}
