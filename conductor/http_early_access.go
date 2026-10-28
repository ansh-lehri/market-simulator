package conductor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// PathEarlyAccessRegister is the landing-infrastructure endpoint for
// early-tester lead capture. Uses /api/ prefix (not /v1/) to keep it
// separate from the simulator contract.
const PathEarlyAccessRegister = "/api/early-access/register"

// earlyAccessRequest is the POST body for lead registration.
type earlyAccessRequest struct {
	Email      string  `json:"email"`
	Experience string  `json:"experience"`
	Market     *string `json:"market"`
	Source     string  `json:"source"`
}

// earlyAccessResponse is the 201 success body.
type earlyAccessResponse struct {
	OK bool `json:"ok"`
}

// handleEarlyAccessRegister captures early-tester leads into the
// early_access_registrations table. Idempotent on duplicate email
// (upsert). Always returns 201 on valid input whether the row was
// new or already existed — the caller never needs to distinguish.
func (conductor *Conductor) handleEarlyAccessRegister(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathEarlyAccessRegister)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathEarlyAccessRegister)
		return
	}

	var body earlyAccessRequest
	if decodeErr := json.NewDecoder(request.Body).Decode(&body); decodeErr != nil {
		conductor.log.Info("conductor: handleEarlyAccessRegister bad body",
			"err", decodeErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathEarlyAccessRegister)
		return
	}

	if !strings.Contains(body.Email, "@") || body.Email == "" {
		writeError(responseWriter, http.StatusBadRequest, "invalid_email", PathEarlyAccessRegister)
		return
	}

	switch body.Experience {
	case "new", "some", "active":
		// valid
	default:
		writeError(responseWriter, http.StatusBadRequest, "missing_experience", PathEarlyAccessRegister)
		return
	}

	// Sanitise market: known values pass through; anything else → null.
	var marketVal *string
	if body.Market != nil {
		switch *body.Market {
		case "india", "us":
			marketVal = body.Market
		}
	}

	const upsertSQL = `
INSERT INTO early_access_registrations (email, experience, market, source)
VALUES ($1, $2, $3, $4)
ON CONFLICT (email) DO UPDATE
  SET experience = EXCLUDED.experience,
      market     = EXCLUDED.market,
      source     = EXCLUDED.source
RETURNING id`

	ctx, cancel := context.WithTimeout(request.Context(), conductor.accountOpenTimeout)
	defer cancel()

	var id int64
	queryErr := conductor.ohlcv.QueryRow(ctx, upsertSQL,
		body.Email, body.Experience, marketVal, body.Source,
	).Scan(&id)
	if queryErr != nil {
		conductor.log.Error("conductor: handleEarlyAccessRegister db error",
			"email_hash", emailHash(body.Email),
			"err", queryErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathEarlyAccessRegister)
		return
	}

	conductor.log.Info("conductor: early access registration recorded",
		"email_hash", emailHash(body.Email),
		"experience", body.Experience,
		"source", body.Source,
		"id", id,
	)
	writeJSON(responseWriter, http.StatusCreated, earlyAccessResponse{OK: true}, PathEarlyAccessRegister)
}

// emailHash returns a short hex digest of the email for structured logs.
// Never logs the raw address.
func emailHash(email string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(email))))
	return fmt.Sprintf("%x", h[:8])
}
