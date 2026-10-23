package conductor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// agentBringUpWireRequest mirrors simulation-agents/transport
// .BringUpRequest field-for-field (same JSON tags, same shape). The two
// services are separate Go modules reached only over HTTP, so this type
// is a deliberate local duplicate of the wire contract, not a shared
// import — there is nothing to import across a module boundary like
// this one. The real server decodes with DisallowUnknownFields, so this
// struct must carry exactly these fields, no more, no fewer.
type agentBringUpWireRequest struct {
	ExchangeID   string    `json:"exchange_id"`
	SessionType  string    `json:"session_type"`
	Country      string    `json:"country"`
	SessionStart time.Time `json:"session_start"`
	SessionEnd   time.Time `json:"session_end"`
	Instruments  []string  `json:"instruments"`
	BrokerID     string    `json:"broker_id"`
	Scenario     string    `json:"scenario,omitempty"`
}

// agentBringUpWireAck mirrors transport.BringUpAck.
type agentBringUpWireAck struct {
	ExchangeID string `json:"exchange_id"`
	Status     string `json:"status"`
}

// agentWindDownWireRequest mirrors transport's unexported
// windDownWireRequest shape.
type agentWindDownWireRequest struct {
	ExchangeID   string `json:"exchange_id"`
	TeardownMode string `json:"teardown_mode"`
}

// agentErrorEnvelope mirrors transport's errorEnvelope — the body the
// real server writes on every non-2xx/204 response.
type agentErrorEnvelope struct {
	Error string `json:"error"`
}

// httpAgentOrchestrator is the production AgentOrchestrator: an HTTP
// client against the simulation-agents process's transport.Server
// (POST /bringup, POST /winddown — see that repo's transport/server.go,
// orchestrator.go, handlers.go). Both calls are synchronous requests
// bounded by the caller-supplied ctx, matching the shape the stub
// already presented — NOT fire-and-forget at the transport level, even
// though WindDown's call sites treat its outcome as log-and-ignore (the
// transport doc comment on the real server's AgentOrchestrator
// interface is explicit that BringUp "returns once [the synchronous
// prefix is] complete," with the bring-up tail continuing in the
// background — this client's single POST/response round trip is
// exactly that synchronous prefix).
//
// baseURL, brokerID, and country are fixed for the lifetime of one
// Conductor process (one broker, one deployment's country). They are
// configured ONCE, via NewHTTPAgentOrchestrator, and never hardcoded a
// second time anywhere else in this codebase — see cmd/server/main.go,
// the only production call site.
type httpAgentOrchestrator struct {
	baseURL    string
	brokerID   string
	country    string
	httpClient *http.Client
	log        *slog.Logger
}

// NewHTTPAgentOrchestrator builds the production AgentOrchestrator.
//
//   - baseURL is the agent server's root (e.g. "http://localhost:8090",
//     no trailing slash required — one is trimmed if present). This is
//     the single place that address is configured; every call this
//     client makes derives its target from this one value.
//   - brokerID and country are stamped onto every outbound
//     BringUpRequest (the real server's multi-broker-routing and
//     trait-distribution fields respectively) — constant per
//     conductor/orchestrator pairing, so they live here rather than on
//     every call site.
//   - A nil logger falls back to slog.Default(). There is no separate
//     client-level timeout knob: every call is bounded by the ctx the
//     caller passes in (conductor's own BringUpTimeout), matching the
//     "handler owns the deadline" convention used everywhere else in
//     this codebase rather than introducing a second, possibly
//     disagreeing timeout source.
func NewHTTPAgentOrchestrator(baseURL, brokerID, country string, log *slog.Logger) AgentOrchestrator {
	if log == nil {
		log = slog.Default()
	}
	return &httpAgentOrchestrator{
		baseURL:    trimTrailingSlash(baseURL),
		brokerID:   brokerID,
		country:    country,
		httpClient: &http.Client{},
		log:        log,
	}
}

func trimTrailingSlash(url string) string {
	if len(url) > 0 && url[len(url)-1] == '/' {
		return url[:len(url)-1]
	}
	return url
}

// BringUp POSTs to /bringup and maps the response into the three
// sentinel categories errors.go documents: ErrOrchestratorTimeout for a
// context that was already done or that expired mid-call,
// ErrOrchestratorUnavailable for a transport failure or a 5xx, and
// ErrAgentBringUpRejected for a 4xx (the server was reached and
// explicitly refused the request shape).
func (client *httpAgentOrchestrator) BringUp(ctx context.Context, request AgentBringUpRequest) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		client.log.Warn("conductor: httpAgentOrchestrator.BringUp context already done",
			"exchange_id", request.ExchangeID,
			"err", ctxErr,
		)
		return fmt.Errorf("%w: %w", ErrOrchestratorTimeout, ctxErr)
	}

	wireRequest := agentBringUpWireRequest{
		ExchangeID:   string(request.ExchangeID),
		SessionType:  request.Mode.String(),
		Country:      client.country,
		SessionStart: request.SessionStart,
		SessionEnd:   request.SessionEnd,
		Instruments:  request.Instruments,
		BrokerID:     client.brokerID,
		Scenario:     request.Scenario,
	}
	body, marshalErr := json.Marshal(wireRequest)
	if marshalErr != nil {
		// Every field is a plain string/time.Time/[]string — a marshal
		// failure here is a dev-fatal invariant violation, not a
		// business error this caller can recover from.
		panic(fmt.Sprintf("conductor: httpAgentOrchestrator.BringUp: marshal request: %v", marshalErr))
	}

	httpRequest, buildErr := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+"/bringup", bytes.NewReader(body))
	if buildErr != nil {
		panic(fmt.Sprintf("conductor: httpAgentOrchestrator.BringUp: build request: %v", buildErr))
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	client.log.Debug("conductor: httpAgentOrchestrator.BringUp calling agent server",
		"exchange_id", request.ExchangeID,
		"market", request.Market,
		"mode", request.Mode,
		"instrument_count", len(request.Instruments),
	)
	response, doErr := client.httpClient.Do(httpRequest)
	if doErr != nil {
		if isTimeoutErr(doErr) {
			client.log.Warn("conductor: httpAgentOrchestrator.BringUp timed out",
				"exchange_id", request.ExchangeID,
				"err", doErr,
			)
			return fmt.Errorf("%w: %w", ErrOrchestratorTimeout, doErr)
		}
		client.log.Warn("conductor: httpAgentOrchestrator.BringUp transport failure",
			"exchange_id", request.ExchangeID,
			"err", doErr,
		)
		return fmt.Errorf("%w: %w", ErrOrchestratorUnavailable, doErr)
	}
	defer response.Body.Close()

	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		var ack agentBringUpWireAck
		if decodeErr := json.NewDecoder(response.Body).Decode(&ack); decodeErr != nil {
			client.log.Warn("conductor: httpAgentOrchestrator.BringUp ack decode failed",
				"exchange_id", request.ExchangeID,
				"err", decodeErr,
			)
			return fmt.Errorf("%w: decode ack: %w", ErrOrchestratorUnavailable, decodeErr)
		}
		client.log.Info("conductor: httpAgentOrchestrator.BringUp succeeded",
			"exchange_id", request.ExchangeID,
			"ack_status", ack.Status,
		)
		return nil
	case response.StatusCode >= 400 && response.StatusCode < 500:
		client.log.Warn("conductor: httpAgentOrchestrator.BringUp rejected",
			"exchange_id", request.ExchangeID,
			"status", response.StatusCode,
			"body", readAgentErrorBody(response.Body),
		)
		return fmt.Errorf("%w: agent server status %d", ErrAgentBringUpRejected, response.StatusCode)
	default:
		client.log.Warn("conductor: httpAgentOrchestrator.BringUp server error",
			"exchange_id", request.ExchangeID,
			"status", response.StatusCode,
			"body", readAgentErrorBody(response.Body),
		)
		return fmt.Errorf("%w: agent server status %d", ErrOrchestratorUnavailable, response.StatusCode)
	}
}

// WindDown POSTs to /winddown. Every call site in this package treats a
// non-nil return as log-and-ignore (WindDown never blocks a teardown's
// forward progress — see canonical_close.go / session_leave.go), but
// the call itself is a real network request, not a no-op.
//
// teardownMode sent on the wire is this Mode's own String() form
// ("canonical" / "ondemand"). The real orchestrator's teardown_mode
// vocabulary is not specified in any doc this client's implementation
// was written against (transport/orchestrator.go takes a bare string
// with no documented enum) — this is the most defensible available
// choice (it reuses the same vocabulary BringUp's session_type already
// sends), not a confirmed contract. Flagged here in case the real
// orchestrator expects a different vocabulary once verified against it
// directly.
func (client *httpAgentOrchestrator) WindDown(ctx context.Context, exchangeID MarketID, market MarketTag, mode Mode) error {
	wireRequest := agentWindDownWireRequest{
		ExchangeID:   string(exchangeID),
		TeardownMode: mode.String(),
	}
	body, marshalErr := json.Marshal(wireRequest)
	if marshalErr != nil {
		panic(fmt.Sprintf("conductor: httpAgentOrchestrator.WindDown: marshal request: %v", marshalErr))
	}

	httpRequest, buildErr := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+"/winddown", bytes.NewReader(body))
	if buildErr != nil {
		panic(fmt.Sprintf("conductor: httpAgentOrchestrator.WindDown: build request: %v", buildErr))
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	client.log.Debug("conductor: httpAgentOrchestrator.WindDown calling agent server",
		"exchange_id", exchangeID,
		"market", market,
		"mode", mode,
	)
	response, doErr := client.httpClient.Do(httpRequest)
	if doErr != nil {
		if isTimeoutErr(doErr) {
			client.log.Warn("conductor: httpAgentOrchestrator.WindDown timed out",
				"exchange_id", exchangeID,
				"err", doErr,
			)
			return fmt.Errorf("%w: %w", ErrOrchestratorTimeout, doErr)
		}
		client.log.Warn("conductor: httpAgentOrchestrator.WindDown transport failure",
			"exchange_id", exchangeID,
			"err", doErr,
		)
		return fmt.Errorf("%w: %w", ErrOrchestratorUnavailable, doErr)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNoContent || (response.StatusCode >= 200 && response.StatusCode < 300) {
		client.log.Info("conductor: httpAgentOrchestrator.WindDown succeeded",
			"exchange_id", exchangeID,
		)
		return nil
	}
	client.log.Warn("conductor: httpAgentOrchestrator.WindDown non-success",
		"exchange_id", exchangeID,
		"status", response.StatusCode,
		"body", readAgentErrorBody(response.Body),
	)
	return fmt.Errorf("%w: agent server status %d", ErrOrchestratorUnavailable, response.StatusCode)
}

// isTimeoutErr reports whether err represents a context-deadline/cancel
// condition or an http.Client-level timeout, so both call sites can map
// it to ErrOrchestratorTimeout rather than the broader
// ErrOrchestratorUnavailable transport-failure bucket.
func isTimeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) {
		return timeoutErr.Timeout()
	}
	return false
}

// readAgentErrorBody best-effort decodes the real server's
// errorEnvelope for a log line; falls back to a byte-limited raw read
// if the body isn't that shape. Never returns an error — this is purely
// diagnostic.
func readAgentErrorBody(body io.Reader) string {
	const maxLoggedBytes = 2048
	limited := io.LimitReader(body, maxLoggedBytes)
	raw, readErr := io.ReadAll(limited)
	if readErr != nil {
		return ""
	}
	var envelope agentErrorEnvelope
	if jsonErr := json.Unmarshal(raw, &envelope); jsonErr == nil && envelope.Error != "" {
		return envelope.Error
	}
	return string(raw)
}

// Compile-time: the production adapter satisfies AgentOrchestrator.
var _ AgentOrchestrator = (*httpAgentOrchestrator)(nil)
