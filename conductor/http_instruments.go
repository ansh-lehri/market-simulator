package conductor

import (
	"errors"
	"io/fs"
	"net/http"

	"simulator/contract"
)

// instrumentEntry is one row of the instrument list. Strictly static:
// id + tick_size are the two fields a trading UI needs to render
// order-entry increments and label rows. Prices (LTP/quote/depth) are
// never served here — they stream over the market-data WebSocket.
type instrumentEntry struct {
	ID       contract.InstrumentID `json:"id"`
	TickSize contract.Ticks        `json:"tick_size"`
}

// instrumentListResponse is the top-level envelope. market echoes the
// query parameter so a client logging a single response has the full
// context without correlating with its own request.
type instrumentListResponse struct {
	Market      string            `json:"market"`
	Instruments []instrumentEntry `json:"instruments"`
}

// handleInstruments serves the per-market static instrument list.
// Config-only read; no registry, no liveness check — the list is
// available whether the market is currently open, closed, or has not
// been brought up at all. The three failure modes have distinct
// statuses so the client can distinguish "wrong URL" from "wrong
// market" from "server broke":
//
//   - 400 when the ?market= query parameter is missing or empty.
//   - 404 when no <tag>.json exists for the requested market (the
//     ReadFile call surfaces fs.ErrNotExist through loadConfig's
//     fmt.Errorf wrap, so errors.Is picks it up).
//   - 500 when the config file is present but malformed (parse error,
//     bad duration string, etc.) — the catalog itself is broken, not
//     the client's request.
func (conductor *Conductor) handleInstruments(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathInstruments)

	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathInstruments)
		return
	}

	marketTagParam := request.URL.Query().Get("market")
	if marketTagParam == "" {
		conductor.log.Info("conductor: handleInstruments missing market param",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "market required", PathInstruments)
		return
	}

	marketConfig, loadErr := loadConfig(conductor.cfgDir, MarketTag(marketTagParam))
	if loadErr != nil {
		if errors.Is(loadErr, fs.ErrNotExist) {
			conductor.log.Info("conductor: handleInstruments unknown market",
				"market", marketTagParam,
				"remote_addr", request.RemoteAddr,
			)
			writeError(responseWriter, http.StatusNotFound, "unknown market", PathInstruments)
			return
		}
		conductor.log.Error("conductor: handleInstruments config load failed",
			"market", marketTagParam,
			"err", loadErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "config load failed", PathInstruments)
		return
	}

	entries := make([]instrumentEntry, 0, len(marketConfig.Instruments))
	for _, instrumentCfg := range marketConfig.Instruments {
		entries = append(entries, instrumentEntry{
			ID:       instrumentCfg.ID,
			TickSize: instrumentCfg.TickSize,
		})
	}
	writeJSON(responseWriter, http.StatusOK, instrumentListResponse{
		Market:      marketTagParam,
		Instruments: entries,
	}, PathInstruments)
}
