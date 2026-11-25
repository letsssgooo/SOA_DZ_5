package http

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
	"github.com/letsssgooo/SOA_DZ_5/movie-service/internal/service"
	"google.golang.org/protobuf/encoding/protojson"
)

const maxEventBytes = 1 << 20

type eventHandler struct {
	events *service.Events
	logger *slog.Logger
}

func NewHandler(events *service.Events, logger *slog.Logger) http.Handler {
	handler := eventHandler{events: events, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handler.postEvent)
	return mux
}

func (h eventHandler) postEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxEventBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read event JSON")
		return
	}

	event := new(eventsv1.MovieEvent)
	if err := protojson.Unmarshal(body, event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid event JSON")
		return
	}

	if err := h.events.Publish(r.Context(), event); err != nil {
		if errors.Is(err, service.ErrInvalidEvent) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.logger.WarnContext(r.Context(), "event publish failed", "event_id", event.GetEventId(), "error", err)
		writeError(w, http.StatusServiceUnavailable, "event publication unavailable")
		return
	}
	writeJSON(w, http.StatusOK, response{EventID: event.GetEventId()})
}

type response struct {
	EventID string `json:"event_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, response{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, body response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
