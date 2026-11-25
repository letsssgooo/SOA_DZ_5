package http

import (
	"context"
	"net/http"
	"time"
)

func NewHandler(recalculate func(context.Context, time.Time) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /aggregations", func(w http.ResponseWriter, r *http.Request) {
		day, err := time.Parse(time.DateOnly, r.URL.Query().Get("date"))
		if err != nil {
			http.Error(w, "date must be in YYYY-MM-DD format", http.StatusBadRequest)
			return
		}
		if err := recalculate(r.Context(), day); err != nil {
			http.Error(w, "aggregation failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}
