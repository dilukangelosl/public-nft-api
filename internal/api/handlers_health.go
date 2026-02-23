package api

import (
	"context"
	"net/http"
	"runtime"
	"time"
)

// GET /v1/health
func (a *API) HandleHealth(w http.ResponseWriter, r *http.Request) {
	// Database ping
	dbStatus := "down"
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := a.Store.Pool.Ping(ctx); err == nil {
		dbStatus = "up"
	}

	// Memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	RespondJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "up",
		"database":    dbStatus,
		"goroutines":  runtime.NumGoroutine(),
		"memory_alloc_mb": m.Alloc / 1024 / 1024,
		"timestamp":   time.Now().Unix(),
	})
}
