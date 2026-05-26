package httpapi

import (
	"context"
	"errors"
	"net/http"
)

const (
	healthPath = "/healthz"
	readyPath  = "/readyz"
)

type ReadinessChecker interface {
	Ready(ctx context.Context) error
}

type ReadinessCheckerFunc func(ctx context.Context) error

func (f ReadinessCheckerFunc) Ready(ctx context.Context) error {
	return f(ctx)
}

type HealthHandler struct {
	checker ReadinessChecker
}

type healthResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// NewHealthHandler создает handler для health и readiness endpoints
func NewHealthHandler(checker ReadinessChecker) *HealthHandler {
	return &HealthHandler{checker: checker}
}

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case healthPath:
		h.handleHealth(w, r)
	case readyPath:
		h.handleReady(w, r)
	default:
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "not found"})
	}
}

func (h *HealthHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (h *HealthHandler) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	if h.checker == nil {
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
		return
	}

	if err := h.checker.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status: "not_ready",
			Error:  readinessError(err),
		})
		return
	}

	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func readinessError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "request canceled"
	}
	return err.Error()
}
