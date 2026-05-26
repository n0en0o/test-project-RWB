package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandlerReturnsOK(t *testing.T) {
	t.Parallel()

	handler := NewHealthHandler(nil)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	var body healthResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "ok" {
		t.Fatalf("Status = %q, want %q", body.Status, "ok")
	}
}

func TestHealthHandlerReadyReturnsOKWithoutChecker(t *testing.T) {
	t.Parallel()

	handler := NewHealthHandler(nil)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestHealthHandlerReadyUsesChecker(t *testing.T) {
	t.Parallel()

	handler := NewHealthHandler(ReadinessCheckerFunc(func(ctx context.Context) error {
		return nil
	}))
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestHealthHandlerReadyReturnsServiceUnavailable(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("kafka is not ready")
	handler := NewHealthHandler(ReadinessCheckerFunc(func(ctx context.Context) error {
		return checkErr
	}))
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	var body healthResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "not_ready" {
		t.Fatalf("Status = %q, want %q", body.Status, "not_ready")
	}
	if body.Error != checkErr.Error() {
		t.Fatalf("Error = %q, want %q", body.Error, checkErr.Error())
	}
}

func TestHealthHandlerRejectsUnsupportedMethod(t *testing.T) {
	t.Parallel()

	handler := NewHealthHandler(nil)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/healthz", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}
