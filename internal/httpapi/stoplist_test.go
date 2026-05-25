package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"test-project-rwb/internal/stoplist"
)

func TestStopListHandlerAddsQuery(t *testing.T) {
	t.Parallel()

	storage := stoplist.NewStore()
	handler := newTestStopListHandler(t, storage)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/stop-list", bytes.NewBufferString(`{"query":"  IPHONE   15 Pro "}`))

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusCreated)
	}
	if !storage.Contains("iphone 15 pro") {
		t.Fatalf("Contains() = false, want true")
	}

	var body stopListItemResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Query != "iphone 15 pro" {
		t.Fatalf("Query = %q, want %q", body.Query, "iphone 15 pro")
	}
}

func TestStopListHandlerListsQueries(t *testing.T) {
	t.Parallel()

	storage := stoplist.NewStore()
	for _, query := range []string{"lego", "iphone"} {
		if _, err := storage.Add(query); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}
	handler := newTestStopListHandler(t, storage)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stop-list", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	var body stopListResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Count != 2 {
		t.Fatalf("Count = %d, want 2", body.Count)
	}
	if body.Items[0] != "iphone" || body.Items[1] != "lego" {
		t.Fatalf("Items = %+v, want sorted items", body.Items)
	}
}

func TestStopListHandlerDeletesQuery(t *testing.T) {
	t.Parallel()

	storage := stoplist.NewStore()
	if _, err := storage.Add("iphone 15 pro"); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	handler := newTestStopListHandler(t, storage)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/stop-list/iphone%2015%20pro", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if storage.Contains("iphone 15 pro") {
		t.Fatalf("Contains() = true, want false")
	}
}

func TestStopListHandlerRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		target string
		body   string
		status int
	}{
		{name: "invalid json", method: http.MethodPost, target: "/stop-list", body: `{bad json}`, status: http.StatusBadRequest},
		{name: "empty query", method: http.MethodPost, target: "/stop-list", body: `{"query":" "}`, status: http.StatusBadRequest},
		{name: "unsupported collection method", method: http.MethodPut, target: "/stop-list", status: http.StatusMethodNotAllowed},
		{name: "unsupported item method", method: http.MethodGet, target: "/stop-list/iphone", status: http.StatusMethodNotAllowed},
		{name: "empty item", method: http.MethodDelete, target: "/stop-list/%20", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := newTestStopListHandler(t, stoplist.NewStore())
			response := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.target, bytes.NewBufferString(tt.body))

			handler.ServeHTTP(response, request)

			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
		})
	}
}

func newTestStopListHandler(t *testing.T, storage StopListStorage) *StopListHandler {
	t.Helper()

	handler, err := NewStopListHandler(storage)
	if err != nil {
		t.Fatalf("NewStopListHandler() error = %v", err)
	}

	return handler
}
