package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"test-project-rwb/internal/stoplist"
)

const stopListPath = "/stop-list"

var ErrNilStopList = errors.New("stop-list storage is required")

type StopListStorage interface {
	Add(query string) (string, error)
	Remove(query string) (string, error)
	List() []string
}

type StopListHandler struct {
	storage StopListStorage
}

type stopListRequest struct {
	Query string `json:"query"`
}

type stopListItemResponse struct {
	Query string `json:"query"`
}

type stopListResponse struct {
	Items []string `json:"items"`
	Count int      `json:"count"`
}

// NewStopListHandler создает HTTP handler для управления стоп-листом
func NewStopListHandler(storage StopListStorage) (*StopListHandler, error) {
	if storage == nil {
		return nil, ErrNilStopList
	}

	return &StopListHandler{storage: storage}, nil
}

func (h *StopListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == stopListPath:
		h.handleCollection(w, r)
	case strings.HasPrefix(r.URL.Path, stopListPath+"/"):
		h.handleItem(w, r)
	default:
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "not found"})
	}
}

func (h *StopListHandler) handleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items := h.storage.List()
		writeJSON(w, http.StatusOK, stopListResponse{
			Items: items,
			Count: len(items),
		})
	case http.MethodPost:
		h.handleAdd(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
	}
}

func (h *StopListHandler) handleAdd(w http.ResponseWriter, r *http.Request) {
	var request stopListRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid json body"})
		return
	}

	query, err := h.storage.Add(request.Query)
	if err != nil {
		if errors.Is(err, stoplist.ErrEmptyQuery) {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "query is required"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to update stop-list"})
		return
	}

	writeJSON(w, http.StatusCreated, stopListItemResponse{Query: query})
}

func (h *StopListHandler) handleItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	rawQuery := strings.TrimPrefix(r.URL.Path, stopListPath+"/")
	query, err := url.PathUnescape(rawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid query path"})
		return
	}

	if _, err := h.storage.Remove(query); err != nil {
		if errors.Is(err, stoplist.ErrEmptyQuery) {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "query is required"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to update stop-list"})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
