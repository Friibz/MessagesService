package handlers

import (
	"ServiceV2/models"
	"ServiceV2/services"
	"encoding/json"
	"net/http"
)

type Handler struct {
	Service *services.Service
}

func NewHandler(svc *services.Service) *Handler {
	return &Handler{
		Service: svc,
	}
}

func (h *Handler) MessagesHandler(w http.ResponseWriter, r *http.Request) {
	var messages []models.Messages
	err := json.NewDecoder(r.Body).Decode(&messages)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	err = h.Service.ProcessRequest(messages)
	if err != nil {
		http.Error(w, "Failed to process request", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("Request processed successfully"))
}

func (h *Handler) StatisticsHandler(w http.ResponseWriter, r *http.Request) {
	res, err := h.Service.GetStats()
	if err != nil {
		http.Error(w, "Failed to retrieve statistics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(map[string]int{"processed_messages": res})
	if err != nil {
		return
	}
}
