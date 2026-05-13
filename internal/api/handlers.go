package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/equi17/samsa/internal/broker"
)

type Handler struct {
	broker *broker.Broker
}

func NewHandler(b *broker.Broker) *Handler {
	return &Handler{
		broker: b,
	}
}

type PublishRequest struct {
	Topic string `json:"topic"`
	Value string `json:"value"`
}

func (h *Handler) Publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PublishRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	h.broker.Publish(req.Topic, req.Value)

	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Consume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	offsetStr := r.URL.Query().Get("offset")

	offset := 0

	if offsetStr != "" {
		var err error

		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
	}

	messages := h.broker.Consume(topic, offset)

	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(messages)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *Handler) Subscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	subscriberID, ch := h.broker.Subscribe(topic)
	defer h.broker.Unsubscribe(topic, subscriberID)

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			
			_, err := w.Write([]byte(msg.Value + "\n"))
			if err != nil {
				return
			}

			flusher.Flush()

		case <-ctx.Done():
			return
		}
	}
}