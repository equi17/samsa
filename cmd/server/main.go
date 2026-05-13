package main

import (
	"log"
	"net/http"

	"github.com/equi17/samsa/internal/api"
	"github.com/equi17/samsa/internal/broker"
)

func main() {
	b := broker.NewBroker()

	handler := api.NewHandler(b)

	http.HandleFunc("/publish", handler.Publish)
	http.HandleFunc("/consume", handler.Consume)
	http.HandleFunc("/subscribe", handler.Subscribe)

	log.Println("server started on :8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}