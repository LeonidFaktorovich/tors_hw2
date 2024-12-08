package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type Handler struct {
	state               *State
	replication_trigger *ReplicationTrigger
}

func (h *Handler) GetHandler(w http.ResponseWriter, r *http.Request) {
	Get(h.state, w, r)
}

func (h *Handler) PostHandler(w http.ResponseWriter, r *http.Request) {
	Post(h.state, h.replication_trigger, w, r)
}

func (h *Handler) PutHandler(w http.ResponseWriter, r *http.Request) {
	Put(h.state, h.replication_trigger, w, r)
}

func (h *Handler) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	Delete(h.state, h.replication_trigger, w, r)
}

func (h *Handler) PatchHandler(w http.ResponseWriter, r *http.Request) {
	Patch(h.state, h.replication_trigger, w, r)
}

func HttpDaemon(state *State, replication_trigger *ReplicationTrigger, port int) {
	r := mux.NewRouter()

	h := Handler{state: state, replication_trigger: replication_trigger}

	r.HandleFunc("/read", h.GetHandler).Methods("GET")
	r.HandleFunc("/create", h.PostHandler).Methods("POST")
	r.HandleFunc("/update", h.PutHandler).Methods("PUT")
	r.HandleFunc("/delete", h.DeleteHandler).Methods("DELETE")
	r.HandleFunc("/update", h.PatchHandler).Methods("PATCH")

	addr := fmt.Sprintf(":%v", port)
	fmt.Printf("Start http server on %s\n", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatalf("Error in http server: %v", err)
	}
}
