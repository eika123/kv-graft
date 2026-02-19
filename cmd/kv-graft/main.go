package main

import (
	"fmt"
	"net/http"

	"github.com/eika123/kv-graft/internal/server"

	"encoding/json"
)

func stateFactory() map[string]string {
	return make(map[string]string)
}

type StateMachine struct {
	kvmap map[string]string
}

type StateMachineInterface interface {
	applyStateChange(key, value string)
	getState(key string) (string, bool)
	getTotalState() StateMachine
}

func helloWorldHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello, World!")
}

func (sm *StateMachine) handleGetByKey(w http.ResponseWriter, r *http.Request) {
	type ResponseGetByKey struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	key := r.URL.Query().Get("key")
	value, exists := sm.kvmap[key]
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	response := ResponseGetByKey{
		Key:   key,
		Value: value,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (sm *StateMachine) handlePutByKey(w http.ResponseWriter, r *http.Request) {
	fmt.Println("PUT request received")
	type RequestPut struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	var req RequestPut
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	sm.kvmap[req.Key] = req.Value
	w.WriteHeader(http.StatusOK)
}

func handlerFactory(appState *StateMachine) http.Handler {

	mux := http.NewServeMux()
	mux.HandleFunc("GET /items", appState.handleGetByKey)
	mux.HandleFunc("PUT /items", appState.handlePutByKey)
	mux.HandleFunc("/hello", helloWorldHandler)
	return mux
}

func main() {

	appState := &StateMachine{
		kvmap: stateFactory(),
	}

	handler := handlerFactory(appState)

	kvserver := server.NewServer(8080, handler)
	fmt.Println("Starting server on  http://localhost:8080")
	if err := kvserver.Start(); err != nil {
		panic(err)
	}
}
