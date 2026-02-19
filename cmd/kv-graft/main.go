package main

import (
	"net/http"

	"github.com/eika123/kv-graft/internal/server"
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

func (sm *StateMachine) handleGet(w http.ResponseWriter, r *http.Request) {

	w.Write([]byte("hello from get"))
}

func (sm *StateMachine) handlePut(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello from put"))
}

func handlerFactory(appState *StateMachine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", appState.handleGet)
	mux.HandleFunc("/put", appState.handlePut)
	return mux
}

func main() {

	appState := &StateMachine{
		kvmap: stateFactory(),
	}

	handler := handlerFactory(appState)

	kvserver := server.NewServer(8080, handler, stateFactory)
	if err := kvserver.Start(); err != nil {
		panic(err)
	}
}
