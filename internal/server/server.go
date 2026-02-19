package server

import (
	"fmt"
	"net/http"
)

type Server[S any] struct {
	port         uint16
	HandlerFunc  http.Handler
	stateMachine S
}

func NewServer[S any](port uint16, HandlerFunc http.Handler, stateConstructor func() S) *Server[S] {
	return &Server[S]{
		port:        port,
		HandlerFunc: HandlerFunc,
		state:       stateConstructor(),
	}
}

func (s *Server[S]) Start() error {
	return http.ListenAndServe(":"+fmt.Sprintf("%d", s.port), s.HandlerFunc)
}
