package server

import (
	"fmt"
	"net/http"
)

type Server struct {
	port        uint16
	HandlerFunc http.Handler
}

func NewServer(port uint16, HandlerFunc http.Handler) *Server {
	return &Server{
		port:        port,
		HandlerFunc: HandlerFunc,
	}
}

func (s *Server) Start() error {
	return http.ListenAndServe(":"+fmt.Sprintf("%d", s.port), s.HandlerFunc)
}
