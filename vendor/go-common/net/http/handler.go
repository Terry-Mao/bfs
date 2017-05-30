package http

import (
	"go-common/net/http/context"
)

// Handler http request handler.
type Handler interface {
	ServeHTTP(context.Context)
}

// HandlerFunc http request handler function.
type HandlerFunc func(context.Context)

// ServeHTTP calls f(w, r).
func (f HandlerFunc) ServeHTTP(c context.Context) {
	f(c)
}
