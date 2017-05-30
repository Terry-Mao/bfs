package http

import (
	ctx "context"
	"net/http"

	"go-common/net/http/context"
	"go-common/net/trace"
)

// Router web http pattern router.
type Router struct {
	family  string
	address string
	mux     *http.ServeMux
	pattern string
}

// NewRouter new a router.
func NewRouter(family, address string, mux *http.ServeMux) *Router {
	return &Router{family: family, address: address, mux: mux}
}

func (r *Router) join(pattern string) string {
	return r.pattern + pattern
}

// Group return a group router.
func (r *Router) Group(pattern string) *Router {
	return &Router{mux: r.mux, pattern: r.join(pattern), family: r.family, address: r.address}
}

// Handle is an adapter which allows the usage of an http.Handler as a request handle.
func (r *Router) Handle(method, pattern string, handlers ...Handler) {
	r.mux.HandleFunc(r.join(pattern), func(w http.ResponseWriter, req *http.Request) {
		handler(method, r.family, r.address, w, req, handlers)
	})
	return
}

// HandlerFunc The HandlerFunc type is an adapter to allow the use of ordinary
// functions as HTTP handlers. If f is a function with the appropriate signature,
// HandlerFunc(f) is a Handler that calls f.
func (r *Router) HandlerFunc(method, pattern string, handlers ...HandlerFunc) {
	r.mux.HandleFunc(r.join(pattern), func(w http.ResponseWriter, req *http.Request) {
		handleFunc(method, r.family, r.address, w, req, handlers)
	})
	return
}

// Get is a shortcut for router.Handle("GET", path, handle).
func (r *Router) Get(pattern string, handlers ...Handler) {
	r.mux.HandleFunc(r.join(pattern), func(w http.ResponseWriter, req *http.Request) {
		handler("GET", r.family, r.address, w, req, handlers)
	})
	return
}

// Post is a shortcut for router.Handle("POST', path, handle).
func (r *Router) Post(pattern string, handlers ...Handler) {
	r.mux.HandleFunc(r.join(pattern), func(w http.ResponseWriter, req *http.Request) {
		handler("POST", r.family, r.address, w, req, handlers)
	})
	return
}

// GetFunc is a shortcut for router.HandleFunc("GET", path, handle)
func (r *Router) GetFunc(pattern string, handlers ...HandlerFunc) {
	r.mux.HandleFunc(r.join(pattern), func(w http.ResponseWriter, req *http.Request) {
		handleFunc("GET", r.family, r.address, w, req, handlers)
	})
	return
}

// PostFunc is a shortcut for router.HandleFunc("GET", path, handle)
func (r *Router) PostFunc(pattern string, handlers ...HandlerFunc) {
	r.mux.HandleFunc(r.join(pattern), func(w http.ResponseWriter, req *http.Request) {
		handleFunc("POST", r.family, r.address, w, req, handlers)
	})
	return
}

func handler(method, family, address string, w http.ResponseWriter, r *http.Request, handlers []Handler) {
	u, t := trace.FromHTTP(r, family, r.Host+r.URL.Path, address)
	t.Server(method)
	defer t.Finish()
	if r.Method != method {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	c := context.NewContext(trace.NewContext2(ctx.Background(), t), u, r, w)
	defer c.Cancel()
	for _, h := range handlers {
		h.ServeHTTP(c)
		if err := c.Err(); err != nil {
			return
		}
	}
}

func handleFunc(method, family, address string, w http.ResponseWriter, r *http.Request, handlers []HandlerFunc) {
	u, t := trace.FromHTTP(r, family, r.Host+r.URL.Path, address)
	t.Server(method)
	defer t.Finish()
	if r.Method != method {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	c := context.NewContext(trace.NewContext2(ctx.Background(), t), u, r, w)
	defer c.Cancel()
	for _, h := range handlers {
		h(c)
		if err := c.Err(); err != nil {
			return
		}
	}
}
