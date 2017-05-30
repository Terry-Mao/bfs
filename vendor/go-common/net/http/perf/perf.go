package perf

import (
	xhttp "net/http"
	"net/http/pprof"

	"go-common/conf"
	"go-common/log"
	"go-common/net/http"
)

// Init start http pprof.
func Init(addr string) {
	mux := xhttp.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	// init serve
	c := &conf.HTTPServer{
		Addrs: []string{addr},
	}
	if err := http.Serve(mux, c); err != nil {
		log.Error("httpx.Serve error(%v)", err)
		panic(err)
	}
}
