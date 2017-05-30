package http

import (
	"net"
	xhttp "net/http"
	"runtime"
	"time"

	"go-common/conf"
	"go-common/log"
	"go-common/net/netutil"
)

// Serve listen and serve http handlers with limit count.
func Serve(mux *xhttp.ServeMux, c *conf.HTTPServer) (err error) {
	for _, addr := range c.Addrs {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("net.Listen(\"tcp\", \"%s\") error(%v)", addr, err)
			return err
		}
		if c.MaxListen > 0 {
			l = netutil.LimitListener(l, c.MaxListen)
		}
		log.Info("start http listen addr: %s", addr)
		for i := 0; i < runtime.NumCPU(); i++ {
			go func() {
				server := &xhttp.Server{Handler: mux, ReadTimeout: time.Duration(c.ReadTimeout), WriteTimeout: time.Duration(c.WriteTimeout)}
				if err := server.Serve(l); err != nil {
					log.Info("server.Serve error(%v)", err)
				}
			}()
		}
	}
	return nil
}
