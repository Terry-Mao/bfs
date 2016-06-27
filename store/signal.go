package main

import (
	log "github.com/golang/glog"
	"os"
	"os/signal"
	"syscall"
)

// StartSignal register signals handler.
func StartSignal(store *Store, server *Server) {
	var (
		c chan os.Signal
		s os.Signal
	)
	c = make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	// Block until a signal is received.
	for {
		s = <-c
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			server.Close()
			store.Close()
			return
		case syscall.SIGHUP:
			// TODO reload
			//return
		default:
			return
		}
	}
}
