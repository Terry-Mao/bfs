package rpc

import (
	"net"
	"strings"

	"go-common/conf"
	"go-common/log"
	xip "go-common/net/ip"
)

// NewServer new a rpc server.
func NewServer(c *conf.RPCServer) *Server {
	s := newServer()
	go rpcListen(c, s)
	return s
}

// Serve rpc server.
func Serve(c *conf.RPCServers) *Server {
	s := newServer()
	for _, rpcServer := range *c {
		go rpcListen(rpcServer, s)
	}
	return s
}

// rpcListen start rpc listen.
func rpcListen(c *conf.RPCServer, s *Server) {
	l, err := net.Listen(c.Proto, c.Addr)
	if err != nil {
		log.Error("net.Listen(rpcAddr:(%v)) error(%v)", c.Addr, err)
		panic(err)
	}
	// if process exit, then close the rpc bind
	defer func() {
		log.Info("rpc addr:(%s) close", c.Addr)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()
	log.Info("start rpc listen addr: %s", c.Addr)
	s.Accept(l)
}

// Server2 rpc server2.
type Server2 struct {
	*Server
	c   *conf.RPCServer2
	zk  *zk
	lis map[net.Listener]struct{}
}

// NewServer2 new a rpc server2.
func NewServer2(c *conf.RPCServer2) *Server2 {
	s := new(Server2)
	s.c = c
	s.lis = map[net.Listener]struct{}{}
	s.Server = newServer()
	if !c.DiscoverOff && c.Zookeeper != nil {
		s.zk = newZKByServer(c.Zookeeper, len(c.Servers))
	}
	for _, rpcServer := range c.Servers {
		if l := s.listen(rpcServer); l != nil {
			go s.listenproc(rpcServer, l)
		}
	}
	return s
}

// Close stop the rpc server.
func (s *Server2) Close() error {
	for l := range s.lis {
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
		delete(s.lis, l)
	}
	return nil
}

func (s *Server2) listen(c *conf.RPCServer) net.Listener {
	l, err := net.Listen(c.Proto, c.Addr)
	if err != nil {
		log.Error("net.Listen(rpcAddr:(%v)) error(%v)", c.Addr, err)
		panic(err)
	}
	log.Info("start rpc listen addr: %s", c.Addr)
	ipPort := strings.Split(c.Addr, ":")
	if len(ipPort) != 2 {
		log.Error("illegal addr: %s", c.Addr)
		return nil
	}
	if ipPort[0] == "0.0.0.0" || ipPort[0] == "" {
		c.Addr = xip.InternalIP() + ":" + ipPort[1]
	}
	s.lis[l] = struct{}{}
	return l
}

func (s *Server2) listenproc(c *conf.RPCServer, l net.Listener) {
	if s.zk != nil {
		s.zk.addServer(c)
		defer s.zk.delServer(c)
	}
	s.Accept(l)
}
