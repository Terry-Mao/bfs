package rpc

import (
	"encoding/json"
	"path"
	"strings"
	"time"

	"go-common/conf"
	"go-common/log"

	izk "github.com/samuel/go-zookeeper/zk"
)

const (
	_evAddServer = 0
	_evDelServer = 1
)

type event struct {
	ev int
	c  *conf.RPCServer
}

type zk struct {
	cli   *izk.Conn
	conf  *conf.Zookeeper
	ev    chan event
	nodes map[*conf.RPCServer]string
}

func newZKByServer(c *conf.Zookeeper, server int) (z *zk) {
	var err error
	z = &zk{
		conf:  c,
		ev:    make(chan event, server*2),
		nodes: make(map[*conf.RPCServer]string, server),
	} // add & del server, double it
	if err = z.connect(); err != nil {
		go z.reconnect()
	}
	go z.serverproc()
	return
}

func newZKByClient(c *conf.Zookeeper) (z *zk) {
	var err error
	if c.Root != "/" {
		c.Root = strings.TrimRight(c.Root, "/")
	}
	z = &zk{
		conf: c,
	} // add & del server, double it
	if err = z.connect(); err != nil {
		go z.reconnect()
	}
	return
}

func (z *zk) connect() (err error) {
	var ev <-chan izk.Event
	if z.cli, ev, err = izk.Connect(z.conf.Addrs, time.Duration(z.conf.Timeout)); err == nil {
		go z.eventproc(ev)
	} else {
		log.Error("zk.Connect(%v) error(%v)", z.conf.Addrs, err)
	}
	return
}

func (z *zk) reconnect() {
	var err error
	for err = z.connect(); err != nil; err = z.connect() {
		time.Sleep(time.Second)
	}
}

func (z *zk) eventproc(s <-chan izk.Event) {
	var (
		ok bool
		e  izk.Event
	)
	for {
		if e, ok = <-s; !ok {
			return
		}
		log.Info("zookeeper get a event: %s", e.State.String())
	}
}

func (z *zk) addServer(c *conf.RPCServer) {
	z.ev <- event{ev: _evAddServer, c: c}
}

func (z *zk) delServer(c *conf.RPCServer) {
	z.ev <- event{ev: _evDelServer, c: c}
}

func (z *zk) serverproc() {
	var (
		ok   bool
		ev   event
		bs   []byte
		node string
		err  error
	)
	for {
		if ev, ok = <-z.ev; !ok {
			return
		}
		for {
			if z.cli == nil {
				time.Sleep(time.Second)
				continue
			}
			if ev.ev == _evAddServer {
				// add node
				if bs, err = json.Marshal(ev.c); err != nil {
					log.Error("json.Marshal(%v) error(%v)", ev.c, err)
					break
				}
				if node, err = z.cli.Create(z.conf.Root, bs,
					izk.FlagEphemeral|izk.FlagSequence, izk.WorldACL(izk.PermAll)); err != nil {
					log.Error("zk.create(%s) error(%v)", z.conf.Root, err)
					time.Sleep(time.Second)
					continue
				}
				z.nodes[ev.c] = node
			} else if ev.ev == _evDelServer {
				// delete node
				if node, ok = z.nodes[ev.c]; ok {
					if err = z.cli.Delete(node, -1); err != nil {
						if err != izk.ErrNoNode {
							time.Sleep(time.Second)
							continue
						}
					}
					delete(z.nodes, ev.c)
				}
			}
			break
		}
	}
}

func (z *zk) servers(group string) (svrs []*conf.RPCServer, ev <-chan izk.Event, err error) {
	var (
		addr  string
		node  string
		addrs []string
		bs    []byte
		svr   *conf.RPCServer
	)
	if z.cli == nil {
		return nil, nil, nil
	}
	if addrs, _, ev, err = z.cli.ChildrenW(z.conf.Root); err != nil {
		log.Error("z.c.ChildrenW(%s) error(%v)", z.conf.Root, err)
		return
	} else if len(addrs) == 0 {
		log.Warn("server(%s) have not node in zk", z.conf.Root)
		return
	}
	svrs = make([]*conf.RPCServer, 0, len(addrs))
	for _, addr = range addrs {
		node = path.Join(z.conf.Root, addr)
		if bs, _, err = z.cli.Get(node); err != nil {
			log.Error("z.c.Get(%s) error(%v)", node, err)
			return
		}
		svr = new(conf.RPCServer)
		if err = json.Unmarshal(bs, svr); err != nil {
			log.Error("json.Unmarshal(%s) node(%s) error(%v)", bs, node, err)
			return
		}
		if svr.Weight > 0 && (group == "" || svr.Group == "" || group == svr.Group) {
			svrs = append(svrs, svr)
			log.Info("syncproc svr: addr:%s, weight:%d, group: %s", svr.Addr, svr.Weight, svr.Group)
		}
	}
	return
}
