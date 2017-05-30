package rpc

import (
	"context"
	"sync/atomic"
	"time"

	"go-common/conf"
	"go-common/log"
	"go-common/stat"

	izk "github.com/samuel/go-zookeeper/zk"
)

const _policySharding = "sharding"

// Client2 support for load balancing and service discovery.
type Client2 struct {
	conf     *conf.RPCClient2
	zk       *zk
	balancer atomic.Value
	backup   *clients
	Stats    stat.Stat
}

// NewClient2 new rpc client2.
func NewClient2(c *conf.RPCClient2) (c2 *Client2) {
	c2 = &Client2{
		zk:   newZKByClient(c.Zookeeper),
		conf: c,
	}
	if c.Backup != nil {
		c2.backup = newClients(c.Backup, c2.Stats)
	}
	go c2.syncproc()
	return
}

// Boardcast boardcast all rpc client.
func (c *Client2) Boardcast(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var (
		ok bool
		b  balancer
	)
	if b, ok = c.balancer.Load().(balancer); ok {
		if err = b.Boardcast(ctx, serviceMethod, args, reply); err != nil {
			return
		}
	}
	if c.backup != nil {
		return c.backup.Boardcast(ctx, serviceMethod, args, reply)
	}
	return nil
}

// Call invokes the named function, waits for it to complete, and returns its error status.
// this include rpc.Client.Call method, and takes a timeout.
func (c *Client2) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var (
		ok bool
		b  balancer
	)
	if b, ok = c.balancer.Load().(balancer); ok {
		if err = b.Call(ctx, serviceMethod, args, reply); err != ErrNoClient {
			return
		}
	}
	if c.backup != nil {
		return c.backup.Call(ctx, serviceMethod, args, reply)
	}
	return ErrNoClient
}

// SetMethodTimeout set rpc client timeout.
func (c *Client2) SetMethodTimeout(method string, timeout time.Duration) {
	var (
		ok bool
		b  balancer
	)
	if b, ok = c.balancer.Load().(balancer); ok {
		b.SetMethodTimeout(method, timeout)
	}
}

// SetTimeout set rpc client timeout.
func (c *Client2) SetTimeout(timeout time.Duration) {
	var (
		ok bool
		b  balancer
	)
	if b, ok = c.balancer.Load().(balancer); ok {
		b.SetTimeout(timeout)
	}
}

func (c *Client2) syncproc() {
	var (
		err       error
		ok        bool
		i, j, idx int
		weights   int
		key       string
		e         izk.Event
		ev        <-chan izk.Event
		blc       balancer
		cli       *Client
		clic      *conf.RPCClient
		cs, wcs   []*Client
		svr       *conf.RPCServer
		svrs      []*conf.RPCServer
		nodes     map[string]struct{}
		dcs       map[string]*Client
		pools     = make(map[string]*Client)
	)
	for {
		if svrs, ev, err = c.zk.servers(c.conf.Group); err != nil {
			time.Sleep(time.Second)
			continue
		} else if len(svrs) == 0 {
			log.Error("no rpc servers")
			c.removeAndClose(pools, pools)
			time.Sleep(time.Second)
			continue
		}
		nodes = make(map[string]struct{}, len(svrs))
		cs = make([]*Client, 0, len(svrs))
		dcs = make(map[string]*Client, len(pools))
		weights = 0
		// add new nodes
		for _, svr = range svrs {
			log.Info("rpc syncproc get svr: %v", svr)
			weights += svr.Weight // calc all weight
			key = svr.Key()
			nodes[key] = struct{}{}
			if cli, ok = pools[key]; !ok {
				clic = new(conf.RPCClient)
				*clic = *c.conf.Client
				clic.Proto = svr.Proto
				clic.Addr = svr.Addr
				cli = Dial(clic)
				cli.Stats = c.Stats
				pools[key] = cli
			}
			cs = append(cs, cli)
		}
		// delete old nodes
		for key, cli = range pools {
			if _, ok = nodes[key]; !ok {
				log.Info("syncproc will delete node: %s", key)
				dcs[key] = cli
			}
		}
		// new client slice by weights
		wcs = make([]*Client, 0, weights)
		for i, j = 0, 0; i < weights; j++ { // j++ means next svr
			idx = j % len(svrs)
			if svr = svrs[idx]; svr.Weight > 0 {
				i++ // i++ means all weights must fill wrrClis
				svr.Weight--
				wcs = append(wcs, cs[idx])
			}
		}
		switch c.conf.Policy {
		case _policySharding:
			blc = &sharding{
				pool:   wcs,
				weight: int64(weights),
				server: int64(len(svrs)),
			}
			log.Info("syncproc sharding weights:%d size:%d raw:%d", weights, weights, len(svrs))
		default:
			blc = &wrr{
				pool:   wcs,
				weight: int64(weights),
				server: int64(len(svrs)),
			}
			log.Info("syncproc wrr weights:%d size:%d raw:%d", weights, weights, len(svrs))
		}
		c.balancer.Store(blc)
		c.removeAndClose(pools, dcs)
		// wait zk event or next loop
		select {
		// TODO zk reconnect
		case e = <-ev:
			log.Info("zk.servers() changed, %v", e)
		case <-time.After(time.Duration(c.conf.PullInterval)):
		}
	}
}

func (c *Client2) removeAndClose(pools, dcs map[string]*Client) {
	if len(dcs) == 0 {
		return
	}
	// after rpc timeout(double duration), close no used clients
	time.Sleep(2 * time.Duration(c.conf.Client.Timeout))
	for key, cli := range dcs {
		delete(pools, key)
		cli.Close()
	}
}
