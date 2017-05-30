package rpc

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	"go-common/conf"
	"go-common/log"
	"go-common/stat"

	"golang.org/x/sync/errgroup"
)

const (
	_clientsPool = 3
)

// balancer interface.
type balancer interface {
	Boardcast(context.Context, string, interface{}, interface{}) error
	Call(context.Context, string, interface{}, interface{}) error
	SetMethodTimeout(method string, timeout time.Duration)
	SetTimeout(timeout time.Duration)
}

// wrr get avaliable rpc client by wrr strategy.
type wrr struct {
	pool   []*Client
	weight int64
	server int64
	idx    int64
}

// Boardcast broad cast to all Client.
// NOTE: reply must be ptr.
func (r *wrr) Boardcast(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	if r.weight == 0 {
		log.Error("wrr get() error weight:%d server:%d idx:%d", len(r.pool), r.server, r.idx)
		return ErrNoClient
	}
	rtp := reflect.TypeOf(reply).Elem()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < int(r.server); i++ {
		j := i
		g.Go(func() error {
			nrp := reflect.New(rtp).Interface()
			return r.pool[j].Call(ctx, serviceMethod, args, nrp)
		})
	}
	return g.Wait()
}

func (r *wrr) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var (
		i int64
		v = atomic.AddInt64(&r.idx, 1)
	)
	if r.weight == 0 {
		log.Error("wrr get() error weight:%d server:%d idx:%d", len(r.pool), r.server, r.idx)
		return ErrNoClient
	}
	for i = 0; i < r.server; i++ {
		if err = r.pool[int((v+i)%r.weight)].Call(ctx, serviceMethod, args, reply); err != ErrNoClient {
			return
		}
	}
	return ErrNoClient
}

func (r *wrr) SetMethodTimeout(method string, timeout time.Duration) {
	for i := 0; i < int(r.server); i++ {
		r.pool[i].SetMethodTimeout(method, timeout)
	}
}

func (r *wrr) SetTimeout(timeout time.Duration) {
	for i := 0; i < int(r.server); i++ {
		r.pool[i].SetTimeout(timeout)
	}
}

type key interface {
	Key() int64
}

type sharding struct {
	pool   []*Client
	weight int64
	server int64
	idx    int64
}

// Boardcast broad cast to all clients.
// NOTE: reply must be ptr.
func (r *sharding) Boardcast(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	if r.weight == 0 {
		log.Error("wrr get() error weight:%d server:%d idx:%d", len(r.pool), r.server, r.idx)
		return ErrNoClient
	}
	rtp := reflect.TypeOf(reply).Elem()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < int(r.server); i++ {
		j := i
		g.Go(func() error {
			nrp := reflect.New(rtp).Interface()
			return r.pool[j].Call(ctx, serviceMethod, args, nrp)
		})
	}
	return g.Wait()
}

func (r *sharding) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var (
		ok bool
		k  key
	)
	if r.weight == 0 {
		log.Error("wrr get() error weight:%d server:%d idx:%d", len(r.pool), r.server, r.idx)
		return ErrNoClient
	}
	if k, ok = args.(key); ok {
		if err = r.pool[int(k.Key()%r.server)].Call(ctx, serviceMethod, args, reply); err != ErrNoClient {
			return
		}
	}
	return ErrNoClient
}

func (r *sharding) SetMethodTimeout(method string, timeout time.Duration) {
	for i := 0; i < int(r.server); i++ {
		r.pool[i].SetMethodTimeout(method, timeout)
	}
}

func (r *sharding) SetTimeout(timeout time.Duration) {
	for i := 0; i < int(r.server); i++ {
		r.pool[i].SetTimeout(timeout)
	}
}

// clients for rr client pool.
type clients struct {
	pool   []*Client
	size   int64
	idx    int64
	server int
}

// newClients connects to RPC servers at the specified network address.
func newClients(conf *conf.RPCClients, stats stat.Stat) *clients {
	clients := new(clients)
	for i := 0; i < _clientsPool; i++ {
		for _, c := range *conf {
			cli := Dial(c)
			cli.Stats = stats
			clients.pool = append(clients.pool, cli)
		}
	}
	clients.size = int64(len(clients.pool))
	clients.idx = 0
	clients.server = len(*conf)
	return clients
}

func (c *clients) Boardcast(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	rtp := reflect.TypeOf(reply).Elem()
	if c.server == 1 {
		return c.pool[0].Call(ctx, serviceMethod, args, reflect.New(rtp).Interface())
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < c.server; i++ {
		g.Go(func() error {
			j := i
			nrp := reflect.New(rtp).Interface()
			return c.pool[j].Call(ctx, serviceMethod, args, nrp)
		})
	}
	return g.Wait()
}

// Call invokes the named function, waits for it to complete, and returns its error status.
// this include rpc.Client.Call method, and takes a timeout.
func (c *clients) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var (
		i int64
		v = atomic.AddInt64(&c.idx, 1)
	)
	// retry all client
	for i = 0; i < c.size; i++ {
		if err = c.pool[int((v+i)%c.size)].Call(ctx, serviceMethod, args, reply); err != ErrNoClient && err != ErrShutdown {
			return
		}
	}
	return ErrNoClient
}

// SetMethodTimeout set rpc client timeout.
func (c *clients) SetMethodTimeout(method string, timeout time.Duration) {
	for _, cli := range c.pool {
		cli.SetMethodTimeout(method, timeout)
	}
}

// SetTimeout set rpc client timeout.
func (c *clients) SetTimeout(timeout time.Duration) {
	for _, cli := range c.pool {
		cli.SetTimeout(timeout)
	}
}
