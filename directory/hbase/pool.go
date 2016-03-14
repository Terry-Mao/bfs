package hbase

import (
	"bfs/directory/hbase/hbasethrift"
	"container/list"
	"errors"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from a pool connection Get method when the
// maximum number of database connections in the pool has been reached.
var ErrPoolExhausted = errors.New("pool: connection pool exhausted")

// ErrPoolClosed is returned from a pool connection Get method when the
// pool closed.
var ErrPoolClosed = errors.New("pool: get on closed pool")

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a global variable.
//
//  func newPool() *pool.Pool {
//      return &pool.Pool{
//          MaxIdle: 3,
//          IdleTimeout: 240 * time.Second,
//          Dial: func () (redis.Conn, error) {
//              c, err := redis.Dial("tcp", server)
//              if err != nil {
//                  return nil, err
//              }
//              return c, err
//          },
//          TestOnBorrow: func(c redis.Conn, t time.Time) error {
//              _, err := c.Do("PING")
//              return err
//          },
//      }
//  }
//
//  var (
//      p *pool.Pool
//  )
//
//  func main() {
//      flag.Parse()
//      p = newPool()
//      ...
//  }
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  func serveHome(w http.ResponseWriter, r *http.Request) {
//      conn, err := p.Get()
//      if err != nil {
//          defer p.Put(conn)
//      }
//      ....
//  }
//
// thrift exmples.
//  p = &pool.Pool{
//    Dail: func() (interface{}, error) {
//        sock, err := thrift.NewTSocketTimeout(":1011", 15*time.Second)
//        if err != nil {
//            return nil, err
//        }
//        tF := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
//        pF := thrift.NewTBinaryProtocolFactoryDefault()
//        client := testRpc.NewRpcServiceClientFactory(tF.GetTransport(sock), pF)
//        client.Transport.Open()
//        return client, nil
//    },
//    Close: func(c interface{}) error {
//        return c.(*testRpc.RpcServiceClient).Transport.Close()
//    },
//    MaxActive: 2,
//    MaxIdle:   3,
//    //IdleTimeout: 1 * time.Second,
//
type Pool struct {

	// Dial is an application supplied function for creating new connections.
	Dial func() (*hbasethrift.THBaseServiceClient, error)

	// Close is an application supplied functoin for closeing connections.
	Close func(c *hbasethrift.THBaseServiceClient) error

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c *hbasethrift.THBaseServiceClient, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// mu protects fields defined below.
	mu     sync.Mutex
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c *hbasethrift.THBaseServiceClient
	t time.Time
}

// New creates a new pool. This function is deprecated. Applications should
// initialize the Pool fields directly as shown in example.
func New(dialFn func() (*hbasethrift.THBaseServiceClient, error), closeFn func(c *hbasethrift.THBaseServiceClient) error, maxIdle int) *Pool {
	return &Pool{Dial: dialFn, Close: closeFn, MaxIdle: maxIdle}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection.
func (p *Pool) Get() (*hbasethrift.THBaseServiceClient, error) {
	p.mu.Lock()
	// if closed
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.active -= 1
			p.mu.Unlock()
			// ic.c.Close()
			p.Close(ic.c)
			p.mu.Lock()
		}
	}
	// Get idle connection.
	for i, n := 0, p.idle.Len(); i < n; i++ {
		e := p.idle.Front()
		if e == nil {
			break
		}
		ic := e.Value.(idleConn)
		p.idle.Remove(e)
		test := p.TestOnBorrow
		p.mu.Unlock()
		if test == nil || test(ic.c, ic.t) == nil {
			return ic.c, nil
		}
		// ic.c.Close()
		p.Close(ic.c)
		p.mu.Lock()
		p.active -= 1
	}
	if p.MaxActive > 0 && p.active >= p.MaxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}
	// No idle connection, create new.
	dial := p.Dial
	p.active += 1
	p.mu.Unlock()
	c, err := dial()
	if err != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		c = nil
	}
	return c, err
}

// Put adds conn back to the pool, use forceClose to close the connection forcely
func (p *Pool) Put(c *hbasethrift.THBaseServiceClient, forceClose bool) error {
	if !forceClose {
		p.mu.Lock()
		if !p.closed {
			p.idle.PushFront(idleConn{t: nowFunc(), c: c})
			if p.idle.Len() > p.MaxIdle {
				// remove exceed conn
				c = p.idle.Remove(p.idle.Back()).(idleConn).c
			} else {
				c = nil
			}
		}
		p.mu.Unlock()
	}
	// close exceed conn
	if c != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		return p.Close(c)
	}
	return nil
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Relaase releases the resources used by the pool.
func (p *Pool) Release() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		p.Close(e.Value.(idleConn).c)
	}
	return nil
}
