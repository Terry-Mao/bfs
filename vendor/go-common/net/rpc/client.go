// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpc

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go-common/conf"
	xlog "go-common/log"
	"go-common/net/netutil"
	"go-common/net/trace"
	"go-common/stat"
	xtime "go-common/time"
)

const (
	_family       = "rpc_client"
	_pingDuration = time.Second * 1
)

var (
	// ErrShutdown shutdown error.
	ErrShutdown = errors.New("connection is shut down")
	// ErrTimeout is returned when calling rpc faild.
	ErrTimeout = errors.New("rpc call timeout")
	// ErrBreaker error of breaker not allowed
	ErrBreaker = errors.New("breaker not allowed")
	// ErrNoClient current no rpc client.
	ErrNoClient = errors.New("no rpc client")

	errClient = new(client)
)

// ServerError represents an error that has been returned from
// the remote side of the RPC connection.
type ServerError string

func (e ServerError) Error() string {
	return string(e)
}

// Call represents an active RPC.
type Call struct {
	ServiceMethod string      // The name of the service and method to call.
	Args          interface{} // The argument to the function (*struct).
	Reply         interface{} // The reply from the function (*struct).
	Trace         *trace.Trace2
	Error         error      // After completion, the error status.
	Done          chan *Call // Strobes when call is complete.
}

// client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type client struct {
	codec *clientCodec

	reqMutex sync.Mutex // protects following
	request  Request

	mutex    sync.Mutex // protects following
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop

	timeout    time.Duration // call timeout
	remoteAddr string        // server address
}

func (client *client) send(call *Call) {
	client.reqMutex.Lock()
	defer client.reqMutex.Unlock()

	// Register this call.
	client.mutex.Lock()
	if client.shutdown || client.closing {
		call.Error = ErrShutdown
		client.mutex.Unlock()
		call.done()
		return
	}
	seq := client.seq
	client.seq++
	client.pending[seq] = call
	client.mutex.Unlock()

	// Encode and send the request.
	client.request.Seq = seq
	client.request.ServiceMethod = call.ServiceMethod
	client.request.Trace = call.Trace
	err := client.codec.WriteRequest(&client.request, call.Args)
	if err != nil {
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *client) input() {
	var err error
	var response Response
	for err == nil {
		response = Response{}
		err = client.codec.ReadResponseHeader(&response)
		if err != nil {
			break
		}
		seq := response.Seq
		client.mutex.Lock()
		call := client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()

		switch {
		case call == nil:
			// We've got no pending call. That usually means that
			// WriteRequest partially failed, and call was already
			// removed; response is a server telling us about an
			// error reading request body. We should still attempt
			// to read error body, but there's no one to give it to.
			err = client.codec.ReadResponseBody(nil)
			if err != nil {
				err = errors.New("reading error body: " + err.Error())
			}
		case response.Error != "":
			// We've got an error response. Give this to the request;
			// any subsequent requests will get the ReadResponseBody
			// error if there is one.
			call.Error = ServerError(response.Error)
			err = client.codec.ReadResponseBody(nil)
			if err != nil {
				err = errors.New("reading error body: " + err.Error())
			}
			call.done()
		default:
			err = client.codec.ReadResponseBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// Terminate pending calls.
	client.reqMutex.Lock()
	client.mutex.Lock()
	client.shutdown = true
	closing := client.closing
	if err == io.EOF {
		if closing {
			err = ErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
	client.mutex.Unlock()
	client.reqMutex.Unlock()
	if err != io.EOF && !closing {
		log.Println("rpc: client protocol error:", err)
	}
}

func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		// We don't want to block here. It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		log.Println("rpc: discarding Call reply due to insufficient Done chan capacity")
	}
}

// Finish must called after Go.
func (call *Call) Finish() {
	if call.Trace != nil {
		call.Trace.Done(&call.Error)
	}
}

// newClient returns a new Client to handle requests to the
// set of services at the other end of the connection.
// It adds a buffer to the write side of the connection so
// the header and payload are sent as a unit.
func newClient(token string, timeout time.Duration, conn net.Conn) (*client, error) {
	encBuf := bufio.NewWriter(conn)
	client := &clientCodec{conn, gob.NewDecoder(conn), gob.NewEncoder(encBuf), encBuf}
	c := newClientWithCodec(client)
	c.timeout = timeout
	c.remoteAddr = conn.RemoteAddr().String()
	// handshake
	if token != "" {
		if err := c.Call(_authServiceMethod, &Auth{User: trace.Owner(), Token: token}, &struct{}{}); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

// newClientWithCodec is like newClient but uses the specified
// codec to encode requests and decode responses.
func newClientWithCodec(codec *clientCodec) *client {
	client := &client{
		codec:   codec,
		pending: make(map[uint64]*Call),
	}
	go client.input()
	return client
}

type clientCodec struct {
	rwc    io.ReadWriteCloser
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
}

func (c *clientCodec) WriteRequest(r *Request, body interface{}) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return c.encBuf.Flush()
}

func (c *clientCodec) ReadResponseHeader(r *Response) error {
	return c.dec.Decode(r)
}

func (c *clientCodec) ReadResponseBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *clientCodec) Close() error {
	return c.rwc.Close()
}

// dial connects to an RPC server at the specified network address.
func dial(network, addr, token string, timeout time.Duration) (*client, error) {
	// TODO dial timeout
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return newClient(token, timeout, conn)
}

// Close close the rpc client.
func (client *client) Close() error {
	client.mutex.Lock()
	if client.closing {
		client.mutex.Unlock()
		return ErrShutdown
	}
	client.closing = true
	client.mutex.Unlock()
	return client.codec.Close()
}

func (client *client) do(call *Call, done chan *Call) {
	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	client.send(call)
}

// Go invokes the function asynchronously. It returns the Call structure representing
// the invocation. The done channel will signal when the call is complete by returning
// the same Call object. If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
// Must call Finish() after call Go.
func (client *client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	client.do(call, done)
	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
func (client *client) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

// Do do a rpc call.
func (client *client) Do(call *Call) {
	client.do(call, make(chan *Call, 1))
}

type td struct {
	path    string
	timeout time.Duration
}

// Client wrapper is a client holder with implements pinger.
type Client struct {
	conf *conf.RPCClient
	quit chan struct{}

	client  atomic.Value
	breaker atomic.Value
	timeout atomic.Value

	bch   chan string
	tch   chan td
	Stats stat.Stat
}

// Dial connects to an RPC server at the specified network address.
func Dial(c *conf.RPCClient) *Client {
	client := &Client{
		conf: c,
		quit: make(chan struct{}),
	}
	// breaker
	if c.Breaker != nil {
		client.breaker.Store(make(map[string]*netutil.Breaker))
		client.bch = make(chan string, 10)
	}
	// timeout
	client.timeout.Store(make(map[string]time.Duration))
	client.tch = make(chan td, 10)
	// proc
	rc, err := dial(c.Proto, c.Addr, c.Token, time.Duration(c.Timeout))
	if err != nil {
		xlog.Error("dial(%s, %s) error(%v)", c.Proto, c.Addr, err)
	} else {
		client.client.Store(rc)
	}
	go client.evproc()
	go client.ping()
	return client
}

// Call invokes the named function, waits for it to complete, and returns its error status.
func (c *Client) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var (
		ok       bool
		rc       *client
		call     *Call
		cancel   func()
		t        *trace.Trace2
		timeout  time.Duration
		breaker  *netutil.Breaker
		breakers map[string]*netutil.Breaker
	)
	if rc, ok = c.client.Load().(*client); !ok || rc == errClient {
		return ErrNoClient
	}
	if t, ok = trace.FromContext2(ctx); ok {
		t = t.Fork(_family, serviceMethod, rc.remoteAddr)
		t.Client("")
		defer t.Done(&err)
	}
	// breaker
	if breakers, ok = c.breaker.Load().(map[string]*netutil.Breaker); ok {
		if breaker, ok = breakers[serviceMethod]; ok {
			if !breaker.Allow() {
				err = ErrBreaker
				return
			}
			defer c.onBreaker(breaker, &err)
		} else {
			select {
			case c.bch <- serviceMethod:
			default:
			}
		}
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing(serviceMethod, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	// timeout
	if timeout, ok = c.timeout.Load().(map[string]time.Duration)[serviceMethod]; !ok {
		timeout = time.Duration(c.conf.Timeout)
	}
	ctx, cancel = context.WithTimeout(ctx, timeout)
	// call
	call = &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Trace:         t,
	}
	rc.Do(call)
	select {
	case call = <-call.Done:
		err = call.Error
	case <-ctx.Done():
		err = ErrTimeout
	}
	cancel()
	return
}

// SetMethodTimeout set rpc client timeout.
func (c *Client) SetMethodTimeout(path string, d time.Duration) {
	c.tch <- td{path: path, timeout: d}
}

// SetTimeout set rpc client timeout.
func (c *Client) SetTimeout(d time.Duration) {
	if c.conf != nil {
		c.conf.Timeout = xtime.Duration(d)
	}
}

func (c *Client) onBreaker(breaker *netutil.Breaker, err *error) {
	if err != nil && *err != nil && (*err == ErrShutdown || *err == ErrTimeout || *err == io.ErrUnexpectedEOF) {
		breaker.Fail()
	} else {
		breaker.Success()
	}
}

func (c *Client) evproc() {
	for {
		select {
		case <-c.quit:
			return
		case v := <-c.bch:
			bo := c.breaker.Load().(map[string]*netutil.Breaker)
			if _, ok := bo[v]; ok {
				continue
			}
			bn := make(map[string]*netutil.Breaker, len(bo)+1)
			bn[v] = netutil.NewBreaker(c.conf.Breaker)
			if c.Stats != nil {
				h := func(state int32) {
					c.Stats.State("breaker "+v, int64(state))
				}
				bn[v].State = h
			}
			for k, v := range bo {
				bn[k] = v
			}
			c.breaker.Store(bn)
		case v := <-c.tch:
			to := c.timeout.Load().(map[string]time.Duration)
			tn := make(map[string]time.Duration, len(to)+1)
			tn[v.path] = v.timeout
			for k, v := range to {
				tn[k] = v
			}
			c.timeout.Store(tn)
		}
	}
}

// ping ping the rpc connect and re connect when has an error.
func (c *Client) ping() {
	var (
		err       error
		cancel    func()
		call      *Call
		ctx       context.Context
		client, _ = c.client.Load().(*client)
	)
	for {
		select {
		case <-c.quit:
			c.client.Store(errClient)
			if client != nil {
				client.Close()
			}
			return
		default:
		}
		if client == nil || err != nil {
			if client, err = dial(c.conf.Proto, c.conf.Addr, c.conf.Token, time.Duration(c.conf.Timeout)); err != nil {
				xlog.Error("dial(%s, %s) error(%v)", c.conf.Proto, c.conf.Addr, err)
				time.Sleep(_pingDuration)
				continue
			}
			c.client.Store(client)
		}
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(c.conf.Timeout))
		select {
		case call = <-client.Go(_pingServiceMethod, _pingArg, _pingArg, make(chan *Call, 1)).Done:
			err = call.Error
		case <-ctx.Done():
			err = ErrTimeout
		}
		cancel()
		if err != nil {
			xlog.Error("client.Call(ping, %s) error(%v)", c.conf.Addr, err)
			if err == ErrShutdown || err == ErrTimeout || err == io.ErrUnexpectedEOF {
				c.client.Store(errClient)
				client.Close()
			} else {
				err = nil // never touch here
			}
		}
		time.Sleep(_pingDuration)
	}
}

// Close close client connection.
func (c *Client) Close() {
	close(c.quit)
}
