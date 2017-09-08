// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	// log "golang/log4go"
	log "github.com/golang/glog"

	"bfs/libs/gohbase/hrpc"
	"bfs/libs/gohbase/pb"

	"github.com/golang/protobuf/proto"
)

// ClientType is a type alias to represent the type of this region client
type ClientType string

var (
	// ErrShortWrite is used when the writer thread only succeeds in writing
	// part of its buffer to the socket, and not all of the buffer was sent
	ErrShortWrite = errors.New("short write occurred while writing to socket")

	// ErrMissingCallID is used when HBase sends us a response message for a
	// request that we didn't send
	ErrMissingCallID = errors.New("HBase responded to a nonsensical call ID")

	// javaRetryableExceptions is a map where all Java exceptions that signify
	// the RPC should be sent again are listed (as keys). If a Java exception
	// listed here is returned by HBase, the client should attempt to resend
	// the RPC message, potentially via a different region client.
	javaRetryableExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.NotServingRegionException":         struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionMovedException":   struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionOpeningException": struct{}{},
	}
)

const (
	// RegionClient is a ClientType that means this will be a normal client
	RegionClient = ClientType("ClientService")

	// MasterClient is a ClientType that means this client will talk to the
	// master server
	MasterClient = ClientType("MasterService")
)

// UnrecoverableError is an error that this region.Client can't recover from.
// The connection to the RegionServer has to be closed and all queued and
// outstanding RPCs will be failed / retried.
type UnrecoverableError struct {
	error
}

func (e UnrecoverableError) Error() string {
	return e.error.Error()
}

// RetryableError is an error that indicates the RPC should be retried because
// the error is transient (e.g. a region being momentarily unavailable).
type RetryableError struct {
	error
}

func (e RetryableError) Error() string {
	return e.error.Error()
}

// Client manages a connection to a RegionServer.
type Client struct {
	id uint32

	conn net.Conn

	// Hostname or IP address of the RegionServer.
	host string

	// Port of the RegionServer.
	port uint16

	// writeMutex is used to prevent multiple threads from writing to the
	// socket at the same time.
	writeMutex *sync.Mutex

	// sendErr is set once a write fails.
	sendErr     error
	sendErrLock sync.Mutex

	rpcs []hrpc.Call

	// Once the rpcs list has grown to a large enough size, this channel is
	// written to to notify the writer thread that it should stop sleeping and
	// process the list
	process chan struct{}

	// sentRPCs contains the mapping of sent call IDs to RPC calls, so that when
	// a response is received it can be tied to the correct RPC
	sentRPCs      map[uint32]hrpc.Call
	sentRPCsMutex *sync.Mutex

	// <= 0 means only wait for timeout(can not be used combined with <=0 flushInterval); = 1 means flush for every request
	rpcQueueSize int
	// <= 0 means no flush-timeout
	flushInterval time.Duration
}

// NewClient creates a new RegionClient.
func NewClient(host string, port uint16, ctype ClientType,
	queueSize int, flushInterval, dialTimeout time.Duration) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	// Read/Write Timeout is not needed as actually no direct wait-on-io will happen.
	// Non-blocking RPC call is ensured by usage of Context
	var (
		conn net.Conn
		err  error
	)
	if int64(dialTimeout) > 0 {
		conn, err = net.DialTimeout("tcp", addr, dialTimeout)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil,
			fmt.Errorf("failed to connect to the RegionServer at %s: %s", addr, err)
	}
	c := &Client{
		conn:          conn,
		host:          host,
		port:          port,
		writeMutex:    &sync.Mutex{},
		process:       make(chan struct{}),
		sentRPCsMutex: &sync.Mutex{},
		sentRPCs:      make(map[uint32]hrpc.Call),
		rpcQueueSize:  queueSize,
		flushInterval: flushInterval,
	}
	err = c.sendHello(ctype)
	if err != nil {
		return nil, err
	}
	go c.processRpcs() // Writer goroutine
	go c.receiveRpcs() // Reader goroutine
	return c, nil
}

// Close asks this region.Client to close its connection to the RegionServer.
// All queued and outstanding RPCs, if any, will be failed as if a connection
// error had happened.
func (c *Client) Close() {
	c.setSendErr(errors.New("shutting down"))
	c.errorEncountered()
}

// Host returns the host that this client talks to
func (c *Client) Host() string {
	return c.host
}

// Port returns the port that this client talks over
func (c *Client) Port() uint16 {
	return c.port
}

func (c *Client) GetSendErr() error {
	c.sendErrLock.Lock()
	err := c.sendErr
	c.sendErrLock.Unlock()
	return err
}

func (c *Client) setSendErr(err error) {
	c.sendErrLock.Lock()
	c.sendErr = err
	c.sendErrLock.Unlock()
}

func (c *Client) processRpcs() {
	for {
		if c.GetSendErr() != nil {
			return
		}

		if c.flushInterval > 0 {
			select {
			case <-time.After(c.flushInterval):
				select {
				case <-c.process:
				// If we got a message on c.process at the same time as our
				// timeout elapsed, we'll non-deterministically land in either
				// cases of this outer select.  Here we double-check whether
				// something was written onto c.process, in which case we don't
				// grab the lock (see comment below in the other case).
				default:
					c.writeMutex.Lock()
				}
			case <-c.process:
				// We don't acquire the lock here, because the thread that sent
				// something on the process channel will have locked the mutex,
				// and will not release it so as to transfer ownership
			}
		} else {
			<-c.process
		}

		rpcs := make([]hrpc.Call, len(c.rpcs))
		for i, rpc := range c.rpcs {
			rpcs[i] = rpc
		}
		c.rpcs = nil
		c.writeMutex.Unlock()

		for i, rpc := range rpcs {
			// If the deadline has been exceeded, don't bother sending the
			// request. The function that placed the RPC in our queue should
			// stop waiting for a result and return an error.
			select {
			case _, ok := <-rpc.GetContext().Done():
				if !ok {
					continue
				}
			default:
			}

			err := c.sendRPC(rpc)
			if err != nil {
				_, ok := err.(UnrecoverableError)
				if ok {
					c.setSendErr(err)

					c.writeMutex.Lock()
					c.rpcs = append(c.rpcs, rpcs[i:]...)
					c.writeMutex.Unlock()

					c.errorEncountered()
					return
				}
				rpc.GetResultChan() <- hrpc.RPCResult{Error: err}
			}
		}
	}
}

func (c *Client) receiveRpcs() {
	var sz [4]byte
	for {
		err := c.readFully(sz[:])
		if err != nil {
			c.setSendErr(err)
			c.errorEncountered()
			return
		}

		buf := make([]byte, binary.BigEndian.Uint32(sz[:]))
		err = c.readFully(buf)
		if err != nil {
			c.setSendErr(err)
			c.errorEncountered()
			return
		}

		resp := &pb.ResponseHeader{}
		respLen, nb := proto.DecodeVarint(buf)
		buf = buf[nb:]
		err = proto.UnmarshalMerge(buf[:respLen], resp)
		buf = buf[respLen:]
		if err != nil {
			// Failed to deserialize the response header
			c.setSendErr(err)
			c.errorEncountered()
			return
		}
		if resp.CallId == nil {
			// Response doesn't have a call ID
			log.Error("Response doesn't have a call ID!")
			c.setSendErr(ErrMissingCallID)
			c.errorEncountered()
			return
		}

		c.sentRPCsMutex.Lock()
		rpc, ok := c.sentRPCs[*resp.CallId]
		c.sentRPCsMutex.Unlock()

		if !ok {
			log.Error("Received a response with an unexpected call ID: %d", *resp.CallId)
			c.sentRPCsMutex.Lock()
			for id, call := range c.sentRPCs {
				log.Error("\t\t%d: %v", id, call)
			}
			c.sentRPCsMutex.Unlock()

			c.setSendErr(fmt.Errorf("HBase sent a response with an unexpected call ID: %d",
				resp.CallId))
			c.errorEncountered()
			return
		}

		var rpcResp proto.Message
		if resp.Exception == nil {
			respLen, nb = proto.DecodeVarint(buf)
			buf = buf[nb:]
			rpcResp = rpc.NewResponse()
			err = proto.UnmarshalMerge(buf, rpcResp)
			buf = buf[respLen:]
		} else {
			javaClass := *resp.Exception.ExceptionClassName
			err = fmt.Errorf("HBase Java exception %s: \n%s", javaClass,
				*resp.Exception.StackTrace)
			if _, ok := javaRetryableExceptions[javaClass]; ok {
				// This is a recoverable error. The client should retry.
				err = RetryableError{err}
			}
		}
		rpc.GetResultChan() <- hrpc.RPCResult{Msg: rpcResp, Error: err}

		c.sentRPCsMutex.Lock()
		delete(c.sentRPCs, *resp.CallId)
		c.sentRPCsMutex.Unlock()
	}
}

func (c *Client) errorEncountered() {
	c.writeMutex.Lock()
	res := hrpc.RPCResult{Error: UnrecoverableError{c.GetSendErr()}}
	for _, rpc := range c.rpcs {
		rpc.GetResultChan() <- res
	}
	c.rpcs = nil
	c.writeMutex.Unlock()

	c.sentRPCsMutex.Lock()
	for _, rpc := range c.sentRPCs {
		rpc.GetResultChan() <- res
	}
	c.sentRPCs = nil
	c.sentRPCsMutex.Unlock()

	c.conn.Close()
}

// Sends the given buffer to the RegionServer.
func (c *Client) write(buf []byte) error {
	n, err := c.conn.Write(buf)

	if err != nil {
		// There was an error while writing
		return err
	}
	if n != len(buf) {
		// We failed to write the entire buffer
		// according to io.Writer interface, this case should not happen
		return ErrShortWrite
	}
	return nil
}

// Tries to read enough data to fully fill up the given buffer.
func (c *Client) readFully(buf []byte) error {
	var err error
	for read, total := 0, 0; total < len(buf); total += read {
		// according to io.Reader interface, n may be less than len(buf) while err is nil
		read, err = c.conn.Read(buf[total:])
		if err != nil {
			// conn error is considered as unrecoverable error
			return UnrecoverableError{fmt.Errorf("Failed to read from the RS: %s", err)}
		} else if read == 0 {
			return fmt.Errorf("Failed to readFully from RS: expect %d but got %d.",
				len(buf), total)
		}
	}
	return nil
}

// Sends the "hello" message needed when opening a new connection.
func (c *Client) sendHello(ctype ClientType) error {
	connHeader := &pb.ConnectionHeader{
		UserInfo: &pb.UserInformation{
			EffectiveUser: proto.String("gopher"),
		},
		ServiceName: proto.String(string(ctype)),
		//CellBlockCodecClass: "org.apache.hadoop.hbase.codec.KeyValueCodec",
	}
	data, err := proto.Marshal(connHeader)
	if err != nil {
		return fmt.Errorf("failed to marshal connection header: %s", err)
	}

	const header = "HBas\x00\x50" // \x50 = Simple Auth.
	buf := make([]byte, 0, len(header)+4+len(data))
	buf = append(buf, header...)
	buf = buf[:len(header)+4]
	binary.BigEndian.PutUint32(buf[6:], uint32(len(data)))
	buf = append(buf, data...)

	return c.write(buf)
}

// QueueRPC will add an rpc call to the queue for processing by the writer
// goroutine
func (c *Client) QueueRPC(rpc hrpc.Call) error {
	sendErr := c.GetSendErr()
	if sendErr != nil {
		return sendErr
	}
	c.writeMutex.Lock()
	c.rpcs = append(c.rpcs, rpc)
	// < 0 means only flush when timeout; 0 means flush each time
	if c.rpcQueueSize > 0 && len(c.rpcs) >= c.rpcQueueSize {
		c.process <- struct{}{}
		// We don't release the lock here, because we want to transfer ownership
		// of the lock to the goroutine that processes the RPCs
	} else {
		c.writeMutex.Unlock()
	}
	return nil
}

// sendRPC sends an RPC out to the wire.
// Returns the response (for now, as the call is synchronous).
func (c *Client) sendRPC(rpc hrpc.Call) error {
	// Header.
	c.id++
	reqheader := &pb.RequestHeader{
		CallId:       &c.id,
		MethodName:   proto.String(rpc.GetName()),
		RequestParam: proto.Bool(true),
	}

	payload, err := rpc.Serialize()
	if err != nil {
		return fmt.Errorf("Failed to serialize RPC: %s", err)
	}
	payloadLen := proto.EncodeVarint(uint64(len(payload)))

	headerData, err := proto.Marshal(reqheader)
	if err != nil {
		return fmt.Errorf("Failed to marshal Get request: %s", err)
	}

	buf := make([]byte, 5, 4+1+len(headerData)+len(payloadLen)+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(cap(buf)-4))
	buf[4] = byte(len(headerData))
	buf = append(buf, headerData...)
	buf = append(buf, payloadLen...)
	buf = append(buf, payload...)

	c.sentRPCsMutex.Lock()
	c.sentRPCs[c.id] = rpc
	c.sentRPCsMutex.Unlock()

	err = c.write(buf)
	if err != nil {
		return UnrecoverableError{err}
	}

	return nil
}
