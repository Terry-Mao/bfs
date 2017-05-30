package log

import (
	"bytes"
	"net"
	"strconv"
	"sync"

	log "golang/log4go"
)

var (
	// message format `{"project":"%s","level":"%s","traceid":"%s","path":"%s","message":"%s","time":%d}`
	_elkMsg1 = []byte(`{"project":"`)
	_elkMsg2 = []byte(`","level":"`)
	_elkMsg3 = []byte(`","traceid":"`)
	_elkMsg4 = []byte(`","path":"`)
	_elkMsg5 = []byte(`","message":"`)
	_elkMsg6 = []byte(`","time":`)
	_elkMsg7 = []byte(`}`)

	_elkLevelInfo  = []byte("info")
	_elkLevelError = []byte("error")
	_elkLevelWarn  = []byte("warn")
)

type ELKConfig struct {
	Project  string
	Addr     string
	ChanSize int
}

type ELK struct {
	project []byte
	conn    net.Conn
	msgs    chan *bytes.Buffer
	pool    sync.Pool
}

// NewELK new a elk
func NewELK(c *ELKConfig) (e *ELK) {
	var err error
	e = &ELK{
		project: []byte(c.Project),
		msgs:    make(chan *bytes.Buffer, c.ChanSize),
	}
	if e.conn, err = net.Dial("udp", c.Addr); err != nil {
		log.Error("net.Dial(udp, %s) error(%v)", c.Addr, err)
		return
	}
	go e.writeproc()
	return
}

// Close close the connection.
func (e *ELK) Close() {
	if e.conn != nil {
		e.conn.Close()
	}
}

// Info send info log to elk.
func (e *ELK) Info(traceID, path, msg string, tm float64) {
	e.send(_elkLevelInfo, []byte(traceID), []byte(path), []byte(msg), []byte(strconv.FormatFloat(tm, 'f', 6, 64)))
}

// Error send error log to elk.
func (e *ELK) Error(traceID, path, msg string, tm float64) {
	e.send(_elkLevelError, []byte(traceID), []byte(path), []byte(msg), []byte(strconv.FormatFloat(tm, 'f', 6, 64)))
}

// Warn send warn log to elk.
func (e *ELK) Warn(traceID, path, msg string, tm float64) {
	e.send(_elkLevelWarn, []byte(traceID), []byte(path), []byte(msg), []byte(strconv.FormatFloat(tm, 'f', 6, 64)))
}

// buf get bytes buffer from pool.
func (e *ELK) buf() (buf *bytes.Buffer) {
	tmp := e.pool.Get()
	var ok bool
	if buf, ok = tmp.(*bytes.Buffer); !ok {
		buf = bytes.NewBuffer([]byte{})
	}
	return
}

// putBuf put bytes buffer into pool.
func (e *ELK) putBuf(buf *bytes.Buffer) {
	buf.Reset()
	e.pool.Put(buf)
}

// send log to udp statsd daemon
func (e *ELK) send(level, traceID, path, msg, tm []byte) {
	if e.conn == nil {
		return
	}
	buf := e.buf()
	buf.Write(_elkMsg1)
	buf.Write(e.project)
	buf.Write(_elkMsg2)
	buf.Write(level)
	buf.Write(_elkMsg3)
	buf.Write(traceID)
	buf.Write(_elkMsg4)
	buf.Write(path)
	buf.Write(_elkMsg5)
	buf.Write(msg)
	buf.Write(_elkMsg6)
	buf.Write(tm)
	buf.Write(_elkMsg7)
	select {
	case e.msgs <- buf:
	default:
		e.putBuf(buf)
		log.Warn("ELK message channel is full")
	}
}

// writeproc write data into connection.
func (e *ELK) writeproc() {
	var buf *bytes.Buffer
	for {
		buf = <-e.msgs
		e.conn.Write(buf.Bytes())
		e.putBuf(buf)
	}
}
