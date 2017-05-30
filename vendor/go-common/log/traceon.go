package log

import (
	"bytes"
	"net"
	"strconv"
	"sync"

	xip "go-common/net/ip"
	log "golang/log4go"
)

/*
- ver:版本号，默认为1
- host:主机
- bus:注册的服务
- metric：指标名
- mtype:指标类型，1：异常或错误，2：普通业务
- value:指标值
- ctt：日志时间，精确到毫秒
- trace: trace id 后跟span id，以“,”分隔
- detail:日志详细信息
*/

var (
	_traceonStart   = []byte("000001\001\001")
	_traceonEnd     = []byte("\001\001")
	_traceonSpliter = []byte("\001")

	_traceonException = []byte("1") // exception
	_traceonMetric    = []byte("2") // metric

	_traceonVer = []byte("1") // traceon version
)

type TraceonConfig struct {
	Business int
	Proto    string
	Addr     string
	ChanSize int
}

type Traceon struct {
	bus  []byte
	host []byte
	conn net.Conn
	msgs chan *bytes.Buffer
	pool sync.Pool
}

// NewTraceon new a Traceon.
func NewTraceon(c *TraceonConfig) (to *Traceon) {
	var err error
	to = &Traceon{
		bus:  []byte(strconv.Itoa(c.Business)),
		host: []byte(xip.InternalIP()),
		msgs: make(chan *bytes.Buffer, c.ChanSize),
	}
	if to.conn, err = net.Dial(c.Proto, c.Addr); err != nil {
		log.Error("net.Dial(%s, %s) error(%v)", c.Proto, c.Addr, err)
		return
	}
	go to.writeproc()
	return
}

// Close close the connection.
func (to *Traceon) Close() {
	if to.conn != nil {
		to.conn.Close()
	}
}

// Exception send exception log to Traceon.
func (to *Traceon) Exception(metric, value, detail string, traceID, spanID, ctt int64) {
	to.send(_traceonException, _traceonVer, []byte(metric), []byte(value), []byte(detail), []byte(strconv.FormatInt(traceID, 10)+","+strconv.FormatInt(spanID, 10)), []byte(strconv.FormatInt(ctt, 10)))
}

// Metric send business metric log to Traceon.
func (to *Traceon) Metric(metric, value, detail string, traceID, spanID, ctt int64) {
	to.send(_traceonMetric, _traceonVer, []byte(metric), []byte(value), []byte(detail), []byte(strconv.FormatInt(traceID, 10)+","+strconv.FormatInt(spanID, 10)), []byte(strconv.FormatInt(ctt, 10)))
}

// buf get bytes buffer from pool.
func (to *Traceon) buf() (buf *bytes.Buffer) {
	tmp := to.pool.Get()
	var ok bool
	if buf, ok = tmp.(*bytes.Buffer); !ok {
		buf = bytes.NewBuffer([]byte{})
	}
	return
}

// putBuf put bytes buffer into pool.
func (to *Traceon) putBuf(buf *bytes.Buffer) {
	buf.Reset()
	to.pool.Put(buf)
}

// send log to udp statsd daemon
func (to *Traceon) send(tp, ver, metric, value, detail, trace, ctt []byte) {
	if to.conn == nil {
		return
	}
	buf := to.buf()
	buf.Write(_traceonStart)
	buf.Write(ver)
	buf.Write(_traceonSpliter)
	buf.Write(to.host)
	buf.Write(_traceonSpliter)
	buf.Write(to.bus)
	buf.Write(_traceonSpliter)
	buf.Write(metric)
	buf.Write(_traceonSpliter)
	buf.Write(tp)
	buf.Write(_traceonSpliter)
	buf.Write(value)
	buf.Write(_traceonSpliter)
	buf.Write(ctt)
	buf.Write(_traceonSpliter)
	buf.Write(trace)
	buf.Write(_traceonSpliter)
	buf.Write(detail)
	buf.Write(_traceonEnd)
	select {
	case to.msgs <- buf:
	default:
		to.putBuf(buf)
		log.Warn("Traceon message channel is full")
	}
}

// writeproc write data into connection.
func (to *Traceon) writeproc() {
	var buf *bytes.Buffer
	for {
		buf = <-to.msgs
		to.conn.Write(buf.Bytes())
		to.putBuf(buf)
	}
}
