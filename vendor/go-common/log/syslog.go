package log

import (
	"bytes"
	"log/syslog"
	"strconv"
	"sync"

	log "golang/log4go"
)

var (
	// message format `{"project":"%s","level":"%s","traceid":"%s","path":"%s","message":"%s","time":%d}`
	_sysMsg1 = []byte(`{"project":"`)
	_sysMsg2 = []byte(`","level":"`)
	_sysMsg3 = []byte(`","traceid":"`)
	_sysMsg4 = []byte(`","path":"`)
	_sysMsg5 = []byte(`","message":"`)
	_sysMsg6 = []byte(`","time":`)
	_sysMsg7 = []byte(`}`)

	_sysLevelInfo  = []byte("info")
	_sysLevelError = []byte("error")
	_sysLevelWarn  = []byte("warn")
)

type SysConfig struct {
	Project  string
	Proto    string
	Addr     string
	ChanSize int
}

type Syslog struct {
	project   []byte
	syslogger *syslog.Writer
	msgs      chan *bytes.Buffer
	pool      sync.Pool
}

// NewSysLog new a syslog
func NewSysLog(c *SysConfig) (s *Syslog) {
	var err error
	s = &Syslog{
		project: []byte(c.Project),
		msgs:    make(chan *bytes.Buffer, c.ChanSize),
	}
	if s.syslogger, err = syslog.Dial(c.Proto, c.Addr, syslog.LOG_WARNING, c.Project); err != nil {
		log.Error("syslog.Dial(%s,%s,%s,%s) error(%v)", c.Proto, c.Addr, syslog.LOG_WARNING, c.Project, err)
		return
	}
	go s.writeproc()
	return
}

// Close close the connection.
func (s *Syslog) Close() {
	if s.syslogger != nil {
		s.syslogger.Close()
	}
}

// Info send info log to syslog.
func (s *Syslog) Info(traceID, path, msg string, tm float64) {
	s.send(_sysLevelInfo, []byte(traceID), []byte(path), []byte(msg), []byte(strconv.FormatFloat(tm, 'f', 6, 64)))
}

// Error send error log to syslog.
func (s *Syslog) Error(traceID, path, msg string, tm float64) {
	s.send(_sysLevelError, []byte(traceID), []byte(path), []byte(msg), []byte(strconv.FormatFloat(tm, 'f', 6, 64)))
}

// Warn send warn log to syslog.
func (s *Syslog) Warn(traceID, path, msg string, tm float64) {
	s.send(_sysLevelWarn, []byte(traceID), []byte(path), []byte(msg), []byte(strconv.FormatFloat(tm, 'f', 6, 64)))
}

// buf get bytes buffer from pool.
func (s *Syslog) buf() (buf *bytes.Buffer) {
	tmp := s.pool.Get()
	var ok bool
	if buf, ok = tmp.(*bytes.Buffer); !ok {
		buf = bytes.NewBuffer([]byte{})
	}
	return
}

// putBuf put bytes buffer into pool.
func (s *Syslog) putBuf(buf *bytes.Buffer) {
	buf.Reset()
	s.pool.Put(buf)
}

// send log to udp statsd daemon
func (s *Syslog) send(level, traceID, path, msg, tm []byte) {
	if s.syslogger == nil {
		return
	}
	buf := s.buf()
	buf.Write(_sysMsg1)
	buf.Write(s.project)
	buf.Write(_sysMsg2)
	buf.Write(level)
	buf.Write(_sysMsg3)
	buf.Write(traceID)
	buf.Write(_sysMsg4)
	buf.Write(path)
	buf.Write(_sysMsg5)
	buf.Write(msg)
	buf.Write(_sysMsg6)
	buf.Write(tm)
	buf.Write(_sysMsg7)
	select {
	case s.msgs <- buf:
	default:
		s.putBuf(buf)
		log.Warn("Syslog message channel is full")
	}
}

// writeproc write data into connection.
func (s *Syslog) writeproc() {
	var err error
	var buf *bytes.Buffer
	for {
		buf = <-s.msgs
		if _, err = buf.WriteTo(s.syslogger); err != nil {
			log.Error("buf.WriteTo() error(%v)", err)
		}
		s.putBuf(buf)
	}
}
