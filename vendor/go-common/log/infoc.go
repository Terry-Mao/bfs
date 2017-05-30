package log

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	log "golang/log4go"
)

const (
	_infocSpliter  = "\001"
	_infocReplacer = "|"

	_infocTimeout = 50 * time.Millisecond
)

var (
	_infocHeader = `"dept":"ugc","app":"%s","type":"%s"`
	_infocEnd    = []byte("\n")
)

type InfocConfig struct {
	Project string
	Name    string
	// udp or tcp
	Proto    string
	Addr     string
	ChanSize int
	// file
	Path string
}

// Infoc infoc struct.
type Infoc struct {
	c      *InfocConfig
	header []byte
	// udp or tcp
	conn net.Conn
	// file
	lg log.Logger
	// chan
	msgs chan *bytes.Buffer
	pool sync.Pool
}

// NewInfoc new infoc logger.
func NewInfoc(c *InfocConfig) (i *Infoc) {
	i = &Infoc{
		c:      c,
		header: []byte(fmt.Sprintf(_infocHeader, c.Project, c.Name)),
		msgs:   make(chan *bytes.Buffer, c.ChanSize),
		pool: sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
	if c.Proto != "" && c.Addr != "" {
		var err error
		if i.conn, err = net.Dial(i.c.Proto, i.c.Addr); err != nil {
			panic(err)
		}
	}
	if c.Path != "" {
		lg := log.Logger{}
		w := log.NewFileLogWriter(c.Path, false)
		w.SetRotateDaily(true)
		w.SetFormat("%M")
		lg.AddFilter("infoc_file", log.INFO, w)
		i.lg = lg
	}
	go i.writeproc()
	return
}

// Info record log to file.
func (i *Infoc) Info(args ...string) error {
	if len(args) == 0 {
		return nil
	}
	buf := i.buf()
	buf.Write(i.header)
	// // append first arg
	if _, err := buf.WriteString(args[0]); err != nil {
		return err
	}
	for _, arg := range args[1:] {
		// append ,arg
		if _, err := buf.WriteString(_infocSpliter); err != nil {
			return err
		}
		if _, err := buf.WriteString(strings.Replace(arg, _infocSpliter, _infocReplacer, -1)); err != nil {
			return err
		}
	}
	buf.Write(_infocEnd)
	select {
	case i.msgs <- buf:
	default:
		i.putBuf(buf)
		log.Warn("Infoc message channel is full")
	}
	return nil
}

// Close close the connection.
func (i *Infoc) Close() {
	if i.conn != nil {
		i.conn.Close()
	}
}

// buf get bytes buffer from pool.
func (i *Infoc) buf() (buf *bytes.Buffer) {
	tmp := i.pool.Get()
	var ok bool
	if buf, ok = tmp.(*bytes.Buffer); !ok {
		buf = bytes.NewBuffer([]byte{})
	}
	return
}

// putBuf put bytes buffer into pool.
func (i *Infoc) putBuf(buf *bytes.Buffer) {
	buf.Reset()
	i.pool.Put(buf)
}

// writeproc write data into connection.
func (i *Infoc) writeproc() {
	var (
		buf *bytes.Buffer
		err error
	)
	for {
		buf = <-i.msgs
		// file
		if i.lg != nil {
			bs := buf.Bytes()
			i.lg.Info(string(bs[:len(bs)-1])) // del \n
		}
		// udp or tcp
		if i.conn != nil {
			// if tcp, set deadline
			if i.c.Proto == "tcp" {
				i.conn.SetDeadline(time.Now().Add(_infocTimeout))
			}
			_, err = i.conn.Write(buf.Bytes())
		}
		if err != nil && i.c.Proto == "tcp" {
			if i.conn != nil {
				i.conn.Close()
			}
			i.conn, err = net.DialTimeout(i.c.Proto, i.c.Addr, _infocTimeout)
		}
		i.putBuf(buf)
	}
}
