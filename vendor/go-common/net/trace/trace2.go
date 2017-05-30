package trace

import (
	"bytes"
	"context"
	"encoding/base64"
	"golang/uuid"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"

	log "golang/log4go"

	"github.com/zhenjl/cityhash"
)

var (
	_ev2 chan *Trace2
	_bs  [20]byte
)

const (
	_maxLevel      = 255
	_maxAnnotation = 10
	// ClassComponent component class
	ClassComponent = int8(0)
	// ClassService service class
	ClassService = int8(1)
	// comment
	_commentString = int8(0)
	_commentBinary = int8(1)
	// separator
	_separator = "\001"
	// protocol
	_spanProto = byte(1)
	_annoProto = byte(2)
	// max id
	_maxInt64 = math.MaxInt64
)

// Init2 init the trace, must set a trace writer and id generator.
func Init2(owner string, writer io.Writer) {
	_ev2 = make(chan *Trace2, 10240)
	_owner = owner
	_writer = writer
	go recordproc()
}

// Stop2 stop the trace.
func Stop2() {
	if _ev2 != nil {
		_ev2 <- nil
	}
}

func id2() uint64 {
	i := [16]byte(uuid.NewV1())
	return cityhash.CityHash64(i[:], 16) % _maxInt64
}

// Trace2 is server and client called trace info.
type Trace2 struct {
	ID       uint64 `json:"-"`
	SpanID   uint64 `json:"-"`
	ParentID uint64 `json:"-"`
	Level    int32  `json:"-"`
	Sampled  bool   `json:"-"`

	address    string      // ip:port address
	family     string      // project name, service name
	title      string      // method name, rpc name
	class      int8        // 0: component, 1: service
	comment    interface{} // comment
	event      int8        // trace event
	startEvent int8        // trace temp event
	time       int64       // trace timestamp
	annotation int         // annotation times
	err        error       // trace error info
	// status
	start bool
	done  bool
	// next Trace2
	next *Trace2
}

// NewTrace2 a root trace.
func NewTrace2(family, title, address string, class int8) *Trace2 {
	t := new(Trace2)
	t.ID = id2()
	t.SpanID = t.ID
	t.ParentID = 0
	var sampled bool
	if _ratio <= 0 {
		sampled = false
	} else if _ratio >= 1 {
		sampled = true
	} else {
		sampled = (rand.Float32() <= _ratio)
	}
	t.Sampled = sampled
	t.family = family
	t.title = title
	t.address = address
	t.class = class
	t.Level = 1
	t.next = nil
	return t
}

// Fork fork a trace with different id.
func (t *Trace2) Fork(family, title, address string) *Trace2 {
	t1 := new(Trace2)
	t1.ID = t.ID
	t1.SpanID = id2()
	t1.ParentID = t.SpanID
	t1.Sampled = t.Sampled
	t1.family = family
	t1.title = title
	t1.address = address
	t1.class = ClassComponent
	t1.Level = t.Level + 1
	t1.next = nil
	return t1
}

// Client record the trace with a event _clientStart
// and called it when the call start.
func (t *Trace2) Client(comment string) {
	if t.start || t.Level > _maxLevel {
		return
	}
	t.time = time.Now().UnixNano()
	t.start = true
	t.startEvent = _clientStart
	t.event = t.startEvent
	t.comment = comment
	if t.Sampled {
		tr := t.copy()
		t.next = tr
	}
}

// Server record the trace with a event _serverReceive
// and called it when the call start.
func (t *Trace2) Server(comment string) {
	if t.start || t.Level > _maxLevel {
		return
	}
	t.time = time.Now().UnixNano()
	t.start = true
	t.startEvent = _serverReceive
	t.event = t.startEvent
	t.comment = comment
	if t.Sampled {
		tr := t.copy()
		t.next = tr
	}
}

// Annotation record the trace with a event UserDefine and called it when you
// want more info, support *net.URL, []byte, string.
func (t *Trace2) Annotation(d interface{}) {
	if t.annotation++; t.annotation > _maxAnnotation {
		return
	}
	t.time = time.Now().UnixNano()
	t.event = _annotation
	t.comment = d
	if t.Sampled {
		tr := t.copy()
		t.next = tr
	}
}

// Finish when trace finish call it.
func (t *Trace2) Finish() {
	if !t.start || t.done {
		return
	}
	t.time = time.Now().UnixNano()
	t.done = true
	switch t.startEvent {
	case _clientStart:
		t.event = _clientReceive
	case _serverReceive:
		t.event = _serverSend
	default:
		return // unknown event
	}
	t.record()
}

// Done when trace finish call it.
func (t *Trace2) Done(err *error) {
	if err != nil {
		t.err = *err
	}
	t.Finish()
}

// SetFamily set the trace family.
func (t *Trace2) SetFamily(family string) {
	t.family = family
}

// SetClass set the trace class.
func (t *Trace2) SetClass(class int8) {
	t.class = class
}

// SetTitle set the trace title.
func (t *Trace2) SetTitle(title string) {
	t.title = title
}

// WriteTo write trace into a buffer.
func (t *Trace2) WriteTo(b *bytes.Buffer) (err error) {
	// write protol.
	var (
		comment string
		errStr  string
	)
	switch value := t.comment.(type) {
	case *url.URL:
		comment = base64.StdEncoding.EncodeToString([]byte(value.String()))
	case string:
		comment = base64.StdEncoding.EncodeToString([]byte(value))
	case []byte:
		comment = base64.StdEncoding.EncodeToString(value)
	}
	if t.event == _annotation {
		err = b.WriteByte(_annoProto)
	} else {
		err = b.WriteByte(_spanProto)
	}
	_, err = b.WriteString(_separator)
	// write data.
	_, err = b.Write(formatUint(uint64(t.time)))
	_, err = b.WriteString(_separator)
	_, err = b.Write(formatUint(t.ID))
	_, err = b.WriteString(_separator)
	_, err = b.Write(formatUint(t.SpanID))
	_, err = b.WriteString(_separator)
	if t.ParentID != 0 {
		_, err = b.Write(formatUint(t.ParentID))
	}
	_, err = b.WriteString(_separator)
	_, err = b.Write(formatUint(uint64(t.event)))
	_, err = b.WriteString(_separator)
	_, err = b.Write(formatUint(uint64(t.Level)))
	_, err = b.WriteString(_separator)
	if t.event == _annotation {
		_, err = b.WriteString(comment)
	} else {
		_, err = b.Write(formatUint(uint64(t.class)))
		_, err = b.WriteString(_separator)
		_, err = b.WriteString(t.address)
		_, err = b.WriteString(_separator)
		_, err = b.WriteString(t.family)
		_, err = b.WriteString(_separator)
		_, err = b.WriteString(t.title)
		_, err = b.WriteString(_separator)
		_, err = b.WriteString(comment)
		_, err = b.WriteString(_separator)
		_, err = b.WriteString(_owner)
		_, err = b.WriteString(_separator)
		if t.err != nil {
			errStr = t.err.Error()
		}
		_, err = b.WriteString(errStr)
	}
	_, err = b.WriteString("\n")
	return
}

// WithHTTP set trace id into http request.
func (t *Trace2) WithHTTP(req *http.Request) {
	req.Header.Set(_httpHeaderID, strconv.FormatUint(t.ID, 10))
	req.Header.Set(_httpHeaderSpanID, strconv.FormatUint(t.SpanID, 10))
	req.Header.Set(_httpHeaderParentID, strconv.FormatUint(t.ParentID, 10))
	req.Header.Set(_httpHeaderSampled, strconv.FormatBool(t.Sampled))
	req.Header.Set(_httpHeaderLevel, strconv.FormatInt(int64(t.Level), 10))
	req.Header.Set(_httpHeaderUser, Owner())
}

// FromHTTP init trace from http request.
func FromHTTP(req *http.Request, family, title, address string) (user string, t *Trace2) {
	var (
		idStr, spanIDStr, parentIDStr, lvStr string
		id, spanID, parentID                 uint64
		lv                                   int64
		err                                  error
	)
	header := req.Header
	idStr = header.Get(_httpHeaderID)
	spanIDStr = header.Get(_httpHeaderSpanID)
	parentIDStr = header.Get(_httpHeaderParentID)
	lvStr = header.Get(_httpHeaderLevel)
	user = header.Get(_httpHeaderUser)
	t = NewTrace2(family, title, address, ClassService)
	if str := header.Get(_httpHeaderSampled); str == "true" {
		t.Sampled = true
	}
	if idStr != "" {
		if id, err = strconv.ParseUint(idStr, 10, 64); err == nil {
			t.ID = id
		}
		if spanID, err = strconv.ParseUint(spanIDStr, 10, 64); err == nil {
			t.SpanID = spanID
		}
		if parentID, err = strconv.ParseUint(parentIDStr, 10, 64); err == nil {
			t.ParentID = parentID
		}
		if lv, err = strconv.ParseInt(lvStr, 10, 64); err == nil {
			t.Level = int32(lv)
		}
	}
	return
}

// record send to recordproc chan.
func (t *Trace2) record() {
	if t != nil && t.Sampled && _writer != nil {
		select {
		case _ev2 <- t:
		default:
			log.Warn("trace chan full, discard the trace: %v", t)
		}
	}
}

func (t *Trace2) copy() (tr *Trace2) {
	tr = new(Trace2)
	*tr = *t
	return
}

// recordproc write to trace writer get data from chan.
func recordproc() {
	var (
		err error
		t   *Trace2
		buf = new(bytes.Buffer)
	)
	for {
		if t = <-_ev2; t != nil {
			for t != nil {
				if err = t.WriteTo(buf); err == nil {
					if _, err = buf.WriteTo(_writer); err != nil {
						log.Error("buf.WriteTo() error(%v)", err)
					}
				}
				buf.Reset()
				t = t.next
			}
			t = nil
		} else {
			log.Warn("trace writeproc goroutine exit")
			return
		}
	}
}

var _contextKey2 = contextKeyT("go-common/net/trace.Trace2")

// NewContext2 new a trace context.
func NewContext2(c context.Context, t *Trace2) context.Context {
	return context.WithValue(c, _contextKey2, t)
}

// FromContext2 returns the trace bound to the context, if any.
func FromContext2(c context.Context) (*Trace2, bool) {
	t, ok := c.Value(_contextKey2).(*Trace2)
	return t, ok
}

func formatUint(u uint64) []byte {
	var i = 20
	for {
		i--
		_bs[i] = byte(u%10 + '0')
		if u = u / 10; u == 0 {
			break
		}
	}
	return _bs[i:]
}
