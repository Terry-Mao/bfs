package needle

import (
	"github.com/Terry-Mao/bfs/libs/errors"
	"hash/crc32"
	"io"
)

// Needles is needle list.
type Needles struct {
	Num     int
	needles []Needle
	buffer  []byte
	size    int
	wn      int
	ws      int32
}

// NewNeedles new a needles.
func NewNeedles(num, size int) (ns *Needles) {
	ns = new(Needles)
	ns.needles = make([]Needle, num)
	ns.buffer = make([]byte, num*Size(size))
	ns.Num = num
	ns.size = size
	ns.wn = 0
	ns.ws = 0
	return
}

// Needle get a needle by index.
func (ns *Needles) Needle(i int) *Needle {
	return &ns.needles[i]
}

// Reset reset needles.
func (ns *Needles) Reset() {
	ns.wn = 0
	ns.ws = 0
}

// Buffer get a needles buffer.
func (ns *Needles) Buffer() []byte {
	return ns.buffer[:ns.ws]
}

// WriteFrom Write needle from io.Reader into buffer.
func (ns *Needles) WriteFrom(key int64, cookie int32, size int32, rd io.Reader) (err error) {
	var (
		n            *Needle
		data         []byte
		headerOffset int32
		dataOffset   int32
		footerOffset int32
		endOffset    int32
	)
	if ns.wn >= ns.Num {
		return errors.ErrNeedleFull
	}
	n = &ns.needles[ns.wn]
	n.initSize(key, cookie, size)
	headerOffset = ns.ws
	dataOffset = headerOffset + _headerSize
	footerOffset = dataOffset + n.Size
	endOffset = footerOffset + n.FooterSize
	data = ns.buffer[dataOffset:footerOffset]
	// write into buffer header->data->footer
	if err = n.WriteHeader(ns.buffer[headerOffset:dataOffset]); err == nil {
		if _, err = rd.Read(data); err == nil {
			n.Data = data
			n.Checksum = crc32.Update(0, _crc32Table, data)
			err = n.WriteFooter(ns.buffer[footerOffset:endOffset])
		}
	}
	ns.wn++
	ns.ws += n.TotalSize
	return
}
