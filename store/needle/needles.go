package needle

import (
	"bfs/libs/errors"
	"io"
)

// Needles is needle list.
type Needles struct {
	rn        int
	wn        int
	Num       int
	needles   []Needle
	TotalSize int32
}

// NewNeedles new a needles.
func NewNeedles(num int) *Needles {
	var ns = new(Needles)
	ns.Num = num
	ns.needles = make([]Needle, num)
	return ns
}

// ReadFrom Write needle from io.Reader into buffer.
func (ns *Needles) ReadFrom(key int64, cookie, size int32, rd io.Reader) (err error) {
	if ns.wn >= ns.Num {
		return errors.ErrNeedleFull
	}
	var n = &ns.needles[ns.wn]
	n.InitWriter(key, cookie, size)
	if err = n.ReadFrom(rd); err != nil {
		n.Close()
		return
	}
	ns.wn++
	ns.TotalSize += n.TotalSize
	return
}

// Next get a needle from needles.
func (ns *Needles) Next() (n *Needle) {
	if ns.rn >= ns.wn {
		return nil
	}
	n = &ns.needles[ns.rn]
	ns.rn++
	return
}

func (ns *Needles) Close() {
	for i := 0; i < ns.wn; i++ {
		ns.needles[i].Close()
	}
}
