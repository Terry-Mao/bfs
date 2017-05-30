package xints

import (
	"database/sql/driver"
	"encoding/binary"
)

// Ints be used to MySql varbinary converting.
type Ints []int64

func (is *Ints) Scan(src interface{}) (err error) {
	switch sc := src.(type) {
	case []byte:
		var res []int64
		for i := 0; i < len(sc) && i+8 <= len(sc); i += 8 {
			ui := binary.BigEndian.Uint64(sc[i : i+8])
			res = append(res, int64(ui))
		}
		*is = res
	}
	return
}

func (is Ints) Value() (driver.Value, error) {
	return is.Bytes(), nil
}

func (is Ints) Bytes() []byte {
	res := make([]byte, 0, 8*len(is))
	for _, i := range is {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		res = append(res, bs...)
	}
	return res
}

func (is *Ints) Evict(e int64) (ok bool) {
	res := make([]int64, len(*is)-1)
	for _, v := range *is {
		if v != e {
			res = append(res, v)
		} else {
			ok = true
		}
	}
	*is = res
	return
}

func (is Ints) Exist(i int64) (e bool) {
	for _, v := range is {
		if v == i {
			e = true
			return
		}
	}
	return
}
