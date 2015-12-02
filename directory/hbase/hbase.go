package hbase

import (
	"bilizone/model/hbase"
	"bilizone/model/stat"
	"bytes"
	"encoding/binary"
	log "github.com/felixhao/log4go"
)

type HBaseDao struct {
}

func NewHBaseDao() *HBaseDao {
	return &HBaseDao{}
}

func (dao *HBaseDao) GetStat(aid int64) (s *stat.Stat, err error) {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(aid))
	// get client
	c, err := hbasePool.Get()
	if err != nil {
		log.Error("hbasePool.Get() error(%v)", err)
		return
	}
	r, err := c.(hbase.THBaseService).Get(stat.HbaseTable, &hbase.TGet{Row: bs})
	if err != nil {
		log.Error("client.Get error(%v)", err)
		return
	}
	s = &stat.Stat{}
	for _, cv := range r.ColumnValues {
		if cv != nil {
			v := int(binary.BigEndian.Uint64(cv.Value))
			if bytes.Equal(cv.Family, stat.HbaseFamilyPlat) {
				if bytes.Equal(cv.Qualifier, stat.HbaseColumnWeb) {
					s.Web = v
				} else if bytes.Equal(cv.Qualifier, stat.HbaseColumnH5) {
					s.H5 = v
				} else if bytes.Equal(cv.Qualifier, stat.HbaseColumnOuter) {
					s.Outer = v
				} else if bytes.Equal(cv.Qualifier, stat.HbaseColumnIos) {
					s.Ios = v
				} else if bytes.Equal(cv.Qualifier, stat.HbaseColumnAndroid) {
					s.Android = v
				}
			} else if bytes.Equal(cv.Family, stat.HbaseFamilyOther) {
				if bytes.Equal(cv.Qualifier, stat.HbaseColumnFav) {
					s.Fav = v
				} else if bytes.Equal(cv.Qualifier, stat.HbaseColumnShare) {
					s.Share = v
				} else if bytes.Equal(cv.Qualifier, stat.HbaseColumnReply) {
					s.Reply = v
				}
			}
		}
	}
	return
}
