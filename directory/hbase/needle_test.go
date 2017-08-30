package hbase

import (
	"bfs/libs/meta"
	"testing"
	"time"
)

func TestPutNeedle(t *testing.T) {
	c := getClient()
	if err := c.putNeedle(&meta.Needle{
		Key:    1234567,
		Cookie: 1111111,
		Vid:    1,
		MTime:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("err:%v", err.Error())
	}

}

func TestGetNeedle(t *testing.T) {
	c := getClient()
	mn, err := c.getNeedle(1234567)
	if err != nil {
		t.Fatalf("err:%v", err.Error())
	}
	t.Logf("mn:%v", mn)
}

func TestDelNeedle(t *testing.T) {
	c := getClient()
	if err := c.delNeedle(1234567); err != nil {
		t.Fatalf("err:%v", err.Error())
	}
}
