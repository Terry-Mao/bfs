package hbase

import (
	"bfs/libs/errors"
	"bfs/libs/meta"
	"testing"
	"time"
)

func TestGetFile(t *testing.T) {
	c := getClient()
	f, err := c.getFile("test", "guhaotest111.jpg")
	if err != nil && err != errors.ErrNeedleNotExist {
		t.Fatalf("err:%v", err.Error())
	}
	t.Logf("f: %v", f)
}

func TestPutFile(t *testing.T) {
	c := getClient()
	err := c.putFile("test", &meta.File{
		Filename: "guhaotest111.jpg",
		Key:      1234567,
		Sha1:     "12312312312312312",
		Mine:     "image/jpg",
		Status:   123,
		MTime:    time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("err:%v", err.Error())
	}
}

func TestDelFile(t *testing.T) {
	c := getClient()
	if err := c.delFile("test", "guhaotest111.jpg"); err != nil {
		t.Fatalf("err:%v", err.Error())
	}
}

func TestExistFile(t *testing.T) {
	c := getClient()
	exist, err := c.existFile("test", "guhaotest111.jpg")
	if err != errors.ErrNeedleExist {
		t.Fatalf("err:%v", err.Error())
	}
	t.Logf("pass:err:%v", exist)
}
