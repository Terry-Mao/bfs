package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestHTTPAdmin(t *testing.T) {
	var (
		s     *Store
		z     *Zookeeper
		resp  *http.Response
		body  []byte
		err   error
		buf   = &bytes.Buffer{}
		tr    = &testRet{}
		bfile = "./test/block_admin_1"
		ifile = "./test/block_admin_1.idx"
	)
	os.Remove(testConf.VolumeIndex)
	os.Remove(testConf.FreeVolumeIndex)
	os.Remove("./test/_free_block_1")
	os.Remove("./test/_free_block_1.idx")
	os.Remove("./test/_free_block_2")
	os.Remove("./test/_free_block_2.idx")
	os.Remove("./test/1_0")
	os.Remove("./test/1_1")
	os.Remove(bfile)
	os.Remove(ifile)
	defer os.Remove(testConf.VolumeIndex)
	defer os.Remove(testConf.FreeVolumeIndex)
	defer os.Remove("./test/_free_block_1")
	defer os.Remove("./test/_free_block_1.idx")
	defer os.Remove("./test/_free_block_2")
	defer os.Remove("./test/_free_block_2.idx")
	defer os.Remove("./test/1_0")
	defer os.Remove("./test/1_1")
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	if z, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack/test-admin/"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	z.DelVolume(1)
	z.DelVolume(2)
	if s, err = NewStore(z, testConf); err != nil {
		t.Errorf("NewStore() error(%v)", err)
		t.FailNow()

	}
	defer s.Close()
	StartAdmin("localhost:6063", s)
	time.Sleep(1 * time.Second)
	buf.Reset()
	buf.WriteString("n=2&bdir=./test/&idir=./test/")
	if resp, err = http.Post("http://localhost:6063/add_free_volume", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post() error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll() error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, tr); err != nil {
		t.Errorf("json.Unmarshal() error(%v)", err)
		t.FailNow()
	}
	if tr.Ret != 1 {
		t.Errorf("add_free_volume: %d", tr.Ret)
		t.FailNow()
	}
	buf.Reset()
	buf.WriteString("vid=1")
	if resp, err = http.Post("http://localhost:6063/add_volume", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post() error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll() error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, tr); err != nil {
		t.Errorf("json.Unmarshal() error(%v)", err)
		t.FailNow()
	}
	if tr.Ret != 1 {
		t.Errorf("add_volume: %d", tr.Ret)
		t.FailNow()
	}
	buf.Reset()
	buf.WriteString("vid=1")
	if resp, err = http.Post("http://localhost:6063/compact_volume", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post() error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll() error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, tr); err != nil {
		t.Errorf("json.Unmarshal() error(%v)", err)
		t.FailNow()
	}
	if tr.Ret != 1 {
		t.Errorf("compact_volume: %d", tr.Ret)
		t.FailNow()
	}
	buf.Reset()
	buf.WriteString("vid=2&bfile=./test/block_admin_1&ifile=./test/block_admin_1.idx")
	if resp, err = http.Post("http://localhost:6063/bulk_volume", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post() error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll() error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, tr); err != nil {
		t.Errorf("json.Unmarshal() error(%v)", err)
		t.FailNow()
	}
	if tr.Ret != 1 {
		t.Errorf("bulk_volume: %d", tr.Ret)
		t.FailNow()
	}
	time.Sleep(2 * time.Second)
}
