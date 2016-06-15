package main

import (
	"bfs/store/zk"
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
		s    *Store
		z    *zk.Zookeeper
		resp *http.Response
		body []byte
		err  error
		buf  = &bytes.Buffer{}
		tr   = &testRet{}
	)
	os.Remove(testConf.Store.VolumeIndex)
	os.Remove(testConf.Store.FreeVolumeIndex)
	os.Remove("./test/_free_block_1")
	os.Remove("./test/_free_block_1.idx")
	os.Remove("./test/_free_block_2")
	os.Remove("./test/_free_block_2.idx")
	os.Remove("./test/_free_block_3")
	os.Remove("./test/_free_block_3.idx")
	os.Remove("./test/1_0")
	os.Remove("./test/1_1")
	os.Remove("./test/block_admin_1")
	os.Remove("./test/block_admin_1.idx")
	defer os.Remove(testConf.Store.VolumeIndex)
	defer os.Remove(testConf.Store.FreeVolumeIndex)
	defer os.Remove("./test/_free_block_1")
	defer os.Remove("./test/_free_block_1.idx")
	defer os.Remove("./test/_free_block_2")
	defer os.Remove("./test/_free_block_2.idx")
	defer os.Remove("./test/_free_block_3")
	defer os.Remove("./test/_free_block_3.idx")
	defer os.Remove("./test/1_0")
	defer os.Remove("./test/1_1")
	defer os.Remove("./test/block_admin_1")
	defer os.Remove("./test/block_admin_1.idx")
	if z, err = zk.NewZookeeper(testConf); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	defer z.Close()
	z.DelVolume(1)
	z.DelVolume(2)
	z.DelVolume(3)
	defer z.DelVolume(1)
	defer z.DelVolume(2)
	defer z.DelVolume(3)
	if s, err = NewStore(testConf); err != nil {
		t.Errorf("NewStore() error(%v)", err)
		t.FailNow()
	}
	defer s.Close()
	StartAdmin("localhost:6063", &Server{store: s, conf: testConf})
	time.Sleep(1 * time.Second)
	// AddFreeVolume
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
	// AddVolume
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
	// CompactVolume
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
	time.Sleep(_compactSleep * 2)
	// BulkVolume
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
