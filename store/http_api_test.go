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

func TestHTTPAPI(t *testing.T) {
	// get volume
	// upload
	// uploads
	// delete
	// deletes
	var (
		s     *Store
		z     *Zookeeper
		resp  *http.Response
		body  []byte
		err   error
		buf   = &bytes.Buffer{}
		tr    = &testRet{}
		file  = "./test/store_api.idx"
		bfile = "./test/block_api_1"
		ifile = "./test/block_api_1.idx"
	)
	os.Remove(file)
	os.Remove(bfile)
	os.Remove(ifile)
	defer os.Remove(file)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	t.Log("NewStore()")
	if z, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack/test-api/"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	z.DelVolume(1)
	if s, err = NewStore(z, file); err != nil {
		t.Errorf("NewStore() error(%v)", err)
		t.FailNow()

	}
	defer s.Close()
	StartAdmin(s, "localhost:6064")
	time.Sleep(1 * time.Second)
	t.Log("AddFreeVolume()")
	buf.Reset()
	buf.WriteString("n=1&bdir=./test/&idir=./test/")
	if resp, err = http.Post("http://localhost:6064/add_free_volume", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post() error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll() error(%v)", err)
		t.FailNow()
	}
	t.Logf("%s", body)
	if err = json.Unmarshal(body, tr); err != nil {
		t.Errorf("json.Unmarshal() error(%v)", err)
		t.FailNow()
	}
	if tr.Ret != 1 {
		t.FailNow()
	}
	t.Log("AddVolume()")
	buf.Reset()
	buf.WriteString("vid=1")
	if resp, err = http.Post("http://localhost:6064/add_volume", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post() error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll() error(%v)", err)
		t.FailNow()
	}
	t.Logf("%s", body)
	if err = json.Unmarshal(body, tr); err != nil {
		t.Errorf("json.Unmarshal() error(%v)", err)
		t.FailNow()
	}
	if tr.Ret != 1 {
		t.FailNow()
	}
	t.Log("Upload")
	t.Log("Uploads")
	t.Log("Get")
	t.Log("Delete")
	t.Log("Deletes")
}
