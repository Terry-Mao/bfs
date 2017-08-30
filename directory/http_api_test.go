package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"bfs/directory/conf"
	dzk "bfs/directory/zk"
	"bfs/libs/meta"
)

func TestHTTPAPI(t *testing.T) {
	var (
		err    error
		config *conf.Config
		zk     *dzk.Zookeeper
		d      *Directory
		key    int64
		cookie int32
		body   []byte
		url    string
		resp   *http.Response
		res    meta.Response
		buf    = &bytes.Buffer{}
	)
	if config, err = conf.NewConfig("./directory.toml"); err != nil {
		t.Errorf("NewConfig() error(%v)", err)
		t.FailNow()
	}

	if zk, err = dzk.NewZookeeper(config); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	defer zk.Close()
	if d, err = NewDirectory(config); err != nil {
		t.Errorf("NewDirectory() error(%v)", err)
		t.FailNow()
	}
	StartApi(config.ApiListen, d)
	time.Sleep(1 * time.Second)
	buf.Reset()
	buf.WriteString("num=1")
	if resp, err = http.Post("http://172.16.13.86:6065/upload", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post error(%v)", err)
		t.FailNow()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("http ERROR")
		t.FailNow()
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, &res); err != nil {
		t.Errorf("json.Unmarshal error(%v)", err)
		t.FailNow()
	}
	key = res.Key
	cookie = res.Cookie
	fmt.Println("put vid:", res.Vid)
	buf.Reset()
	url = fmt.Sprintf("http://172.16.13.86:6065/get?key=%d&cookie=%d", key, cookie)
	if resp, err = http.Get(url); err != nil {
		t.Errorf("http ERROR error(%v)", err)
		t.FailNow()
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("http ERROR")
		t.FailNow()
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, &res); err != nil {
		t.Errorf("json.Unmarshal error(%v)", err)
		t.FailNow()
	}
	fmt.Println("get vid:", res.Vid)
	buf.Reset()
	buf.WriteString(fmt.Sprintf("key=%d&cookie=%d", key, cookie))
	if resp, err = http.Post("http://172.16.13.86:6065/del", "application/x-www-form-urlencoded", buf); err != nil {
		t.Errorf("http.Post error(%v)", err)
		t.FailNow()
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("http ERROR")
		t.FailNow()
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Errorf("ioutil.ReadAll error(%v)", err)
		t.FailNow()
	}
	if err = json.Unmarshal(body, &res); err != nil {
		t.Errorf("json.Unmarshal error(%v)", err)
		t.FailNow()
	}
	fmt.Println("del vid", res.Vid)
}
