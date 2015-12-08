package main

import (
	"testing"
	"time"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"bytes"
//	"fmt"
)


func TestHTTPAPI(t *testing.T) {
    var (
            err        error
            config     *Config
            zk         *Zookeeper
            d          *Directory
            body       []byte
            resp       *http.Response
            dataJson   map[string]interface{}
            buf        = &bytes.Buffer{}
    )
    if config, err = NewConfig("./directory.conf"); err != nil {
        t.Errorf("NewConfig() error(%v)", err)
        t.FailNow()
    }

    if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack", "/volume", "/group"); err != nil {
        t.Errorf("NewZookeeper() error(%v)", err)
        t.FailNow()
    }
    if d, err = NewDirectory(config, zk); err != nil {
        t.Errorf("NewDirectory() error(%v)", err)
        t.FailNow()
    }
    StartApi(config.ApiListen, d)
    buf.Reset()
    buf.WriteString("num=1")
    if resp, err = http.Post("http://localhost:6065/upload", "application/x-www-form-urlencoded", buf); err != nil {
    	t.Errorf("http.Post error(%v)", err)
    	t.FailNow()
    }
    defer resp.Body.Close()
    if body, err = ioutil.ReadAll(resp.Body); err != nil {
    	t.Errorf("ioutil.ReadAll error(%v)", err)
    	t.FailNow()
    }
    if err = json.Unmarshal(body, &dataJson); err != nil {
    	t.Errorf("json.Unmarshal error(%v)", err)
    	t.FailNow()
    }

}
