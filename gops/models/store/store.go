package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"github.com/astaxie/beego"
	"strconv"
)

const (
	_storeAddFreeVolumeApi = "http://%s/add_free_volume"
	_storeAddVolumeApi     = "http://%s/add_volume"
	_storeCompactVolumeApi = "http://%s/compact_volume"
	_storeBulkVolumeApi    = "http://%s/bulk_volume"


)

var (
	httpCodeErr = errors.New("rtore api return http statuscode error.")
	paramsErr   = errors.New("store api return  params error.")
	internalErr = errors.New("store api return  internal error.")
)


type StoreResp struct {
	Ret int
}

type Store struct {
	domain string
}

func New()(store *Store,err error){
	var (
		domain string
	)

	domain = beego.AppConfig.String("store:domain")
	store = &Store{
		domain:domain,
	}

	return
}

func (s *Store) AddFreeVolume(host string, n int32, bdir, idir string) (err error) {
	var (
		uri    string
		params url.Values
		body   []byte
		resp   *StoreResp
	)

	uri = fmt.Sprintf(_storeAddFreeVolumeApi, host)
	params = url.Values{}
	params.Set("n", strconv.FormatInt(int64(n),10))
	params.Set("bdir", bdir)
	params.Set("idir", idir)

	if body, err = httpPost(uri, params); err != nil {
		return
	}

	resp = new(StoreResp)
	if err = json.Unmarshal(body, resp); err != nil {
		return
	}

	if resp.Ret == 65534 {
		return paramsErr
	} else if resp.Ret == 65535 {
		return internalErr
	}

	return
}

func (s *Store) AddVolume(host string,vid uint64) (err error) {
	var (
		uri    string
		params url.Values
		body   []byte
		resp   *StoreResp
	)

	uri = fmt.Sprintf(_storeAddVolumeApi, host)
	params = url.Values{}
	params.Set("vid", strconv.FormatUint(uint64(vid),10))

	if body, err = httpPost(uri, params); err != nil {
		return
	}

	resp = new(StoreResp)
	if err = json.Unmarshal(body, resp); err != nil {
		return
	}

	if resp.Ret == 65534 {
		return paramsErr
	} else if resp.Ret == 65535 {
		return internalErr
	}

	return
}

func (s *Store) CompactVolume(vid uint32) (err error) {
	return
}

func (s *Store) BulkVolume(vid uint32) (err error) {
	return
}

func httpPost(url string, data url.Values) (body []byte, err error) {
	var (
		resp *http.Response
	)

	resp, err = http.PostForm(url, data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, httpCodeErr
	}

	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	return
}
