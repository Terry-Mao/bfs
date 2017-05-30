package ugc

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync/atomic"
	"time"

	"go-common/log"
)

const (
	// code
	_codeOk           = 0
	_codeNotModified  = -304
	_codeNothingFound = -404
	// api
	_apiGet        = "http://%s/v1/config/get?%s"
	_apiCheck      = "http://%s/v1/config/check?%s"
	_fileVerSuffix = ".ver"
	// timeout
	_retryInterval  = 1 * time.Second
	_httpTimeout    = 60 * time.Second
	_unknownVersion = -1
)

var (
	conf config
)

type version struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    *struct {
		Version int64 `json:"version"`
	} `json:"data"`
}

type result struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    *data  `json:"data"`
}

type data struct {
	Version int64  `json:"version"`
	Content string `json:"content"`
	Md5     string `json:"md5"`
}

type config struct {
	Svr      string
	Ver      string
	Path     string
	Filename string
	Host     string
	Addr     string
	Env      string
	Token    string
}

// Client is config client.
type Client struct {
	ver     int64
	httpCli *http.Client
	values  atomic.Value
	event   chan struct{}
}

func init() {
	// env
	conf.Svr = os.Getenv("CONF_APPID")
	conf.Ver = os.Getenv("CONF_VERSION")
	conf.Addr = os.Getenv("CONF_HOST")
	conf.Host = os.Getenv("CONF_HOSTNAME")
	conf.Path = os.Getenv("CONF_PATH")
	conf.Env = os.Getenv("CONF_ENV")
	conf.Token = os.Getenv("CONF_TOKEN")
	// flags
	hostname, _ := os.Hostname()
	flag.StringVar(&conf.Svr, "conf_appid", conf.Svr, `app name.`)
	flag.StringVar(&conf.Ver, "conf_version", conf.Ver, `app version.`)
	flag.StringVar(&conf.Addr, "conf_host", conf.Addr, `config center api host.`)
	flag.StringVar(&conf.Host, "conf_hostname", hostname, `hostname.`)
	flag.StringVar(&conf.Path, "conf_path", conf.Path, `config file path.`)
	flag.StringVar(&conf.Env, "conf_env", conf.Env, `config Env.`)
	flag.StringVar(&conf.Token, "conf_token", conf.Token, `config Token.`)

}

// New new a ugc config center client.
func New() (c *Client, err error) {
	if conf.Svr == "" || conf.Host == "" || conf.Path == "" || conf.Addr == "" || conf.Ver == "" || conf.Env == "" || conf.Token == "" {
		err = fmt.Errorf("at least one params is empty. app=%s, version=%s, hostname=%s, addr=%s, path=%s, Env=%s, Token =%s", conf.Svr, conf.Ver, conf.Host, conf.Addr, conf.Path, conf.Env, conf.Token)
		return
	}
	c = &Client{
		httpCli: &http.Client{Timeout: _httpTimeout},
		event:   make(chan struct{}, 10),
	}
	c.values.Store(make(map[string]string))
	if err = c.init(); err != nil {
		return
	}
	go c.updateproc()
	return
}

//Path get confFile Path
func (c *Client) Path() string {
	return conf.Path
}

// Value return config value.
func (c *Client) Value(key string) (string, bool) {
	m, ok := c.values.Load().(map[string]string)
	if !ok {
		return "", false
	}
	v, ok := m[key]
	return v, ok
}

// Event client update event.
func (c *Client) Event() <-chan struct{} {
	return c.event
}

// checkLocal check local config is ok
func (c *Client) init() (err error) {
	var ver int64
	if ver, err = c.checkVersion(_unknownVersion); err != nil {
		fmt.Printf("get remote version error(%v)\n", err)
		return
	}
	for i := 0; i < 3; i++ {
		if ver == _unknownVersion {
			fmt.Println("get null version")
			return
		}
		if err = c.download(ver); err == nil {
			return
		}
		fmt.Printf("retry times: %d, c.download() error(%v)\n", i, err)
		time.Sleep(_retryInterval)
	}
	return
}

func (c *Client) updateproc() (err error) {
	var ver int64
	for {
		time.Sleep(_retryInterval)
		if ver, err = c.checkVersion(c.ver); err != nil {
			log.Error("c.checkVersion(%d) error(%v)", c.ver, err)
			continue
		} else if ver == c.ver {
			continue
		}
		if err = c.download(ver); err != nil {
			log.Error("c.download() error(%s)", err)
			continue
		}
		c.event <- struct{}{}
	}
}

// download download config from config service
func (c *Client) download(ver int64) (err error) {
	var data *data
	if data, err = c.getConfig(ver); err != nil {
		return
	}
	return c.update(data)
}

// poll config server
func (c *Client) checkVersion(reqVer int64) (ver int64, err error) {
	var (
		url  string
		req  *http.Request
		resp *http.Response
		rb   []byte
	)
	if url = c.makeURL(_apiCheck, reqVer); url == "" {
		err = fmt.Errorf("checkVersion() c.makeUrl() error url empty")
		return
	}
	// http
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return
	}
	if resp, err = c.httpCli.Do(req); err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("checkVersion() http error url(%s) status: %d", url, resp.StatusCode)
		return
	}
	// ok
	if rb, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	defer resp.Body.Close()
	v := &version{}
	if err = json.Unmarshal(rb, v); err != nil {
		return
	}
	switch v.Code {
	case _codeOk:
		if v.Data == nil {
			err = fmt.Errorf("checkVersion() response error result: %v", v)
			return
		}
		ver = v.Data.Version
	case _codeNotModified:
		ver = reqVer
	default:
		err = fmt.Errorf("checkVersion() response error result: %v", v)
	}
	return
}

// updateVersion update config version
func (c *Client) getConfig(ver int64) (data *data, err error) {
	var (
		url  string
		req  *http.Request
		resp *http.Response
		rb   []byte
		res  = &result{}
	)
	if url = c.makeURL(_apiGet, ver); url == "" {
		err = fmt.Errorf("getConfig() c.makeUrl() error url empty")
		return
	}
	// http
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return
	}
	if resp, err = c.httpCli.Do(req); err != nil {
		return
	}
	// ok
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("getConfig() http error url(%s) status: %d", url, resp.StatusCode)
		return
	}
	if rb, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	defer resp.Body.Close()
	if err = json.Unmarshal(rb, res); err != nil {
		return
	}
	switch res.Code {
	case _codeOk:
		// has new config
		if res.Data == nil {
			err = fmt.Errorf("getConfig() response error result: %v", res)
			return
		}
		data = res.Data
	default:
		err = fmt.Errorf("getConfig() response error result: %v", res)
	}
	return
}

// update write config
func (c *Client) update(d *data) (err error) {
	var (
		bs  = []byte(d.Content)
		tmp = make(map[string]string)
	)
	// md5 file
	if mh := md5.Sum(bs); hex.EncodeToString(mh[:]) != d.Md5 {
		err = fmt.Errorf("md5 mismatch, local:%s, remote:%s", hex.EncodeToString(mh[:]), d.Md5)
		return
	}
	// write conf
	if err = json.Unmarshal(bs, &tmp); err != nil {
		return
	}
	for key, value := range tmp {
		if err = ioutil.WriteFile(path.Join(conf.Path, key), []byte(value), 0644); err != nil {
			return
		}
	}
	// update current version
	c.ver = d.Version
	c.values.Store(tmp)
	return
}

// makeUrl signed url
func (c *Client) makeURL(api string, ver int64) (query string) {
	params := url.Values{}
	// service
	params.Set("service", conf.Svr)
	params.Set("hostname", conf.Host)
	params.Set("build", conf.Ver)
	params.Set("version", fmt.Sprint(ver))
	params.Set("ip", localIP())
	params.Set("environment", conf.Env)
	params.Set("token", conf.Token)
	// api
	query = fmt.Sprintf(api, conf.Addr, params.Encode())
	return
}

// localIP return local IP of the host.
func localIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
