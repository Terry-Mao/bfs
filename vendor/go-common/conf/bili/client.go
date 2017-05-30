package bili

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	// code
	_codeOk       = 0
	_codeSrvError = -500
	_codeNoModify = -304
	// api url
	_apiGet     = "http://%s/api/config/file?%s"
	_apiPulling = "http://%s/api/polling?%s"
	// file suffix
	_fileTmpSuffix = ".tmp"
)

var (
	conf config
)

func init() {
	parseEnv()

	hostname, _ := os.Hostname()
	flag.StringVar(&conf.App, "conf_appid", conf.App, `app name.`)
	flag.StringVar(&conf.Version, "conf_version", conf.Version, `app version.`)
	flag.StringVar(&conf.Apihost, "conf_host", conf.Apihost, `config center api host.`)
	flag.StringVar(&conf.Env, "conf_env", conf.Env, `run env of server.`)
	flag.StringVar(&conf.Hostname, "conf_hostname", hostname, `hostname.`)
	flag.StringVar(&conf.Path, "conf_path", conf.Path, `config file path.`)
	flag.StringVar(&conf.FileName, "conf_filename", conf.FileName, `config file name.`)
}

func parseEnv() {
	conf.App = os.Getenv("CONF_APPID")
	conf.Version = os.Getenv("CONF_VERSION")
	conf.Apihost = os.Getenv("CONF_HOST")
	conf.Env = os.Getenv("CONF_ENV")
	conf.Hostname = os.Getenv("CONF_HOSTNAME")
	conf.Path = os.Getenv("CONF_PATH")
	conf.FileName = os.Getenv("CONF_FILENAME")
}

type Client struct {
	// conf    *Config
	getAll  bool
	err     chan error
	httpCli *http.Client
	// notifyFn func(configFile string)
}

type config struct {
	Apihost      string
	App          string
	Version      string
	Env          string
	Path         string
	Hostname     string
	FileName     string
	IntervalTime time.Duration
}

type api struct {
	Code int     `json:"code"`
	Ts   int     `json:"ts"`
	Msg  string  `json:"message"`
	Data []*data `json:"data"`
}

type data struct {
	App     string `json:"app"`
	Version string `json:"version"`
	Env     string `json:"env"`
	Key     string `json:"key"`
}

func New() (c *Client, filepath string, err error) {
	if conf.App == "" || conf.Version == "" || conf.Hostname == "" || conf.Apihost == "" || conf.Env == "" || conf.Path == "" || conf.FileName == "" {
		err = fmt.Errorf("at least one params is empty. app=%s, version=%s, hostname=%s, api=%s, env=%s, path=%s, filename=%s",
			conf.App, conf.Version, conf.Hostname, conf.Apihost, conf.Env, conf.Path, conf.FileName)
		return
	}
	conf.IntervalTime = time.Duration(time.Second * 30)
	c = &Client{
		getAll: true,
		err:    make(chan error, 256),
		httpCli: &http.Client{
			Timeout: time.Second * 5,
		},
	}
	// deal with the end symbol "/"
	if !strings.HasSuffix(conf.Path, "/") {
		conf.Path += "/"
	}
	// init try 3 times
	initReTry := 0
	filepath = conf.Path + conf.FileName
	for {
		if err = c.polling(); err != nil {
			if initReTry >= 3 {
				// fail 3 times, use local config file
				if conf.FileName != "" {
					// keep origin err info, so use ferr
					if _, ferr := os.Stat(filepath); ferr == nil {
						err = nil
					}
				}
				return
			}
			initReTry++
		} else {
			break
		}
	}
	// c.notifyFn = fn
	go c.doPolling()
	return
}

func (c *Client) doPolling() {
	for {
		if err := c.polling(); err != nil {
			c.setChan(err)
		}
		time.Sleep(conf.IntervalTime)
	}
}

func (c *Client) polling() (err error) {
	var (
		url  string
		req  *http.Request
		resp *http.Response
		rb   []byte
	)
	// make api url
	if url = c.makeUrl(_apiPulling, nil); url == "" {
		err = fmt.Errorf("polling c.makeUrl() error, url empty")
		return
	}
	// http
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return
	}
	req.Header.Add("hostname", conf.Hostname)
	if resp, err = c.httpCli.Do(req); err != nil {
		return
	}
	// ok
	if resp.StatusCode == http.StatusOK {
		if rb, err = ioutil.ReadAll(resp.Body); err != nil {
			return
		}
		defer resp.Body.Close()
		r := &api{}
		if err = json.Unmarshal(rb, r); err != nil {
			return
		}
		switch r.Code {
		case _codeOk:
			if len(r.Data) > 0 {
				isGetAll := false
				for _, v := range r.Data {
					if err = c.down(v); err != nil {
						isGetAll = true
					}
				}
				c.getAll = isGetAll
			} else {
				err = fmt.Errorf("polling response error code: %d, err msg: response form error", r.Code)
			}
		case _codeNoModify:
		case _codeSrvError:
			err = fmt.Errorf("polling response error code: %d, err msg: %s", r.Code, r.Msg)
		default:
			err = fmt.Errorf("polling response error code: %d, err msg: %s", r.Code, r.Msg)
		}
	} else {
		err = fmt.Errorf("polling http error url(%s) status: %v", url, resp.StatusCode)
	}
	return
}

func (c *Client) down(data *data) (err error) {
	var (
		url      string
		rb       []byte
		resp     *http.Response
		confFile = conf.Path + data.Key
		tmpFile  = confFile + _fileTmpSuffix
		bakFile  = fmt.Sprintf("%s%v-%s", conf.Path, time.Now().Format("2006_01_02"), data.Key)
	)
	// make api url
	if url = c.makeUrl(_apiGet, data); url == "" {
		err = fmt.Errorf("api/config/file c.makeUrl() error, url empty")
		return
	}
	// http
	if resp, err = c.httpCli.Get(url); err != nil {
		return
	}
	if resp.StatusCode == http.StatusOK {
		if rb, err = ioutil.ReadAll(resp.Body); err != nil {
			return
		}
		defer resp.Body.Close()
		// write conf
		if err = ioutil.WriteFile(tmpFile, rb, 0644); err != nil {
			return
		}
		// ex. 2006-01-02_name.bak
		if err = os.Rename(confFile, bakFile); err != nil {
			err = nil
		}
		// new conf
		if err = os.Rename(tmpFile, confFile); err != nil {
			return
		}
		// notify
		// if c.notifyFn != nil {
		// 	c.notifyFn(confFile)
		// }
	} else {
		err = fmt.Errorf("down http error status: %d", resp.StatusCode)
	}
	return
}

func (c *Client) makeUrl(api string, data *data) (query string) {
	params := url.Values{}
	if api == _apiPulling {
		params.Set("app", conf.App)
		params.Set("version", conf.Version)
		params.Set("env", conf.Env)
		if c.getAll {
			params.Set("isFirstReq", "0")
		}
	} else if api == _apiGet {
		params.Set("app", data.App)
		params.Set("version", data.Version)
		params.Set("env", data.Env)
		params.Set("key", data.Key)
	}
	query = fmt.Sprintf(api, conf.Apihost, params.Encode())
	return
}

func (c *Client) setChan(err error) {
	select {
	case c.err <- err:
	default:
	}
}

func (c *Client) Error() <-chan error {
	return c.err
}
