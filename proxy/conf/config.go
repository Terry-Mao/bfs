package conf

import (
	"bfs/libs/memcache"
	"bfs/libs/time"
	"path"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	PprofEnable bool
	PprofListen string

	// api
	HttpAddr string
	// directory
	BfsAddr string
	// download domain
	Domain string
	// location prefix
	Prefix string
	// file
	MaxFileSize int
	// aliyun
	AliyunKeyId     string
	AliyunKeySecret string
	// netcenter
	NetUserName string
	NetPasswd   string
	// qcloud
	QcloudKeyID     string
	QcloudKeySecret string
	// ats server list
	Ats *Ats
	// purge channel
	PurgeMaxSize int
	// memcache
	ExpireMc time.Duration
	Mc       *memcache.Config
	// limit rate
	Limit *Limit
}

type Ats struct {
	AtsServerList []string
}

// Limit limit rate
type Limit struct {
	Rate  float64
	Brust int
}

// NewConfig new a config.
func NewConfig(conf string) (c *Config, err error) {
	c = new(Config)
	if _, err = toml.DecodeFile(conf, c); err != nil {
		return
	}
	// bfs,/bfs,/bfs/ convert to /bfs/
	if c.Prefix != "" {
		c.Prefix = path.Join("/", c.Prefix) + "/"
		// http://domain/ covert to http://domain
		c.Domain = strings.TrimRight(c.Domain, "/")
	}
	return
}
