package conf

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

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
}

// Code to implement the TextUnmarshaler interface for `duration`:
type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// NewConfig new a config.
func NewConfig(conf string) (c *Config, err error) {
	var (
		file *os.File
		blob []byte
	)
	c = new(Config)
	if file, err = os.Open(conf); err != nil {
		return
	}
	if blob, err = ioutil.ReadAll(file); err != nil {
		return
	}
	if err = toml.Unmarshal(blob, c); err != nil {
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
