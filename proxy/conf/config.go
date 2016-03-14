package conf

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"os"
	"time"
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
	// file
	MaxFileSize int
	// aliyun
	AliyunKeyId     string
	AliyunKeySecret string
	// netcenter
	NetUserName string
	NetPasswd   string
	// purge channel
	PurgeMaxSize int
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
	err = toml.Unmarshal(blob, c)
	return
}
