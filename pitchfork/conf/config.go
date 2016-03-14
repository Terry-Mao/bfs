package conf

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"os"
	"time"
)

type Config struct {
	Store     *Store
	Zookeeper *Zookeeper
}

type Store struct {
	StoreCheckInterval  duration
	NeedleCheckInterval duration
	RackCheckInterval   duration
}

type Zookeeper struct {
	VolumeRoot    string
	StoreRoot     string
	PitchforkRoot string
	Addrs         []string
	Timeout       duration
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
