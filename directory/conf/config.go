package conf

import (
	"io/ioutil"
	"os"
	"time"

	xtime "bfs/libs/time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Snowflake *Snowflake
	Zookeeper *Zookeeper
	HBase     *HBase

	MaxNum      int
	ApiListen   string
	PprofEnable bool
	PprofListen string
}

type Snowflake struct {
	ZkAddrs   []string
	ZkTimeout duration
	ZkPath    string
	WorkId    int64
}

type Zookeeper struct {
	Addrs        []string
	Timeout      duration
	PullInterval duration
	VolumeRoot   string
	StoreRoot    string
	GroupRoot    string
}

// HBase config.
type HBase struct {
	ZookeeperHbase *ZookeeperHbase
	// default "" means use default hbase zk path. It should correspond to server config
	Master        string
	Meta          string
	TestRowKey    string
	DialTimeout   xtime.Duration // 0 means no dial timeout
	ReadTimeout   xtime.Duration
	ReadsTimeout  xtime.Duration
	WriteTimeout  xtime.Duration
	WritesTimeout xtime.Duration
}

type ZookeeperHbase struct {
	Root    string
	Addrs   []string
	Timeout xtime.Duration
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
