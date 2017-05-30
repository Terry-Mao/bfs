package conf

import (
	"fmt"

	xtime "go-common/time"
)

// Common common
type Common struct {
	Version   string
	User      string
	Pid       string
	Dir       string
	Perf      string
	CheckFile string
	Log       string
	Trace     bool
	Debug     bool
	// for trace
	Family string
}

// App bilibili intranet authorization.
type App struct {
	Key    string
	Secret string
}

// Consul consul config.
type Consul struct {
	Family      string
	ConsulAddr  string
	ServicePort int
	Timeout     xtime.Duration
}

// Breaker broker config.
type Breaker struct {
	Window  xtime.Duration
	Sleep   xtime.Duration
	Bucket  int
	Ratio   float32
	Request uint64
}

// =================================== RPC ===================================

// RPCServer rpc server settings.
type RPCServer struct {
	Proto  string
	Addr   string
	Group  string
	Weight int // weight of rpc server and also means num of client connections.
}

// Key rpc string.
func (s *RPCServer) Key() string {
	return fmt.Sprintf("%s@%s", s.Proto, s.Addr)
}

// RPCServers multiple rpc servers settings.
type RPCServers []*RPCServer

// RPCClient net/rpc client settings.
type RPCClient struct {
	Proto   string
	Addr    string
	Token   string
	Timeout xtime.Duration
	Breaker *Breaker
}

// RPCClients multiple rpc clients settings.
type RPCClients []*RPCClient

// RPCServer2 net/rpc service discover server settings.
type RPCServer2 struct {
	DiscoverOff bool
	Token       string
	Servers     []*RPCServer
	Zookeeper   *Zookeeper
}

// RPCClient2 net/rpc service discover client settings.
type RPCClient2 struct {
	Policy       string
	Group        string
	Client       *RPCClient
	Backup       *RPCClients
	Zookeeper    *Zookeeper
	PullInterval xtime.Duration
}

// ThriftClient thrift client settings.
type ThriftClient struct {
	Addr        string
	Active      int
	Idle        int
	DialTimeout xtime.Duration
	IdleTimeout xtime.Duration
}

// =================================== RPC ===================================

// =================================== HTTP ==================================

// HTTPServer http server settings.
type HTTPServer struct {
	Addrs        []string
	MaxListen    int32
	ReadTimeout  xtime.Duration
	WriteTimeout xtime.Duration
}

// HTTPClient http client settings.
type HTTPClient struct {
	Dial      xtime.Duration
	Timeout   xtime.Duration
	KeepAlive xtime.Duration
	Breaker   *Breaker
}

// MultiHTTP outer/inner/local http server settings.
type MultiHTTP struct {
	Outer *HTTPServer
	Inner *HTTPServer
	Local *HTTPServer
}

// =================================== HTTP ==================================

// =================================== MySQL =================================

// MySQL config.
type MySQL struct {
	Addr   string // for trace
	DSN    string // data source name
	Active int    // pool
	Idle   int    // pool
}

// =================================== MySQL =================================

// ================================== CACHE ==================================

// Redis client settings.
type Redis struct {
	Name         string // redis name, for trace
	Proto        string
	Addr         string
	Auth         string
	Active       int // pool
	Idle         int // pool
	DialTimeout  xtime.Duration
	ReadTimeout  xtime.Duration
	WriteTimeout xtime.Duration
	IdleTimeout  xtime.Duration
}

// Memcache client settings.
type Memcache struct {
	Name         string // memcache name, for trace
	Proto        string
	Addr         string
	Active       int // pool
	Idle         int // pool
	DialTimeout  xtime.Duration
	ReadTimeout  xtime.Duration
	WriteTimeout xtime.Duration
	IdleTimeout  xtime.Duration
}

// ================================== CACHE ==================================

// =================================== ID ====================================

// Snowflake2 client settings.
// with zookeeper
type Snowflake2 struct {
	Zookeeper *Zookeeper
	WorkerIDs []int64
}

// Snowflake local
type Snowflake struct {
	DatacenterID int64
	WorkerID     int64
	Twepoch      int64 // usually 1288834974657
}

// =================================== ID ====================================

// ================================== Kafka ==================================

// KafkaProducer kafka producer settings.
type KafkaProducer struct {
	Zookeeper *Zookeeper
	Brokers   []string
	Sync      bool // true: sync, false: async
}

// KafkaConsumer kafka client settings.
type KafkaConsumer struct {
	Monitor   *HTTPServer // Consumer Ping Addr
	Group     string
	Topics    []string
	Offset    bool // true: new, false: old
	Zookeeper *Zookeeper
}

// ================================== Kafka ==================================

// ================================== HBase ==================================

// HBase config.
type HBase struct {
	Zookeeper *Zookeeper
	// default "" means use default hbase zk path. It should correspond to server config
	Master        string
	Meta          string
	TestRowKey    string         // should used service-specific name like 'account-service' and it's preferred to have it with server info like 'account-service-172.16.11.11'
	DialTimeout   xtime.Duration // 0 means no dial timeout
	ReadTimeout   xtime.Duration
	ReadsTimeout  xtime.Duration
	WriteTimeout  xtime.Duration
	WritesTimeout xtime.Duration
}

// ================================== HBase ==================================

// ================================== Databus ================================

// Databus config
type Databus struct {
	Key    string
	Secret string
	Group  string
	Topic  string
	Action string // shoule be "pub" or "sub" or "pubsub"
	Offset string // should be "new" or "old"
	Buffer int
	Redis  *Redis
}

// ================================== Databus ================================

// ================================== Trace ==================================

// Tracer config
type Tracer struct {
	Proto string
	Addr  string
	Tag   string
}

// ================================== Trace ==================================

// ================================ Zookeeper ================================

// Zookeeper Server&Client settings.
type Zookeeper struct {
	Root    string
	Addrs   []string
	Timeout xtime.Duration
}

// ================================ Zookeeper ================================

// ================================ Ecode ================================

// Ecode encode
type Ecode struct {
	Service string
	MySQL   *MySQL
}

// ================================ Ecode ================================
