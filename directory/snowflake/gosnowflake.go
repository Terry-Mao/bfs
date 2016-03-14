// Copyright © 2014 Terry Mao All rights reserved.
// This file is part of gosnowflake.

// gosnowflake is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// gosnowflake is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with gosnowflake.  If not, see <http://www.gnu.org/licenses/>.

package snowflake

import (
	log "github.com/alecthomas/log4go"
	"encoding/json"
	"errors"
	myrpc "github.com/Terry-Mao/gosnowflake/rpc"
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"net/rpc"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	zkNodeDelaySleep    = 1 * time.Second // zk error delay sleep
	zkNodeDelayChild    = 3 * time.Second // zk node delay get children
	rpcClientPingSleep  = 1 * time.Second // rpc client ping need sleep
	rpcClientRetrySleep = 1 * time.Second // rpc client retry connect need sleep

	RPCPing    = "SnowflakeRPC.Ping"
	RPCNextId  = "SnowflakeRPC.NextId"
	RPCNextIds = "SnowflakeRPC.NextIds"
)

var (
	ErrNoRpcClient = errors.New("rpc: no rpc client service")
	// zk
	mutex     sync.Mutex
	zkConn    *zk.Conn
	zkPath    string
	zkServers []string
	zkTimeout time.Duration
	// worker
	workerIdMap = map[int64]*Client{}
)

// Init init the gosnowflake client.
func Init(zservers []string, zpath string, ztimeout time.Duration) (err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if zkConn != nil {
		return
	}
	zkPath = zpath
	zkServers = zservers
	zkTimeout = ztimeout
	conn, session, err := zk.Connect(zkServers, zkTimeout)
	if err != nil {
		log.Error("zk.Connect(\"%v\", %d) error(%v)", zkServers, zkTimeout, err)
		return
	}
	zkConn = conn
	go func() {
		for {
			event := <-session
			log.Info("zk connect get a event: %s", event.Type.String())
		}
	}()
	return
}

// Client is gosnowfalke client.
type Client struct {
	workerId int64
	clients  []*rpc.Client // key is workerId
	stop     chan bool
	leader   string
}

// Peer store data in zookeeper.
type Peer struct {
	RPC    []string `json:"rpc"`
	Thrift []string `json:"thrift"`
}

// NewClient new a gosnowfalke client.
func NewClient(workerId int64) (c *Client) {
	var ok bool
	mutex.Lock()
	defer mutex.Unlock()
	if c, ok = workerIdMap[workerId]; ok {
		return
	}
	c = &Client{
		workerId: workerId,
		clients:  nil,
		leader:   "",
	}
	go c.watchWorkerId(workerId, strconv.FormatInt(workerId, 10))
	workerIdMap[workerId] = c
	return
}

// Id generate a snowflake id.
func (c *Client) Id() (id int64, err error) {
	client, err := c.client()
	if err != nil {
		return
	}
	if err = client.Call(RPCNextId, c.workerId, &id); err != nil {
		log.Error("rpc.Call(\"%s\", %d, &id) error(%v)", RPCNextId, c.workerId, err)
	}
	return
}

// Ids generate a snowflake id.
func (c *Client) Ids(num int) (ids []int64, err error) {
	client, err := c.client()
	if err != nil {
		return
	}
	if err = client.Call(RPCNextIds, &myrpc.NextIdsArgs{WorkerId: c.workerId, Num: num}, &ids); err != nil {
		log.Error("rpc.Call(\"%s\", %d, &id) error(%v)", RPCNextId, c.workerId, err)
	}
	return
}

// closeRpc close rpc resource.
func closeRpc(clients []*rpc.Client, stop chan bool) {
	// rpc
	for _, client := range clients {
		if client != nil {
			if err := client.Close(); err != nil {
				log.Error("client.Close() error(%v)", err)
			}
		}
	}
	// ping&retry goroutine
	if stop != nil {
		close(stop)
	}
}

// Close destroy the client from global client cache.
func (c *Client) Close() {
	closeRpc(c.clients, c.stop)
	mutex.Lock()
	defer mutex.Unlock()
	delete(workerIdMap, c.workerId)
}

// client get a rand rpc client.
func (c *Client) client() (*rpc.Client, error) {
	clientNum := len(c.clients)
	if clientNum == 0 {
		return nil, ErrNoRpcClient
	} else if clientNum == 1 {
		return c.clients[0], nil
	} else {
		return c.clients[rand.Intn(clientNum)], nil
	}
}

// watchWorkerId watch the zk node change.
func (c *Client) watchWorkerId(workerId int64, workerIdStr string) {
	workerIdPath := path.Join(zkPath, workerIdStr)
	log.Debug("workerIdPath: %s", workerIdPath)
	for {
		rpcs, _, watch, err := zkConn.ChildrenW(workerIdPath)
		if err != nil {
			log.Error("zkConn.ChildrenW(%s) error(%v)", workerIdPath, err)
			time.Sleep(zkNodeDelaySleep)
			continue
		}
		if len(rpcs) == 0 {
			log.Error("zkConn.ChildrenW(%s) no nodes", workerIdPath)
			time.Sleep(zkNodeDelaySleep)
			continue
		}
		// leader selection
		sort.Strings(rpcs)
		newLeader := rpcs[0]
		if c.leader == newLeader {
			log.Info("workerId: %s add a new standby gosnowflake node", workerIdStr)
		} else {
			log.Info("workerId: %s oldLeader: \"%s\", newLeader: \"%s\" not equals, continue leader selection", workerIdStr, c.leader, newLeader)
			// get new leader info
			workerNodePath := path.Join(zkPath, workerIdStr, newLeader)
			bs, _, err := zkConn.Get(workerNodePath)
			if err != nil {
				log.Error("zkConn.Get(%s) error(%v)", workerNodePath, err)
				time.Sleep(zkNodeDelaySleep)
				continue
			}
			peer := &Peer{}
			if err = json.Unmarshal(bs, peer); err != nil {
				log.Error("json.Unmarshal(%s, peer) error(%v)", string(bs), err)
				time.Sleep(zkNodeDelaySleep)
				continue
			}
			// init rpc
			tmpClients := make([]*rpc.Client, len(peer.RPC))
			tmpStop := make(chan bool, 1)
			for i, addr := range peer.RPC {
				clt, err := rpc.Dial("tcp", addr)
				if err != nil {
					log.Error("rpc.Dial(tcp, \"%s\") error(%v)", addr, err)
					continue
				}
				tmpClients[i] = clt
				go c.pingAndRetry(tmpStop, clt, addr)
			}
			// old rpc clients
			oldClients := c.clients
			oldStop := c.stop
			// atomic replace variable
			c.leader = newLeader
			c.clients = tmpClients
			c.stop = tmpStop
			// if exist, free resource
			if oldClients != nil {
				closeRpc(oldClients, oldStop)
			}
		}
		// new zk event
		event := <-watch
		log.Error("zk node(\"%s\") changed %s", workerIdPath, event.Type.String())
	}
}

// pingAndRetry ping the rpc connect and re connect when has an error.
func (c *Client) pingAndRetry(stop <-chan bool, client *rpc.Client, addr string) {
	defer func() {
		if err := client.Close(); err != nil {
			log.Error("client.Close() error(%v)", err)
		}
	}()
	var (
		failed bool
		status int
		err    error
		tmp    *rpc.Client
	)
	for {
		select {
		case <-stop:
			log.Info("addr: \"%s\" pingAndRetry goroutine exit", addr)
			return
		default:
		}
		if !failed {
			if err = client.Call(RPCPing, 0, &status); err != nil {
				log.Error("client.Call(%s) error(%v)", RPCPing, err)
				failed = true
				continue
			} else {
				failed = false
				time.Sleep(rpcClientPingSleep)
				continue
			}
		}
		if tmp, err = rpc.Dial("tcp", addr); err != nil {
			log.Error("rpc.Dial(tcp, %s) error(%v)", addr, err)
			time.Sleep(rpcClientRetrySleep)
			continue
		}
		client = tmp
		failed = false
		log.Info("client reconnect %s ok", addr)
	}
}
