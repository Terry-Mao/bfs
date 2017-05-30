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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	log "golang/log4go"
	"net/rpc"
	"strconv"
	"time"
)

/*
   dataCenter:1
   zookeeper
   ============
   /gosnowflake-servers/
       .../workerId/
       .../1/ # watcher
            .../ephemeral|sequence
            .../1 # leader
            .../2 # standby
       .../2/
            .../1 # leader
       .../3/
            .../1 # leader

    1. peers: get all worker path's children, such as /gosnowflake-servers/1/1.
    2. SanityCheckPeers: check current process with all peers.
    3. RegWorkerId: register current process as a standby or leader, this will
       cause all watchers receive a node add event then start leader selection.
       if node = leader then ignore
       else init rpc
    4. when process exit, the zk ephemeral node will disappear, then trigger a
       node del event to all watchers, start leader selection.
       if node = leader then ignore
       else if node != leader then init rpc
       else if don't exist any node then retry wait node add event.

*/

const (
	timestampMaxDelay = int64(10 * time.Second)
)

// Peer store data in zookeeper.
type Peer struct {
	RPC    []string `json:"rpc"`
	Thrift []string `json:"thrift"`
}

var (
	zkConn *zk.Conn
)

// InitZK init the zookeeper connection.
func InitZK() error {
	conn, session, err := zk.Connect(MyConf.ZKAddr, MyConf.ZKTimeout)
	if err != nil {
		log.Error("zk.Connect(\"%v\", %d) error(%v)", MyConf.ZKAddr, MyConf.ZKTimeout, err)
		return err
	}
	zkConn = conn
	go func() {
		for {
			event := <-session
			log.Info("zookeeper get a event: %s", event.State.String())
		}
	}()
	return nil
}

// RegWorkerId as a leader worker or a standby worker.
func RegWorkerId(workerId int64) (err error) {
	log.Info("trying to claim workerId: %d", workerId)
	workerIdPath := fmt.Sprintf("%s/%d", MyConf.ZKPath, workerId)
	if _, err = zkConn.Create(workerIdPath, []byte(""), 0, zk.WorldACL(zk.PermAll)); err != nil {
		if err == zk.ErrNodeExists {
			log.Warn("zk.create(\"%s\") exists", workerIdPath)
		} else {
			log.Error("zk.create(\"%s\") error(%v)", workerIdPath, err)
			return
		}
	}
	d, err := json.Marshal(&Peer{RPC: MyConf.RPCBind, Thrift: MyConf.ThriftBind})
	if err != nil {
		log.Error("json.Marshal() error(%v)", err)
		return
	}
	workerIdPath += "/"
	if _, err = zkConn.Create(workerIdPath, d, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll)); err != nil {
		log.Error("zk.create(\"%s\") error(%v)", workerIdPath, err)
		return
	}
	return
}

// getPeers get workers all children in zookeeper.
func getPeers() (map[int][]*Peer, error) {
	// try create ZKPath
	if _, err := zkConn.Create(MyConf.ZKPath, []byte(""), 0, zk.WorldACL(zk.PermAll)); err != nil {
		if err == zk.ErrNodeExists {
			log.Warn("zk.create(\"%s\") exists", MyConf.ZKPath)
		} else {
			log.Error("zk.create(\"%s\") error(%v)", MyConf.ZKPath, err)
			return nil, err
		}
	}
	// get all workers
	workers, _, err := zkConn.Children(MyConf.ZKPath)
	if err != nil {
		log.Error("zk.Get(\"%s\") error(%v)", MyConf.ZKPath, err)
		return nil, err
	}
	res := make(map[int][]*Peer, len(workers))
	for _, worker := range workers {
		id, err := strconv.Atoi(worker)
		if err != nil {
			log.Error("strconv.Atoi(\"%s\") error(%v)", worker, err)
			return nil, err
		}
		workerIdPath := fmt.Sprintf("%s/%s", MyConf.ZKPath, worker)
		// get all worker's nodes
		nodes, _, err := zkConn.Children(workerIdPath)
		for _, node := range nodes {
			nodePath := fmt.Sprintf("%s/%s", workerIdPath, node)
			// get golang rpc & thrift address
			d, _, err := zkConn.Get(nodePath)
			if err != nil {
				log.Error("zk.Get(\"%s\") error(%v)", nodePath, err)
				return nil, err
			}
			peer := &Peer{}
			if err = json.Unmarshal(d, peer); err != nil {
				log.Error("json.Unmarshal(\"%s\", peer) error(%v)", d, err)
				return nil, err
			}
			peers, ok := res[id]
			if !ok {
				peers = []*Peer{peer}
			} else {
				peers = append(peers, peer)
			}
			res[id] = peers
		}
	}
	return res, nil
}

// SanityCheckPeers check the zookeeper datacenterId and all nodes time.
func SanityCheckPeers() error {
	peers, err := getPeers()
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return nil
	}
	timestamps := int64(0)
	timestamp := int64(0)
	datacenterId := int64(0)
	peerCount := int64(0)
	for id, workers := range peers {
		for _, peer := range workers {
			// rpc or thrift call
			if len(peer.RPC) > 0 {
				// golang rpc call
				cli, err := rpc.Dial("tcp", peer.RPC[0])
				if err != nil {
					log.Error("rpc.Dial(\"tcp\", \"%s\") error(%v)", peer.RPC[0], err)
					return err
				}
				defer cli.Close()
				if err = cli.Call("SnowflakeRPC.DatacenterId", 0, &datacenterId); err != nil {
					log.Error("rpc.Call(\"SnowflakeRPC.DatacenterId\", 0) error(%v)", err)
					return err
				}
				if err = cli.Call("SnowflakeRPC.Timestamp", 0, &timestamp); err != nil {
					log.Error("rpc.Call(\"SnowflakeRPC.Timestamp\", 0) error(%v)", err)
					return err
				}
			} else if len(peer.Thrift) > 0 {
				// TODO thrift call
			} else {
				log.Error("workerId: %d don't have any rpc address", id)
				return errors.New("workerId no rpc")
			}
			// check datacenterid
			if datacenterId != MyConf.DatacenterId {
				log.Error("workerId: %d has datacenterId %d, but ours is %d", id, datacenterId, MyConf.DatacenterId)
				return errors.New("Datacenter id insanity")
			}
			// add timestamps
			timestamps += timestamp
			peerCount++
		}
	}
	// check 10s
	// calc avg timestamps
	now := time.Now().Unix()
	avg := int64(timestamps / peerCount)
	log.Debug("timestamps: %d, peer: %d, avg: %d, now - avg: %d, maxdelay: %d", timestamps, peerCount, avg, now-avg, timestampMaxDelay)
	if now-avg > timestampMaxDelay {
		log.Error("timestamp sanity check failed. Mean timestamp is %d, but mine is %d so I'm more than 10s away from the mean", avg, now)
		return errors.New("timestamp sanity check failed")
	}
	return nil
}

// CloseZK close the zookeeper connection.
func CloseZK() {
	zkConn.Close()
}
