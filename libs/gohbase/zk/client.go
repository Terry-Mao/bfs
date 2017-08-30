// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package zk encapsulates our interactions with ZooKeeper.
package zk

import (
	"encoding/binary"
	"time"

	log "github.com/golang/glog"

	"bfs/libs/gohbase/pb"
	"path"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	ResourceTypeMaster = iota
	ResourceTypeMeta   = iota
	resourceCount      = iota
)

const (
	ServerStateUp   int = 1
	ServerStateDown int = 2
)

// ResourceName is a type alias that is used to represent different resources
// in ZooKeeper
type ResourceName string

var (
	// Meta is a ResourceName that indicates that the location of the Meta
	// table is what will be fetched

	// Master is a ResourceName that indicates that the location of the Master
	// server is what will be fetched

	defaultNames = [resourceCount]string{
		"/hbase/master",
		"/hbase/meta-region-server",
	}
)

type ServerInfo struct {
	Host string
	Port uint16
}

type serverInfo struct {
	Host        string
	Port        uint16
	State       int
	ChangeCount int64
	UpdateTime  time.Time
}

func (ms *serverInfo) Valid() bool {
	return ms != nil && ms.Host != "" && ms.Port > 0 && ms.State == ServerStateUp
}

func (ms *serverInfo) ServerInfo() (res *ServerInfo) {
	if ms.Valid() {
		res = &ServerInfo{
			Host: ms.Host,
			Port: ms.Port,
		}
	}
	return
}

func (ms *serverInfo) Disable() (changed bool) {
	changed = ms.Valid() // only changes when original state is valid
	ms.ChangeCount += 1
	ms.UpdateTime = time.Now()
	ms.State = ServerStateDown
	return
}

func (ms *serverInfo) Clear() {
	ms.ChangeCount += 1
	ms.UpdateTime = time.Now()
	ms.State = ServerStateDown
	ms.Host = ""
	ms.Port = 0
}

func (ms *serverInfo) Update(newMS *serverInfo) (changed bool) {
	ms.UpdateTime = newMS.UpdateTime
	changed = !ms.Equals(newMS)
	if changed {
		ms.ChangeCount += 1
	}
	ms.Host = newMS.Host
	ms.Port = newMS.Port
	ms.State = newMS.State
	return
}

func (ms *serverInfo) Equals(newMS *serverInfo) bool {
	return ms.Host == newMS.Host && ms.Port == newMS.Port && ms.State == newMS.State
}

type ServerWatcher interface {
	// SetServer implementation should ensure it will not block
	SetServer(resourceType int, ms *ServerInfo)
}

type ZKClient struct {
	quorum []string
	conn   *zk.Conn

	watchersLock   *sync.Mutex
	serverWatchers [resourceCount][]ServerWatcher
	watchStopChan  [resourceCount]chan struct{} // buf-1 ensures send will not block
	resources      [resourceCount]string
	serverInfos    [resourceCount]*serverInfo
}

func NewZKClient(zks []string, zkRoot, master, meta string, useMaster, useMeta bool, sessionTimeout time.Duration) (res *ZKClient, err error) {
	c := &ZKClient{
		quorum:       zks,
		watchersLock: &sync.Mutex{},
	}
	if master == "" {
		master = defaultNames[ResourceTypeMaster]
	}
	if meta == "" {
		meta = defaultNames[ResourceTypeMeta]
	}
	if zkRoot != "" {
		master = path.Join(zkRoot, master)
		meta = path.Join(zkRoot, meta)
	}
	c.resources[ResourceTypeMaster] = master
	c.resources[ResourceTypeMeta] = meta
	conn, _, err := zk.Connect(c.quorum, sessionTimeout)
	if err != nil {
		return // XXX
	}
	c.conn = conn
	for i := 0; i < resourceCount; i++ {
		c.watchStopChan[i] = make(chan struct{}, 1)
		c.serverInfos[i] = &serverInfo{}
	}
	err = c.watchServer(useMaster, useMeta)
	if err != nil {
		return
		conn.Close()
	}
	res = c
	return
}

func (c *ZKClient) watchServer(useMaster, useMeta bool) (err error) {
	wg := &sync.WaitGroup{}
	wg.Add(resourceCount)
	for i := 0; i < resourceCount; i++ {
		if (i == ResourceTypeMaster && !useMaster) || (i == ResourceTypeMeta && !useMeta) {
			wg.Done()
			continue
		}
		go func(resourceType int, wg *sync.WaitGroup) {
			for {
				path := c.resources[resourceType]
				buf, _, evCh, getErr := c.conn.GetW(path)
				sleep := int64(0)
				curServerInfo := c.serverInfos[resourceType]
				if getErr != nil {
					log.Errorf("c.conn.GetW(%s) failed, err is (%v)", path, getErr)
					if wg != nil {
						err = getErr
						wg.Done()
						return
					}
					sleep = 1
					// XXX
				} else {
					var changed bool
					newServerInfo := serverInfoFromContent(buf, resourceType)
					if newServerInfo == nil {
						log.Errorf("serverInfoFromContent(%v, %d) return nil serverInfo", buf, resourceType)
						changed = curServerInfo.Disable()
					} else {
						changed = curServerInfo.Update(newServerInfo)
					}
					if changed {
						log.Info("server %d change to (%v) as newServerInfo: (%v)", resourceType, curServerInfo, newServerInfo)
						for _, watcher := range c.serverWatchers[resourceType] {
							watcher.SetServer(resourceType, curServerInfo.ServerInfo())
						}
					}
					if wg != nil {
						wg.Done()
					}
					for ev := range evCh {
						log.Info("resourceType %d receive zk event %v", resourceType, ev)
						curServerInfo = c.serverInfos[resourceType]
						switch ev.Type {
						case zk.EventNodeCreated:
							fallthrough
						case zk.EventNodeDataChanged:
							buf, _, getErr = c.conn.Get(path)
							if getErr != nil {
								log.Error("failed to get (%s) from zk after event (%v)", path, ev.Type)
								continue // XXX
							}
							newServerInfo := serverInfoFromContent(buf, resourceType)
							if newServerInfo == nil {
								changed = curServerInfo.Disable()
							} else {
								changed = curServerInfo.Update(newServerInfo)
							}
						case zk.EventNodeDeleted:
							changed = curServerInfo.Disable()
						default:
							log.Info("resource type %d receives event %d from zk", resourceType, ev.Type)
						}
						if changed {
							log.Info("server %d change to (%v) as event %v", resourceType, curServerInfo, ev)
							for _, watcher := range c.serverWatchers[resourceType] {
								watcher.SetServer(resourceType, curServerInfo.ServerInfo())
							}
						}
					}
					log.Warning("evCh is closed!")
				}
				select {
				case <-c.watchStopChan[resourceType]:
					log.Info("quit watch for resource type %d as receive signal from stop chan", resourceType)
					return
				default:
					wg = nil
					if sleep > 0 {
						time.Sleep(time.Duration(sleep) * time.Second)
					}
					continue
				}
			}
		}(i, wg)
	}
	wg.Wait()
	return
}

func (c *ZKClient) WatchServer(serverType int, watcher ServerWatcher) {
	if serverType < 0 || serverType >= resourceCount || watcher == nil {
		return
	}
	c.watchersLock.Lock()
	c.serverWatchers[serverType] = append(c.serverWatchers[serverType], watcher)
	c.watchersLock.Unlock()
}

// LocateResource returns the location of the specified resource.
func (c *ZKClient) LocateResource(resourceType int) (res *ServerInfo) {
	if resourceType < 0 || resourceType >= resourceCount {
		return
	}
	res = c.serverInfos[resourceType].ServerInfo()
	log.Infof("LocateResource(%d), return %v", resourceType, res)
	return
}

func serverInfoFromContent(buf []byte, resourceType int) (res *serverInfo) {
	if len(buf) == 0 {
		log.Errorf("%d was empty!", resourceType)
		return
	} else if buf[0] != 0xFF {
		log.Errorf("The first byte of %d was 0x%x, not 0xFF", resourceType, buf[0])
		return
	}
	metadataLen := binary.BigEndian.Uint32(buf[1:])
	if metadataLen < 1 || metadataLen > 65000 {
		log.Error("Invalid metadata length for %d: %d", resourceType, metadataLen)
		return
	}
	buf = buf[1+4+metadataLen:]
	magic := binary.BigEndian.Uint32(buf)
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"
	if magic != pbufMagic {
		log.Error("Invalid magic number for %d: %d", resourceType, magic)
		return
	}
	buf = buf[4:]
	var server *pb.ServerName
	if resourceType == ResourceTypeMeta {
		meta := &pb.MetaRegionServer{}
		err := proto.UnmarshalMerge(buf, meta)
		if err != nil {
			log.Error("Failed to deserialize the MetaRegionServer entry from ZK: %s", err)
			return
		}
		server = meta.Server
	} else {
		master := &pb.Master{}
		err := proto.UnmarshalMerge(buf, master)
		if err != nil {
			log.Error("Failed to deserialize the Master entry from ZK: %s", err)
			return
		}
		server = master.Master
	}
	res = &serverInfo{
		Host:       *server.HostName,
		Port:       uint16(*server.Port),
		UpdateTime: time.Now(),
		State:      ServerStateUp,
	}
	return
}
