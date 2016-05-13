package zk

import (
	"github.com/samuel/go-zookeeper/zk"
	"github.com/astaxie/beego"
	"time"
	"encoding/json"
	"bfs/gops/models/types"
	"strings"
	"strconv"
	"bfs/gops/models/global"
)

const (
	_storeRoot = "/store"
	_volumeRoot = "/volume"
	_rackRoot = "/rack"
	_groupRoot = "/group"

	FLAG_PERSISTENT = int32(0)
	FLAG_EPHEMERAL = int32(1)
)

var (
	ACL = zk.WorldACL(zk.PermAll)
)


type ZooKeeper struct {
	c *zk.Conn
}

func New() (z *ZooKeeper, err error) {
	var (
		servers []string
		timeout int64
		s <- chan zk.Event
	)
	servers = beego.AppConfig.Strings("ZkServers")
	if timeout, err = beego.AppConfig.Int64("ZkTimeout"); err != nil {
		return
	}

	z = new(ZooKeeper)

	z.c, s, err = zk.Connect(servers, time.Duration(timeout) * time.Second)

	go func() {
		var e zk.Event

		for {
			if e = <-s; e.Type == 0 {
				return
			}

			beego.Info("zookeeper get a event:", e.State.String())
		}
	}()

	return
}

type Node struct {
	Path     string `json:"-"`
	Name     string `json:"name"`
	Data     map[string]interface{} `json:"data"`
	Children []*Node `json:"children"`
}

func (z *ZooKeeper) GetRack() (racks []*types.Rack, err error) {
	var (
		children []string
		children1 []string
		rack *types.Rack
		store *types.Store
		data []byte
	)

	if children, _, err = z.c.Children(_rackRoot); err != nil {
		return
	}
	racks = make([]*types.Rack, len(children))
	for i, child := range children {
		rack = new(types.Rack)
		rack.Name = child

		if children1, _, err = z.c.Children(_rackRoot + "/" + child); err != nil {
			return
		}

		rack.Stores = make([]*types.Store, len(children1))
		for j, child1 := range children1 {
			store = new(types.Store)
			if data, _, err = z.c.Get(_rackRoot + "/" + child + "/" + child1); err != nil {
				return
			}

			if err = json.Unmarshal(data, store); err != nil {
				return
			}

			if store.Volumes , _, err = z.c.Children(_rackRoot + "/" + child  + "/" + child1); err != nil {
				return
			}


			store.Ip = strings.Split(store.Stat, ":")[0]
			rack.Stores[j] = store
		}
		racks[i] = rack
	}


	return
}

func (z *ZooKeeper) GetGroup() (groups []*types.Group, err error) {

	var (
		children []string
		group *types.Group
		child string
		i,j int
		storeId string
	)

	if children, _, err = z.c.Children(_groupRoot); err != nil {
		return
	}

	groups = make([]*types.Group, len(children))
	for i, child = range children {
		group = new(types.Group)
		if group.Id,err = strconv.ParseUint(child,10,64);err != nil{
			return
		}

		if group.StoreIds, _, err = z.c.Children(_groupRoot + "/" + child); err != nil {
			return
		}

		group.Stores = make([]*types.Store,len(group.StoreIds))
		for j,storeId = range group.StoreIds {
			group.Stores[j] = global.STORES[storeId]
		}

		groups[i] = group
	}

	return
}


func (z *ZooKeeper)GetVolume() (volumes []*types.Volume, err error) {
	var (
		children []string
		volume *types.Volume
		data []byte
	)

	if children, _, err = z.c.Children(_volumeRoot); err != nil {
		return
	}

	volumes = make([]*types.Volume, len(children))
	for i, child := range children {
		volume = new(types.Volume)

		if data, _, err = z.c.Get(_volumeRoot + "/" + child); err != nil {
			return
		}

		if err = json.Unmarshal(data, volume); err != nil {
			return
		}

		if volume.Id,err = strconv.ParseUint(child,10,64);err != nil{
			return
		}

		if volume.StoreIds, _, err = z.c.Children(_volumeRoot + "/" + child); err != nil {
			return
		}

		volumes[i] = volume
	}

	return
}


func (z *ZooKeeper) CreateGroup(groupId uint64, storeId string) (err error) {

	var (
		exist bool
		groupPath string
		storePath string
	)

	groupPath = _groupRoot + "/" + strconv.Itoa(int(groupId))
	if exist, _, err = z.c.Exists(groupPath); err != nil {
		beego.Error(err)
		return
	}

	if !exist {
		if _, err = z.c.Create(groupPath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			beego.Error(err)
			return
		}
	}

	storePath = groupPath + "/" + storeId
	if exist, _, err = z.c.Exists(storePath); err != nil {
		return
	}

	if !exist {
		if _, err = z.c.Create(storePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}

	return
}



func (z *ZooKeeper) AddVolume(vid uint64, storeId string) (err error) {
	var (
		volumePath string
		storePath string
		exist bool
	)

	volumePath = _volumeRoot + "/" + strconv.FormatUint(vid,10)
	if exist, _, err = z.c.Exists(volumePath); err != nil {
		return
	}

	if !exist {
		if _, err = z.c.Create(volumePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}

	storePath = volumePath +"/"+ storeId
	if exist, _, _ = z.c.Exists(storePath); err != nil {
		return
	}

	if !exist {
		if _, err = z.c.Create(storePath, []byte{}, FLAG_PERSISTENT, ACL);err != nil{
			return
		}
	}
	return
}

