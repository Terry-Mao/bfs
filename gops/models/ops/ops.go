package ops

import (
	"bfs/gops/models/store"
	"bfs/gops/models/zk"
	"errors"
	"bfs/gops/models/types"
	"github.com/astaxie/beego"
	"time"
	"bfs/gops/models/global"
)



var (


	addGroupParamsErr = errors.New("param ips length can't zero and aliquot copys ")
	groupNotExistErr = errors.New("group is not exist")
)

type Ops struct {
	store *store.Store
	zk    *zk.ZooKeeper
}

var OpsManager *Ops

func InitOps() (err error) {
	OpsManager, err = New()

	go OpsManager.LoadData()
	return
}

func (o *Ops)LoadData() {
	for {
		o.LoadRacks()
		o.LoadGroups()
		o.LoadVolumes()

		beego.Info("加载数据完成[racks,groups,volumes]...")
		time.Sleep(10 * time.Second)
	}
}


func New() (ops *Ops, err error) {
	ops = new(Ops)

	if ops.store, err = store.New(); err != nil {
		return
	}

	if ops.zk, err = zk.New(); err != nil {
		return
	}


	return
}

func (o *Ops)LoadRacks() {
	var (
		racks []*types.Rack
		err error
		rack *types.Rack
		store *types.Store
	)

	if racks, err = o.GetRack(); err != nil {
		beego.Error(err)
	}

	global.STORES = make(map[string]*types.Store)

	for _, rack = range racks {
		for _, store = range rack.Stores {
			global.STORES[store.Id] = store
		}
	}
}

func (o *Ops)LoadGroups() {
	var (
		groups []*types.Group
		err error
		group *types.Group
		storeId string
	)

	if groups, err = o.GetGroup(); err != nil {
		beego.Error(err)
	}

	global.IN_GROUP_STORES = make(map[string]*types.Store)
	global.GROUPS = make(map[uint64]*types.Group)

	global.MAX_GROUP_ID = 0
	for _, group = range groups {
		if group.Id > global.MAX_GROUP_ID {
			global.MAX_GROUP_ID = group.Id
		}
		for _, storeId = range group.StoreIds {
			global.IN_GROUP_STORES[storeId] = global.STORES[storeId]
		}
		global.GROUPS[group.Id] = group
	}

}

func (o *Ops)LoadVolumes() {
	var (
		volumes []*types.Volume
		err error
		volume *types.Volume
	)

	if volumes, err = o.GetVolume(); err != nil {
		beego.Error(err)
	}


	global.MAX_VOLUME_ID= 0
	for _, volume = range volumes {
		if volume.Id > global.MAX_VOLUME_ID {
			global.MAX_VOLUME_ID= volume.Id
		}
	}
}


func (o *Ops) GetRack() (racks []*types.Rack, err error) {
	racks, err = o.zk.GetRack()
	return
}

func (o *Ops) GetFreeStore() (stores []*types.Store, err error) {
	var (
		racks []*types.Rack
		rack *types.Rack
		store *types.Store
		ok bool
	)

	racks, err = o.zk.GetRack()

	stores = make([]*types.Store, 0)
	for _, rack = range racks {
		for _, store = range rack.Stores {
			if _, ok = global.IN_GROUP_STORES[store.Id]; !ok {
				stores = append(stores, store)
			}
		}
	}

	return
}

func (o *Ops) GetGroup() (groups []*types.Group, err error) {
	groups, err = o.zk.GetGroup()
	return
}

func (o *Ops) GetVolume() (volumes []*types.Volume, err error) {
	volumes, err = o.zk.GetVolume()
	return
}

func (o *Ops) AddFreeVolume(host string, n int32, bdir, idir string) (err error) {
	err = o.store.AddFreeVolume(host, n, bdir, idir)
	return
}

func (o *Ops)AddGroup(stores []string, copys, racks int) (err error) {
	var (
		groupId uint64
		storeId string
	)

	if len(stores) == 0 {
		err = addGroupParamsErr
		return
	}

	groupId = global.MAX_GROUP_ID + 1
	for _, storeId = range stores {
		if err = o.zk.CreateGroup(groupId, storeId); err != nil {
			return
		}
		global.IN_GROUP_STORES[storeId] = global.STORES[storeId]
	}
	return
}

func (o *Ops) AddVolume(groupId uint64, n int) (err error) {
	var (
		vid uint64
		group *types.Group
		ok bool
		store *types.Store
	)


	if group, ok = global.GROUPS[groupId]; !ok {
		return groupNotExistErr
	}
	for i := 0; i < n; i++ {

		vid = global.MAX_VOLUME_ID + 1

		for _, store = range group.Stores {
			if err = o.store.AddVolume(store.Admin, vid); err != nil {
				return
			}

			if err = o.zk.AddVolume(vid,store.Id) ;err != nil {
				return
			}
		}

		global.MAX_VOLUME_ID = vid
	}
	return
}




