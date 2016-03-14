package zk

import (
	"bfs/libs/meta"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestZk(t *testing.T) {

	var (
		zk                             *Zookeeper
		rack, store, volume, group     string
		racks, stores, volumes, groups []string
		data                           []byte
		storeMeta                      *meta.Store
		volumeState                    *meta.VolumeState
		err                            error
	)

	if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack", "/volume", "/group"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}

	if racks, _, err = zk.WatchRacks(); err != nil {
		t.Errorf("WatchRacks() error(%v)", err)
		t.FailNow()
	}

	for _, rack = range racks {
		if stores, err = zk.Stores(rack); err != nil {
			t.Errorf("Stores() error(%v)", err)
			t.FailNow()
		}
		for _, store = range stores {
			if data, err = zk.Store(rack, store); err != nil {
				t.Errorf("Store() error(%v)", err)
				t.FailNow()
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(data, storeMeta); err != nil {
				t.Errorf("Unmarshal error(%v)", err)
				t.FailNow()
			}
			fmt.Println("store:", storeMeta.Id, storeMeta.Stat)
			if volumes, err = zk.StoreVolumes(rack, store); err != nil {
				t.Errorf("StoreVolumes() error(%v)", err)
				t.FailNow()
			}
			for _, volume = range volumes {
				fmt.Println("store:", store, "volume:", volume)
			}
		}
	}
	if volumes, err = zk.Volumes(); err != nil {
		t.Errorf("Volumes() error(%v)", err)
		t.FailNow()
	}
	for _, volume = range volumes {
		if data, err = zk.Volume(volume); err != nil {
			t.Errorf("Volume() error(%v)", err)
			t.FailNow()
		}
		volumeState = new(meta.VolumeState)
		if err = json.Unmarshal(data, volumeState); err != nil {
			t.Errorf("Unmarshal error(%v)", err)
			t.FailNow()
		}
		fmt.Println("volume:", volumeState.FreeSpace)
		if stores, err = zk.VolumeStores(volume); err != nil {
			t.Errorf("VolumeStores error(%v)", err)
			t.FailNow()
		}
		for _, store = range stores {
			fmt.Println("Volume:", volume, " store:", store)
		}
	}
	if groups, _, err = zk.WatchGroups(); err != nil {
		t.Errorf("WatchGroups error(%v)", err)
		t.FailNow()
	}
	for _, group = range groups {
		if stores, err = zk.GroupStores(group); err != nil {
			t.Errorf("GroupStores error(%v)")
			t.FailNow()
		}
		for _, store = range stores {
			fmt.Println("group:", group, " store:", store)
		}
	}
}
