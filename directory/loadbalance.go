package main
import "github.com/Terry-Mao/bfs/libs/meta"

type StoreList []*meta.Store

// StoreState contains store state data
type StoreState struct {
	id              string
	status          int           //RW permissions
	restSpace       int           //rest space of store
	avgResponseTime float         //average response time of write req
	numReqs         int           //num reqs of interval time
	score           int           //score effect probability of being chosen
}

type StoreStateList []*StoreState

// cal_score calculating score for all stores
func (StoreStateList s)calScore() err error {
	//
}

// getWritableStoreGroup get one suitable group store for writing
func (StoreStateList s)getWritableStoreGroup() []*meta.Store {
	//先得到每个store的得分，然后分组，再计算每组的得分  每组的得分为该组的store的最低分
}

// getReadableStoreGroup get one suitable group store fro reading
func (StoreStateList s)getReadableStoreGroup() []*meta.Store {
	//
}

