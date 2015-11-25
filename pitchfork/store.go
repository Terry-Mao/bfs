package main
import "github.com/Terry-Mao/bfs/libs/meta"

const maxOffset = 4294967295

type StoreList []*meta.Store

// Len
func (sl StoreList) Len() int {
	return len(sl)
}

// Less
func (sl StoreList) Less(i, j int) bool {
	return sl[i].Id < sl[j].Id
}

// Swap
func (sl StoreList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}
