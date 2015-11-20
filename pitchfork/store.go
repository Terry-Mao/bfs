package main

const maxOffset = 4294967295

type Store struct {
	rack      string
	ID        string
	host      string
	status    int32
}
type StoreList []*Store

func (sl StoreList) Len() int {
	return len(sl)
}

func (sl StoreList) Less(i, j int) bool {
	return sl[i].ID < sl[j].ID
}

func (sl StoreList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}
