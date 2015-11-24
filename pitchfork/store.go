package main

const maxOffset = 4294967295

//Store save store node
type Store struct {
	rack      string
	ID        string
	host      string
	status    uint32
}
type StoreList []*Store

//Len
func (sl StoreList) Len() int {
	return len(sl)
}

//Less
func (sl StoreList) Less(i, j int) bool {
	return sl[i].ID < sl[j].ID
}

//Swap
func (sl StoreList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}
