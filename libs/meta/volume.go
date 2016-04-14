package meta

import "bfs/libs/stat"

type Volume struct {
	Id    int32       `json:"id"`
	Block *SuperBlock `json:"block"`
	Stats *stat.Stats `json:"stats"`
}

type Volumes struct {
	Volumes []*Volume `json:"volumes"`
}

// VolumeState  for zk /volume stat
type VolumeState struct {
	TotalWriteProcessed uint64 `json:"total_write_processed"`
	TotalWriteDelay     uint64 `json:"total_write_delay"`
	FreeSpace           uint32 `json:"free_space"`
}
