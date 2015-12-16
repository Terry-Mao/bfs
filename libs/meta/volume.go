package meta

import "github.com/Terry-Mao/bfs/libs/stat"

type Volume struct {
	Id           int32       `json:"id"`
	Block        *SuperBlock `json:"block"`
	CheckNeedles []Needle    `json:"check_needles"`
	Stats        *stat.Stats `json:"stats"`
}

type InfoVolume struct {
	Volumes []*Volume `json:"volumes"`
}

// VolumeState  for zk /volume stat
type VolumeState struct {
	TotalWriteProcessed uint64 `json:"total_write_processed"`
	TotalWriteDelay     uint64 `json:"total_write_delay"`
	FreeSpace           uint32 `json:"free_space"`
}
