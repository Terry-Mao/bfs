package index

import (
	"time"
)

// index options.
type Options struct {
	MergeAtTime   time.Duration `json:"merge_at_duration"`
	MergeAtWrite  int           `json:"merge_at_write"`
	RingBuffer    int           `json:"ring_buffer"`
	BufferSize    int           `json:"buffer_size"`
	SyncAtWrite   int           `json:"sync_at_write"`
	Syncfilerange bool          `json:"sync_file_range"`
}
