package block

// block options
type Options struct {
	BufferSize    int  `json:"buffer_size"`
	SyncAtWrite   int  `json:"sync_at_write"`
	Syncfilerange bool `json:"sync_file_range"`
}
