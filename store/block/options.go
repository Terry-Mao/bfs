package block

// block options
type Options struct {
	NeedleMaxSize int  `json:"needle_max_size"`
	BufferSize    int  `json:"buffer_size"`
	SyncAtWrite   int  `json:"sync_at_write"`
	Syncfilerange bool `json:"sync_file_range"`
}
