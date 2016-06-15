package meta

type Needle struct {
	Key    int64 `json:"key"`
	Cookie int32 `json:"cookie"`
	Vid    int32 `json:"vid"`
	MTime  int64 `json:"update_time"`
}
