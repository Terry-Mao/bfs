package volume

import (
	"time"
)

// volume options
type Options struct {
	Debug         bool          `json:"debug"`
	NeedleCache   int           `json:"needle_cache"`
	DeleteChan    int           `json:"delete_chan"`
	CheckSize     int           `json:"check_size"`
	CheckInterval int           `json:"check_interval"`
	SignalCount   int           `json:"signal_count"`
	SignalTime    time.Duration `json:"signal_time"`
}
