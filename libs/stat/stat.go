package stat

import (
	"time"
)

const (
	statCalcDuration = 1 * time.Second
)

type Stats struct {
	// qps & tps
	TotalCommandsProcessed  uint64 `json:"total_commands_processed"`
	TotalWriteProcessed     uint64 `json:"total_write_processed"`
	WriteTPS                uint64 `json:"write_tps"`
	lastTotalWriteProcessed uint64 `json:"-"`
	TotalDelProcessed       uint64 `json:"total_del_processed"`
	DelTPS                  uint64 `json:"del_tps"`
	lastTotalDelProcessed   uint64 `json:"-"`
	TotalGetProcessed       uint64 `json:"total_get_processed"`
	GetQPS                  uint64 `json:"get_qps"`
	lastTotalGetProcessed   uint64 `json:"-"`
	TotalFlushProcessed     uint64 `json:total_flush_processed`
	FlushTPS                uint64 `json:"flush_tps"`
	lastTotalFlushProcessed uint64 `json:"-"`
	TotalCompactProcessed   uint64 `json:"total_compact_processed"`
	// bytes
	TotalTransferedBytes     uint64 `json:"total_transfered_bytes"`
	TransferedFlow           uint64 `json:"transfered_flow"`
	lastTotalTransferedBytes uint64 `json:"-"`
	TotalReadBytes           uint64 `json:"total_read_bytes"`
	ReadFlow                 uint64 `json:"read_flow"`
	lastTotalReadBytes       uint64 `json:"-"`
	TotalWriteBytes          uint64 `json:"total_write_bytes"`
	WriteFlow                uint64 `json:"write_flow"`
	lastTotalWriteBytes      uint64 `json:"-"`
	// delay
	TotalDelay          uint64 `json:"total_delay"`
	lastTotalDelay      uint64 `json:"-"`
	Delay               uint64 `json:"delay"`
	TotalAddDelay       uint64 `json:"total_add_delay"`
	lastTotalAddDelay   uint64 `json:"-"`
	AddDelay            uint64 `json:"add_delay"`
	TotalWriteDelay     uint64 `json:"total_write_delay"`
	lastTotalWriteDelay uint64 `json:"-"`
	WriteDelay          uint64 `json:"write_delay"`
	TotalDelDelay       uint64 `json:"total_del_delay"`
	lastTotalDelDelay   uint64 `json:"-"`
	DelDelay            uint64 `json:"del_delay"`
	TotalGetDelay       uint64 `json:"total_get_delay"`
	lastTotalGetDelay   uint64 `json"-"`
	GetDelay            uint64 `json:"get_delay"`
	TotalFlushDelay     uint64 `json:"total_flush_delay"`
	lastTotalFlushDelay uint64 `json:"-"`
	FlushDelay          uint64 `json:"flush_delay"`
	TotalCompactDelay   uint64 `json:"total_compact_delay"`
}

// Calc calc the commands qps/tps.
func (s *Stats) Calc() {
	// qps & tps
	s.WriteTPS = s.TotalWriteProcessed - s.lastTotalWriteProcessed
	s.lastTotalWriteProcessed = s.TotalWriteProcessed
	s.DelTPS = s.TotalDelProcessed - s.lastTotalDelProcessed
	s.lastTotalDelProcessed = s.TotalDelProcessed
	s.GetQPS = s.TotalGetProcessed - s.lastTotalGetProcessed
	s.lastTotalGetProcessed = s.TotalGetProcessed
	s.FlushTPS = s.TotalFlushProcessed - s.lastTotalFlushProcessed
	s.lastTotalFlushProcessed = s.TotalFlushProcessed
	s.TotalCommandsProcessed = s.TotalWriteProcessed + s.TotalDelProcessed +
		s.TotalGetProcessed + s.TotalFlushProcessed +
		s.TotalCompactProcessed
	// bytes
	s.ReadFlow = s.TotalReadBytes - s.lastTotalReadBytes
	s.lastTotalReadBytes = s.TotalReadBytes
	s.WriteFlow = s.TotalWriteBytes - s.lastTotalWriteBytes
	s.lastTotalWriteBytes = s.TotalWriteBytes
	s.TotalTransferedBytes = s.TotalReadBytes + s.TotalWriteBytes
	s.TransferedFlow = s.TotalTransferedBytes - s.lastTotalTransferedBytes
	s.lastTotalTransferedBytes = s.TotalTransferedBytes
	// delay
	s.AddDelay = s.TotalAddDelay - s.lastTotalAddDelay
	s.lastTotalAddDelay = s.TotalAddDelay
	s.WriteDelay = s.TotalWriteDelay - s.lastTotalWriteDelay
	s.lastTotalWriteDelay = s.TotalWriteDelay
	s.DelDelay = s.TotalDelDelay - s.lastTotalDelDelay
	s.lastTotalDelDelay = s.TotalDelDelay
	s.GetDelay = s.TotalGetDelay - s.lastTotalGetDelay
	s.lastTotalGetDelay = s.TotalGetDelay
	s.FlushDelay = s.TotalFlushDelay - s.lastTotalFlushDelay
	s.lastTotalFlushDelay = s.TotalFlushDelay
	s.TotalDelay = s.TotalAddDelay + s.TotalWriteDelay + s.TotalDelDelay +
		s.TotalGetDelay + s.TotalFlushDelay
	s.Delay = s.TotalDelay - s.lastTotalDelay
	s.lastTotalDelay = s.TotalDelay
	return
}

// Merge merge other stats.
func (s *Stats) Merge(s1 *Stats) {
	// qps & tps
	s.TotalWriteProcessed += s1.TotalWriteProcessed
	s.TotalDelProcessed += s1.TotalDelProcessed
	s.TotalGetProcessed += s1.TotalGetProcessed
	s.TotalFlushProcessed += s1.TotalFlushProcessed
	s.TotalCompactProcessed += s1.TotalCompactProcessed
	// bytes
	s.TotalReadBytes += s1.TotalReadBytes
	s.TotalWriteBytes += s1.TotalWriteBytes
	// delay
	s.TotalAddDelay += s1.TotalAddDelay
	s.TotalWriteDelay += s1.TotalWriteDelay
	s.TotalDelDelay += s1.TotalDelDelay
	s.TotalGetDelay += s1.TotalGetDelay
	s.TotalFlushDelay += s1.TotalFlushDelay
	s.TotalCompactDelay += s1.TotalCompactDelay
}

// Reset reset the stat.
func (s *Stats) Reset() {
	// qps & tps
	s.TotalWriteProcessed = 0
	s.TotalDelProcessed = 0
	s.TotalGetProcessed = 0
	s.TotalFlushProcessed = 0
	s.TotalCompactProcessed = 0
	// bytes
	s.TotalReadBytes = 0
	s.TotalWriteBytes = 0
	// delay
	s.TotalAddDelay = 0
	s.TotalWriteDelay = 0
	s.TotalDelDelay = 0
	s.TotalGetDelay = 0
	s.TotalFlushDelay = 0
	s.TotalCompactDelay = 0
}

// Stat is store server stat.
type Info struct {
	// server
	Ver       string    `json:"ver""`
	GitSHA1   string    `json:"git_sha1"`
	StartTime time.Time `json:"start_time"`
	OS        string    `json:"os"`
	ProcessId int       `json:"process_id"`
	// clients
	TotalConnectionsReceived uint64 `json:"total_connections_received"`
	ConnectedClients         uint64 `json:"connected_clients"`
	BlockedClients           uint64 `json:"blocked_clients"`
	// stats
	Stats *Stats `json:"stats"`
}
