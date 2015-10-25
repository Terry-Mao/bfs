package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"sort"
	"time"
)

const (
	statCalcDuration = 1 * time.Second
)

var (
	StoreInfo *Info
)

func init() {
	StoreInfo = &Info{
		Ver:       Ver,
		StartTime: time.Now(),
		Stats:     &Stats{},
	}
}

type Stats struct {
	// qps & tps
	TotalCommandsProcessed  uint64 `json:"total_commands_processed"`
	TotalAddProcessed       uint64 `json:"total_add_processed"`
	TotalAddTPS             uint64 `json:"total_add_tps"`
	lastTotalAddProcessed   uint64 `json:"-"`
	TotalWriteProcessed     uint64 `json:"total_write_processed"`
	TotalWriteTPS           uint64 `json:"total_write_tps"`
	lastTotalWriteProcessed uint64 `json:"-"`
	TotalDelProcessed       uint64 `json:"total_del_processed"`
	TotalDelTPS             uint64 `json:"total_del_tps"`
	lastTotalDelProcessed   uint64 `json:"-"`
	TotalGetProcessed       uint64 `json:"total_get_processed"`
	TotalGetQPS             uint64 `json:"total_get_qps"`
	lastTotalGetProcessed   uint64 `json:"-"`
	TotalFlushProcessed     uint64 `json:total_flush_processed`
	TotalFlushTPS           uint64 `json:"total_flush_tps"`
	lastTotalFlushProcessed uint64 `json:"-"`
	TotalCompressProcessed  uint64 `json:"total_compress_processed"`
	// TODO
	// bytes
	// delay
}

// Calc calc the commands qps/tps.
func (s *Stats) Calc() {
	s.TotalAddTPS = s.TotalAddProcessed - s.lastTotalAddProcessed
	s.lastTotalAddProcessed = s.TotalAddProcessed
	s.TotalWriteTPS = s.TotalWriteProcessed - s.lastTotalWriteProcessed
	s.lastTotalWriteProcessed = s.TotalWriteProcessed
	s.TotalDelTPS = s.TotalDelProcessed - s.lastTotalDelProcessed
	s.lastTotalDelProcessed = s.TotalDelProcessed
	s.TotalGetQPS = s.TotalGetProcessed - s.lastTotalGetProcessed
	s.lastTotalGetProcessed = s.TotalGetProcessed
	s.TotalFlushTPS = s.TotalFlushProcessed - s.lastTotalFlushProcessed
	s.lastTotalFlushProcessed = s.TotalFlushProcessed
	s.TotalCommandsProcessed = s.TotalAddProcessed + s.TotalWriteProcessed +
		s.TotalDelProcessed + s.TotalGetProcessed + s.TotalFlushProcessed +
		s.TotalCompressProcessed
}

// Merge merge other stats.
func (s *Stats) Merge(s1 *Stats) {
	s.TotalAddProcessed += s1.TotalAddProcessed
	s.TotalWriteProcessed += s1.TotalWriteProcessed
	s.TotalDelProcessed += s1.TotalDelProcessed
	s.TotalGetProcessed += s1.TotalGetProcessed
	s.TotalFlushProcessed += s1.TotalFlushProcessed
	s.TotalCompressProcessed += s1.TotalCompressProcessed
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

// retWrite marshal the result and write to client(get).
func retWrite(w http.ResponseWriter, r *http.Request, res map[string]interface{}, start time.Time) {
	var data, err = json.Marshal(res)
	if err != nil {
		log.Errorf("json.Marshal(\"%v\") error(%v)", res, err)
		return
	}
	if _, err := w.Write(data); err != nil {
		log.Errorf("w.Write(\"%s\") error(%v)", string(data), err)
	}
	log.Infof("req: \"%s\", get: res:\"%s\", ip:\"%s\", time:\"%fs\"", r.URL.String(), string(data), r.RemoteAddr, time.Now().Sub(start).Seconds())
}

func StartStat(s *Store, addr string) {
	http.HandleFunc("/stat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var (
			v       *Volume
			vid     int32
			ok      bool
			res     = map[string]interface{}{}
			vids    = make([]int32, 0, len(s.volumes))
			volumes = make([]*Volume, 0, len(s.volumes))
		)
		defer retWrite(w, r, res, time.Now())
		for vid, v = range s.volumes {
			vids = append(vids, vid)
		}
		sort.Sort(Int32Slice(vids))
		for _, vid = range vids {
			if v, ok = s.volumes[vid]; ok {
				volumes = append(volumes, v)
			}
		}
		res["server"] = StoreInfo
		res["volumes"] = volumes
		return
	})
	go func() {
		http.ListenAndServe(addr, nil)
	}()
	return
}
