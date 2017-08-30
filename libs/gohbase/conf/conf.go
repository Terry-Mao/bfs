package conf

import "time"

// Conf RpcQueueSize <= 0 && FlushInterval <= 0 is not allowed
type Conf struct {
	ZkRoot                                string
	Zkquorum                              []string
	Master, Meta                          string
	RpcQueueSize                          int
	ZkTimeout, FlushInterval, DialTimeout time.Duration
}

func NewConf(zkquorum []string, zkRoot, master, meta string, zkTimeout time.Duration, rpcQueueSize int, flushInterval, dialTimeout time.Duration) (res *Conf) {
	// set default value
	if rpcQueueSize <= 0 && flushInterval <= 0 {
		rpcQueueSize = 1
	}
	if dialTimeout == 0 {
		dialTimeout = 10 * time.Second
	}
	res = &Conf{
		ZkRoot:        zkRoot,
		Zkquorum:      zkquorum,
		Master:        master,
		Meta:          meta,
		ZkTimeout:     zkTimeout,
		RpcQueueSize:  rpcQueueSize,
		FlushInterval: flushInterval,
		DialTimeout:   dialTimeout,
	}

	return
}
