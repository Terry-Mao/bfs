// Copyright © 2014 Terry Mao All rights reserved.
// This file is part of gosnowflake.

// gosnowflake is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// gosnowflake is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with gosnowflake.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"errors"
	myrpc "golang/gosnowflake/rpc"
	"net"
	"net/rpc"
	"time"

	log "golang/log4go"
)

type SnowflakeRPC struct {
	workers Workers
}

// StartRPC start rpc listen.
func InitRPC(workers Workers) error {
	s := &SnowflakeRPC{workers: workers}
	rpc.Register(s)
	for _, bind := range MyConf.RPCBind {
		log.Info("start listen rpc addr: \"%s\"", bind)
		go rpcListen(bind)
	}
	return nil
}

// rpcListen start rpc listen.
func rpcListen(bind string) {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		log.Error("net.Listen(\"tcp\", \"%s\") error(%v)", bind, err)
		panic(err)
	}
	// if process exit, then close the rpc bind
	defer func() {
		log.Info("rpc addr: \"%s\" close", bind)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()
	rpc.Accept(l)
}

// NextId generate a id.
func (s *SnowflakeRPC) NextId(workerId int64, id *int64) error {
	worker, err := s.workers.Get(workerId)
	if err != nil {
		return err
	}
	if tid, err := worker.NextId(); err != nil {
		log.Error("worker.NextId() error(%v)", err)
		return err
	} else {
		*id = tid
		return nil
	}
}

// NextIds generate specified num ids.
func (s *SnowflakeRPC) NextIds(args *myrpc.NextIdsArgs, ids *[]int64) error {
	if args == nil {
		return errors.New("args is nil")
	}
	worker, err := s.workers.Get(args.WorkerId)
	if err != nil {
		return err
	}
	if tids, err := worker.NextIds(args.Num); err != nil {
		log.Error("worker.NextIds(%d) error(%v)", args.Num, err)
		return err
	} else {
		*ids = tids
		return nil
	}
}

// AtomId atomic add id.
func (s *SnowflakeRPC) AtomId(workerId int64, id *int64) error {
	worker, err := s.workers.Get(workerId)
	if err != nil {
		return err
	}
	*id = worker.AtomId()
	return nil
}

// DatacenterId return the services's datacenterId.
func (s *SnowflakeRPC) DatacenterId(ignore int, dataCenterId *int64) error {
	*dataCenterId = MyConf.DatacenterId
	return nil
}

// Timestamp return the service current unix seconds.
func (s *SnowflakeRPC) Timestamp(ignore int, timestamp *int64) error {
	*timestamp = time.Now().Unix()
	return nil
}

// Ping return the service status.
func (s *SnowflakeRPC) Ping(ignore int, status *int) error {
	*status = 0
	return nil
}
