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
	"flag"
	"runtime"

	log "golang/log4go"
)

func main() {
	flag.Parse()
	// config
	if err := InitConfig(); err != nil {
		panic(err)
	}
	runtime.GOMAXPROCS(MyConf.MaxProc)
	// init log
	log.LoadConfiguration(MyConf.Log)
	log.Info("gosnowflake service start [datacenter: %d]", MyConf.DatacenterId)
	// process
	if err := InitProcess(); err != nil {
		panic(err)
	}
	// pprof
	InitPprof()
	// zookeeper
	if err := InitZK(); err != nil {
		panic(err)
	}
	defer CloseZK()
	// safty check
	if err := SanityCheckPeers(); err != nil {
		panic(err)
	}
	// workers
	workers, err := NewWorkers()
	if err != nil {
		panic(err)
	}
	// rpc
	if err := InitRPC(workers); err != nil {
		panic(err)
	}
	// init signals, block wait signals
	sc := InitSignal()
	HandleSignal(sc)
	log.Info("gosnowflake service stop")
}
