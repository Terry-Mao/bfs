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

// Reference: https://github.com/twitter/snowflake

package main

import (
	"errors"
	"fmt"

	log "golang/log4go"
)

type Workers []*IdWorker

// NewWorkers new id workers instance.
func NewWorkers() (Workers, error) {
	idWorkers := make([]*IdWorker, maxWorkerId)
	for _, workerId := range MyConf.WorkerId {
		if t := idWorkers[workerId]; t != nil {
			log.Error("init workerId: %d already exists", workerId)
			return nil, fmt.Errorf("init workerId: %d exists", workerId)
		}
		idWorker, err := NewIdWorker(workerId, MyConf.DatacenterId, MyConf.Twepoch, MyConf.AtomStart, MyConf.AtomRecord)
		if err != nil {
			log.Error("NewIdWorker(%d, %d) error(%v)", MyConf.DatacenterId, workerId)
			return nil, err
		}
		idWorkers[workerId] = idWorker
		if err := RegWorkerId(workerId); err != nil {
			log.Error("RegWorkerId(%d) error(%v)", workerId, err)
			return nil, err
		}
	}
	return Workers(idWorkers), nil
}

// Get get a specified worker by workerId.
func (w Workers) Get(workerId int64) (*IdWorker, error) {
	if workerId > maxWorkerId || workerId < 0 {
		log.Error("worker Id can't be greater than %d or less than 0", maxWorkerId)
		return nil, errors.New(fmt.Sprintf("worker Id: %d error", workerId))
	}
	if worker := w[workerId]; worker == nil {
		log.Warn("workerId: %d not register", workerId)
		return nil, fmt.Errorf("snowflake workerId: %d don't register in this service", workerId)
	} else {
		return worker, nil
	}
}
