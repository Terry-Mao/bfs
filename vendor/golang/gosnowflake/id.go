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
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "golang/log4go"
)

const (
	twepoch            = int64(1288834974657)
	workerIdBits       = uint(5)
	datacenterIdBits   = uint(5)
	maxWorkerId        = -1 ^ (-1 << workerIdBits)
	maxDatacenterId    = -1 ^ (-1 << datacenterIdBits)
	sequenceBits       = uint(12)
	workerIdShift      = sequenceBits
	datacenterIdShift  = sequenceBits + workerIdBits
	timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits
	sequenceMask       = -1 ^ (-1 << sequenceBits)
	maxNextIdsNum      = 100
)

type IdWorker struct {
	sequence      int64
	lastTimestamp int64
	workerId      int64
	twepoch       int64
	datacenterId  int64
	mutex         sync.Mutex

	atomId *int64
	count  *int64
	record *os.File
}

// NewIdWorker new a snowflake id generator object.
func NewIdWorker(workerId, datacenterId, twepoch, atomStart int64, atomRecord string) (*IdWorker, error) {
	idWorker := &IdWorker{}
	if workerId > maxWorkerId || workerId < 0 {
		log.Error("worker Id can't be greater than %d or less than 0", maxWorkerId)
		return nil, errors.New(fmt.Sprintf("worker Id: %d error", workerId))
	}
	if datacenterId > maxDatacenterId || datacenterId < 0 {
		log.Error("datacenter Id can't be greater than %d or less than 0", maxDatacenterId)
		return nil, errors.New(fmt.Sprintf("datacenter Id: %d error", datacenterId))
	}
	idWorker.workerId = workerId
	idWorker.datacenterId = datacenterId
	idWorker.lastTimestamp = -1
	idWorker.sequence = 0
	idWorker.twepoch = twepoch
	idWorker.mutex = sync.Mutex{}

	count := int64(0)
	idWorker.count = &count
	if atomRecord != "" {
		f, err := os.OpenFile(atomRecord, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Error("can not open file %s, error(%v)", atomRecord, err)
			return nil, err
		}
		r := bufio.NewReader(f)
		line, _, _ := r.ReadLine()
		l := string(line)
		if l != "" {
			i, err := strconv.ParseInt(l, 10, 64)
			if err != nil {
				log.Error("strconv.ParseInt(%s) error(%v)", l, err)
				return nil, err
			}
			atomStart = i + maxNextIdsNum
		}
		idWorker.record = f
	}
	idWorker.atomId = &atomStart
	log.Debug("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d", timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId)
	return idWorker, nil
}

// timeGen generate a unix millisecond.
func timeGen() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// tilNextMillis spin wait till next millisecond.
func tilNextMillis(lastTimestamp int64) int64 {
	timestamp := timeGen()
	for timestamp <= lastTimestamp {
		timestamp = timeGen()
	}
	return timestamp
}

// NextId get a snowflake id.
func (id *IdWorker) NextId() (int64, error) {
	id.mutex.Lock()
	defer id.mutex.Unlock()
	timestamp := timeGen()
	if timestamp < id.lastTimestamp {
		log.Error("clock is moving backwards.  Rejecting requests until %d.", id.lastTimestamp)
		return 0, errors.New(fmt.Sprintf("Clock moved backwards.  Refusing to generate id for %d milliseconds", id.lastTimestamp-timestamp))
	}
	if id.lastTimestamp == timestamp {
		id.sequence = (id.sequence + 1) & sequenceMask
		if id.sequence == 0 {
			timestamp = tilNextMillis(id.lastTimestamp)
		}
	} else {
		id.sequence = 0
	}
	id.lastTimestamp = timestamp
	return ((timestamp - id.twepoch) << timestampLeftShift) | (id.datacenterId << datacenterIdShift) | (id.workerId << workerIdShift) | id.sequence, nil
}

// NextIds get snowflake ids.
func (id *IdWorker) NextIds(num int) ([]int64, error) {
	if num > maxNextIdsNum || num < 0 {
		log.Error("NextIds num can't be greater than %d or less than 0", maxNextIdsNum)
		return nil, errors.New(fmt.Sprintf("NextIds num: %d error", num))
	}
	ids := make([]int64, num)
	id.mutex.Lock()
	defer id.mutex.Unlock()
	for i := 0; i < num; i++ {
		timestamp := timeGen()
		if timestamp < id.lastTimestamp {
			log.Error("clock is moving backwards.  Rejecting requests until %d.", id.lastTimestamp)
			return nil, errors.New(fmt.Sprintf("Clock moved backwards.  Refusing to generate id for %d milliseconds", id.lastTimestamp-timestamp))
		}
		if id.lastTimestamp == timestamp {
			id.sequence = (id.sequence + 1) & sequenceMask
			if id.sequence == 0 {
				timestamp = tilNextMillis(id.lastTimestamp)
			}
		} else {
			id.sequence = 0
		}
		id.lastTimestamp = timestamp
		ids[i] = ((timestamp - id.twepoch) << timestampLeftShift) | (id.datacenterId << datacenterIdShift) | (id.workerId << workerIdShift) | id.sequence
	}
	return ids, nil
}

// AtomId get a atom incr id.
func (id *IdWorker) AtomId() int64 {
	i := atomic.AddInt64(id.atomId, 1)
	c := atomic.AddInt64(id.count, 1)
	if c >= maxNextIdsNum {
		id.record.Seek(0, 0)
		id.record.WriteString(strconv.FormatInt(i, 10) + "\n")
		atomic.StoreInt64(id.count, 1)
	}
	return i
}
