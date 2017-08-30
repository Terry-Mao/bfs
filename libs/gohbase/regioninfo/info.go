// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package regioninfo contains data structures to represent HBase regions.
package regioninfo

import (
	"encoding/binary"
	"fmt"
	"sync"

	"bfs/libs/gohbase/pb"
	"bytes"
	"time"

	log "github.com/golang/glog"

	"github.com/golang/protobuf/proto"
)

const minEstablishInterval = int64(time.Duration(500 * time.Millisecond))

// Info describes a region.
type Info struct {
	// Table name.
	Table []byte

	// RegionName.
	RegionName []byte

	// StartKey
	StartKey []byte

	// StopKey.
	StopKey []byte

	// Once a region becomes unreachable, this channel is created, and any
	// functions that wish to be notified when the region becomes available
	// again can read from this channel, which will be closed when the region
	// is available again
	available     chan struct{}
	availableLock sync.Mutex

	lastUnAvailable int64
	curUnAvailable  int64
}

// InfoFromCell parses a KeyValue from the meta table and creates the
// corresponding Info object.
func InfoFromCell(cell *pb.Cell) (*Info, error) {
	value := cell.Value
	if len(value) == 0 {
		return nil, fmt.Errorf("empty value in %q", cell)
	} else if value[0] != 'P' {
		return nil, fmt.Errorf("unsupported region info version %d in %q",
			value[0], cell)
	}
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"
	magic := binary.BigEndian.Uint32(value)
	if magic != pbufMagic {
		return nil, fmt.Errorf("invalid magic number in %q", cell)
	}
	regInfo := &pb.RegionInfo{}
	err := proto.UnmarshalMerge(value[4:len(value)-4], regInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to decode %q: %s", cell, err)
	}
	return &Info{
		Table:         regInfo.TableName.Qualifier,
		RegionName:    cell.Row,
		StartKey:      regInfo.StartKey,
		StopKey:       regInfo.EndKey,
		availableLock: sync.Mutex{},
	}, nil
}

func (i *Info) Equals(i1 *Info) bool {
	if i == nil {
		return i1 == nil
	}
	if i1 == nil {
		return false
	}
	return bytes.Equal(i.Table, i1.Table) && bytes.Equal(i.RegionName, i1.RegionName) && bytes.Equal(i.StartKey, i1.StartKey) && bytes.Equal(i.StopKey, i1.StopKey)
}

// IsUnavailable returns true if this region has been marked as unavailable.
func (i *Info) IsUnavailable() bool {
	i.availableLock.Lock()
	res := i.available != nil
	i.availableLock.Unlock()
	return res
}

// GetAvailabilityChan returns a channel that can be used to wait on for
// notification that a connection to this region has been reestablished.
// If this region is not marked as unavailable, nil will be returned.
func (i *Info) GetAvailabilityChan() <-chan struct{} {
	i.availableLock.Lock()
	ch := i.available
	i.availableLock.Unlock()
	return ch
}

// MarkUnavailable will mark this region as unavailable, by creating the struct
// returned by GetAvailabilityChan. If this region was marked as available
// before this, true will be returned.
func (i *Info) MarkUnavailable() bool {
	created := false
	i.availableLock.Lock()
	if i.available == nil {
		i.available = make(chan struct{})
		created = true
		//i.lastUnAvailable, i.curUnAvailable = i.curUnAvailable, time.Now().UnixNano()
		i.lastUnAvailable = i.curUnAvailable
		i.curUnAvailable = time.Now().UnixNano()
	}
	i.availableLock.Unlock()
	return created
}

// MarkAvailable will mark this region as available again, by closing the struct
// returned by GetAvailabilityChan
func (i *Info) MarkAvailable() {
	i.availableLock.Lock()
	ch := i.available
	i.available = nil
	close(ch)
	i.availableLock.Unlock()
}

func (i *Info) String() string {
	return fmt.Sprintf("*regioninfo.Info{Table: %q, RegionName: %q, StartKey: %q, StopKey: %q, lastUnAvailable: %d. curUnAvailable: %d}",
		i.Table, i.RegionName, i.StartKey, i.StopKey, i.lastUnAvailable, i.curUnAvailable)
}

func (i *Info) Park4Establish() {
	if i != nil && i.curUnAvailable != 0 && i.lastUnAvailable != 0 && i.curUnAvailable > i.lastUnAvailable && (i.curUnAvailable-i.lastUnAvailable) < minEstablishInterval {
		d := time.Duration(minEstablishInterval - i.curUnAvailable + i.lastUnAvailable)
		log.Info("park for establish for (%v)", d)
		time.Sleep(d)
	}
}

func (i *Info) DupExtInfo(origInfo *Info) {
	if i != nil && origInfo != nil {
		i.curUnAvailable = origInfo.curUnAvailable
		i.lastUnAvailable = origInfo.lastUnAvailable
	}
}

// CompareGeneric is the same thing as Compare but for interface{}.
func CompareGeneric(a, b interface{}) int {
	return Compare(a.([]byte), b.([]byte))
}

// Compare compares two region names.
// We can't just use bytes.Compare() because it doesn't play nicely
// with the way META keys are built as the first region has an empty start
// key.  Let's assume we know about those 2 regions in our cache:
//   .META.,,1
//   tableA,,1273018455182
// We're given an RPC to execute on "tableA", row "\x00" (1 byte row key
// containing a 0).  If we use Compare() to sort the entries in the cache,
// when we search for the entry right before "tableA,\000,:"
// we'll erroneously find ".META.,,1" instead of the entry for first
// region of "tableA".
//
// Since this scheme breaks natural ordering, we need this comparator to
// implement a special version of comparison to handle this scenario.
func Compare(a, b []byte) int {
	var length int
	if la, lb := len(a), len(b); la < lb {
		length = la
	} else {
		length = lb
	}
	// Reminder: region names are of the form:
	//   table_name,start_key,timestamp[.MD5.]
	// First compare the table names.
	var i int
	for i = 0; i < length; i++ {
		ai := a[i] // Saves one pointer deference every iteration.
		bi := b[i] // Saves one pointer deference every iteration.
		if ai != bi {
			// The name of the tables differ.
			if ai == ',' {
				return -1001 // `a' has a smaller table name.  a < b
			} else if bi == ',' {
				return 1001 // `b' has a smaller table name.  a > b
			}
			return int(ai) - int(bi)
		}
		if ai == ',' {
			// Remember: at this point ai == bi.
			break // We're done comparing the table names.  They're equal.
		}
	}

	// Now find the last comma in both `a' and `b'.  We need to start the
	// search from the end as the row key could have an arbitrary number of
	// commas and we don't know its length.
	aComma := findCommaFromEnd(a, i)
	bComma := findCommaFromEnd(b, i)
	// If either `a' or `b' is followed immediately by another comma, then
	// they are the first region (it's the empty start key).
	i++ // No need to check against `length', there MUST be more bytes.

	// Compare keys.
	var firstComma int
	if aComma < bComma {
		firstComma = aComma
	} else {
		firstComma = bComma
	}
	for ; i < firstComma; i++ {
		ai := a[i]
		bi := b[i]
		if ai != bi {
			// The keys differ.
			return int(ai) - int(bi)
		}
	}
	if aComma < bComma {
		return -1002 // `a' has a shorter key.  a < b
	} else if bComma < aComma {
		return 1002 // `b' has a shorter key.  a > b
	}

	// Keys have the same length and have compared identical.  Compare the
	// rest, which essentially means: use start code as a tie breaker.
	for ; /*nothing*/
	i < length; i++ {
		ai := a[i]
		bi := b[i]
		if ai != bi {
			// The start codes differ.
			return int(ai) - int(bi)
		}
	}

	return len(a) - len(b)
}

// Because there is no `LastIndexByte()' in the standard `bytes' package.
func findCommaFromEnd(b []byte, offset int) int {
	for i := len(b) - 1; i > offset; i-- {
		if b[i] == ',' {
			return i
		}
	}
	panic(fmt.Errorf("No comma found in %q after offset %d", b, offset))
}
