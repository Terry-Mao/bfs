// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"

	"bfs/libs/gohbase/filter"
	"bfs/libs/gohbase/pb"

	"github.com/golang/protobuf/proto"
)

// Scan represents a scanner on an HBase table.
type Scan struct {
	base

	// Maps a column family to a list of qualifiers
	families map[string][]string

	closeScanner bool

	startRow []byte
	stopRow  []byte

	timeRange TimeRange

	scannerID *uint64

	caching int
	limit   int

	filters filter.Filter
}

// NewScan creates a scanner for the given table.
func NewScan(ctx context.Context, table []byte, options ...func(Call) error) (*Scan, error) {
	scan := &Scan{
		base: base{
			table: table,
			ctx:   ctx,
		},
		closeScanner: false,
	}
	err := applyOptions(scan, options...)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

// NewScanRange creates a scanner for the given table and key range.
// The range is half-open, i.e. [startRow; stopRow[ -- stopRow is not
// included in the range.
func NewScanRange(ctx context.Context, table, startRow, stopRow []byte,
	options ...func(Call) error) (*Scan, error) {
	scan := &Scan{
		base: base{
			table: table,
			key:   startRow,
			ctx:   ctx,
		},
		closeScanner: false,
		startRow:     startRow,
		stopRow:      stopRow,
	}
	err := applyOptions(scan, options...)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

// NewScanStr creates a scanner for the given table.
func NewScanStr(ctx context.Context, table string, options ...func(Call) error) (*Scan, error) {
	return NewScan(ctx, []byte(table), options...)
}

// NewScanRangeStr creates a scanner for the given table and key range.
// The range is half-open, i.e. [startRow; stopRow[ -- stopRow is not
// included in the range.
func NewScanRangeStr(ctx context.Context, table, startRow, stopRow string,
	options ...func(Call) error) (*Scan, error) {
	return NewScanRange(ctx, []byte(table), []byte(startRow), []byte(stopRow), options...)
}

// NewScanFromID creates a new Scan request that will return additional
// results from the given scanner ID.  This is an internal method, users
// are not expected to deal with scanner IDs.
func NewScanFromID(ctx context.Context, table []byte,
	scannerID uint64, startRow []byte) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			key:   []byte(startRow),
			ctx:   ctx,
		},
		scannerID:    &scannerID,
		closeScanner: false,
	}
}

// NewCloseFromID creates a new Scan request that will close the scanner for
// the given scanner ID.  This is an internal method, users are not expected
// to deal with scanner IDs.
func NewCloseFromID(ctx context.Context, table []byte,
	scannerID uint64, startRow []byte) *Scan {
	return &Scan{
		base: base{
			table: []byte(table),
			key:   []byte(startRow),
			ctx:   ctx,
		},
		scannerID:    &scannerID,
		closeScanner: true,
	}
}

func (s *Scan) SetTimeRange(tr TimeRange) {
	s.timeRange = tr
}

// GetName returns the name of this RPC call.
func (s *Scan) GetName() string {
	return "Scan"
}

// GetStopRow returns the end key (exclusive) of this scanner.
func (s *Scan) GetStopRow() []byte {
	return s.stopRow
}

// GetStartRow returns the start key (inclusive) of this scanner.
func (s *Scan) GetStartRow() []byte {
	return s.startRow
}

// GetFamilies returns the set families covered by this scanner.
// If no families are specified then all the families are scanned.
func (s *Scan) GetFamilies() map[string][]string {
	return s.families
}

// GetRegionStop returns the stop key of the region currently being scanned.
// This is an internal method, end users are not expected to use it.
func (s *Scan) GetRegionStop() []byte {
	return s.region.StopKey
}

// GetFilter returns the filter set on this scanner.
func (s *Scan) GetFilter() filter.Filter {
	return s.filters
}

// Serialize converts this Scan into a serialized protobuf message ready
// to be sent to an HBase node.
func (s *Scan) Serialize() ([]byte, error) {
	scan := &pb.ScanRequest{
		Region:       s.regionSpecifier(),
		CloseScanner: &s.closeScanner,
	}
	if s.caching > 0 {
		scan.NumberOfRows = proto.Uint32(uint32(s.caching))
	} else {
		scan.NumberOfRows = proto.Uint32(20)
	}
	if s.scannerID == nil {
		scan.Scan = &pb.Scan{
			Column:   familiesToColumn(s.families),
			StartRow: s.startRow,
			StopRow:  s.stopRow,
		}
		if s.timeRange.Valid() {
			from, to := s.timeRange.From, s.timeRange.To
			scan.Scan.TimeRange = &pb.TimeRange{
				From: &from,
				To:   &to,
			}
		}
		if s.filters != nil {
			pbFilter, err := s.filters.ConstructPBFilter()
			if err != nil {
				return nil, err
			}
			scan.Scan.Filter = pbFilter
		}
	} else {
		scan.ScannerId = s.scannerID
	}
	return proto.Marshal(scan)
}

// NewResponse creates an empty protobuf message to read the response
// of this RPC.
func (s *Scan) NewResponse() proto.Message {
	return &pb.ScanResponse{}
}

// SetFamilies sets the families covered by this scanner.
func (s *Scan) SetFamilies(fam map[string][]string) error {
	s.families = fam
	return nil
}

// SetFilter sets the request's filter.
func (s *Scan) SetFilter(ft filter.Filter) error {
	s.filters = ft
	return nil
}

func (s *Scan) SetCaching(caching int) {
	s.caching = caching
}

func (s *Scan) SetLimit(limit int) {
	s.limit = limit
}

func (s *Scan) Limit() int {
	return s.limit
}
