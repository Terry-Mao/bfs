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

// Get represents a Get HBase call.
type Get struct {
	base

	families map[string][]string //Maps a column family to a list of qualifiers

	// Return the row for the given key or, if this key doesn't exist,
	// whichever key happens to be right before.
	closestBefore bool

	// Don't return any KeyValue, just say whether the row key exists in the
	// table or not.
	existsOnly bool

	timeRange TimeRange

	filters filter.Filter
}

// NewGet creates a new Get request for the given table and row key.
func NewGet(ctx context.Context, table, key []byte,
	options ...func(Call) error) (*Get, error) {
	g := &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   ctx,
			ct:    CallTypeGet,
		},
	}
	err := applyOptions(g, options...)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// NewGetStr creates a new Get request for the given table and row key.
func NewGetStr(ctx context.Context, table, key string,
	options ...func(Call) error) (*Get, error) {
	return NewGet(ctx, []byte(table), []byte(key), options...)
}

// NewGetBefore creates a new Get request for the row with a key equal to or
// immediately less than the given key, in the given table.
func NewGetBefore(ctx context.Context, table, key []byte,
	options ...func(Call) error) (*Get, error) {
	g := &Get{
		base: base{
			table: table,
			key:   key,
			ctx:   ctx,
			ct:    CallTypeGet,
		},
		closestBefore: true,
	}
	err := applyOptions(g, options...)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Get) SetTimeRange(tr TimeRange) {
	g.timeRange = tr
}

// GetName returns the name of this RPC call.
func (g *Get) GetName() string {
	return "Get"
}

// GetFilter returns the filter of this Get request.
func (g *Get) GetFilter() filter.Filter {
	return g.filters
}

// GetFamilies returns the families to retrieve with this Get request.
func (g *Get) GetFamilies() map[string][]string {
	return g.families
}

// SetFilter sets filter to use for this Get request.
func (g *Get) SetFilter(f filter.Filter) error {
	g.filters = f
	// TODO: Validation?
	return nil
}

// SetFamilies sets families to retrieve with this Get request.
func (g *Get) SetFamilies(f map[string][]string) error {
	g.families = f
	// TODO: Validation?
	return nil
}

// ExistsOnly makes this Get request not return any KeyValue, merely whether
// or not the given row key exists in the table.
func (g *Get) ExistsOnly() error {
	g.existsOnly = true
	return nil
}

// Serialize serializes this RPC into a buffer.
func (g *Get) Serialize() ([]byte, error) {
	get := &pb.GetRequest{
		Region: g.regionSpecifier(),
		Get: &pb.Get{
			Row:    g.key,
			Column: familiesToColumn(g.families),
		},
	}
	if g.timeRange.Valid() {
		from, to := g.timeRange.From, g.timeRange.To
		get.Get.TimeRange = &pb.TimeRange{
			From: &from,
			To:   &to,
		}
	}
	if g.closestBefore {
		get.Get.ClosestRowBefore = proto.Bool(true)
	}
	if g.existsOnly {
		get.Get.ExistenceOnly = proto.Bool(true)
	}
	if g.filters != nil {
		pbFilter, err := g.filters.ConstructPBFilter()
		if err != nil {
			return nil, err
		}
		get.Get.Filter = pbFilter
	}
	return proto.Marshal(get)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (g *Get) NewResponse() proto.Message {
	return &pb.GetResponse{}
}

// familiesToColumn takes a map from strings to lists of strings, and converts
// them into protobuf Columns
func familiesToColumn(families map[string][]string) []*pb.Column {
	cols := make([]*pb.Column, len(families))
	counter := 0
	for family, qualifiers := range families {
		bytequals := make([][]byte, len(qualifiers))
		for i, qual := range qualifiers {
			bytequals[i] = []byte(qual)
		}
		cols[counter] = &pb.Column{
			Family:    []byte(family),
			Qualifier: bytequals,
		}
		counter++
	}
	return cols
}
