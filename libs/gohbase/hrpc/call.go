// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"sync"
	"unsafe"

	"bfs/libs/gohbase/filter"
	"bfs/libs/gohbase/pb"
	"bfs/libs/gohbase/regioninfo"

	"github.com/golang/protobuf/proto"
)

type CallType struct {
	Value int
	Name  string
}

type TimeRange struct {
	From uint64
	To   uint64
}

func (tr TimeRange) Valid() bool {
	return tr.From > 0 && tr.To > 0
}

var (
	CallTypeCall CallType = CallType{0, "Call"}
	CallTypeGet  CallType = CallType{1, "Get"}
	CallTypeScan CallType = CallType{2, "Scan"}
	// mutate: >= 100
	CallTypeAppend    CallType = CallType{100, "Append"}
	CallTypeIncrement CallType = CallType{101, "Increment"}
	CallTypePut       CallType = CallType{102, "Put"}
	CallTypeDelete    CallType = CallType{103, "Delete"}
)

var (
	NotGeneralCallErr error = errors.New("not general call")
)

func (ct CallType) IsMutate() bool {
	return ct.Value >= 100
}

func (ct CallType) GeneralCall() bool {
	return ct != CallTypeScan // && ct != CallTypeIncrement
}

// Call represents an HBase RPC call.
type Call interface {
	Table() []byte

	Key() []byte

	GetRegion() *regioninfo.Info
	SetRegion(region *regioninfo.Info)
	GetName() string
	Serialize() ([]byte, error)
	// Returns a newly created (default-state) protobuf in which to store the
	// response of this call.
	NewResponse() proto.Message

	GetResultChan() chan RPCResult

	GetContext() context.Context

	SetFamilies(fam map[string][]string) error
	SetFilter(ft filter.Filter) error

	CallType() CallType
}

type Calls struct {
	Calls []Call
	Ctx   context.Context
}

func NewCalls(cs []Call, ctx context.Context) *Calls {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Calls{
		Calls: cs,
		Ctx:   ctx,
	}
}

// RPCResult is struct that will contain both the resulting message from an RPC
// call, and any errors that may have occurred related to making the RPC call.
type RPCResult struct {
	Msg   proto.Message
	Error error
}

type base struct {
	table []byte

	key []byte

	region *regioninfo.Info

	// Protects access to resultch.
	resultchLock sync.Mutex

	resultch chan RPCResult

	ctx context.Context

	ct CallType
}

func (b *base) CallType() CallType {
	return b.ct
}

func (b *base) GetContext() context.Context {
	return b.ctx
}

func (b *base) GetRegion() *regioninfo.Info {
	return b.region
}

func (b *base) SetRegion(region *regioninfo.Info) {
	b.region = region
}

func (b *base) regionSpecifier() *pb.RegionSpecifier {
	regionType := pb.RegionSpecifier_REGION_NAME
	return &pb.RegionSpecifier{
		Type:  &regionType,
		Value: []byte(b.region.RegionName),
	}
}

func applyOptions(call Call, options ...func(Call) error) error {
	for _, option := range options {
		err := option(call)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *base) Table() []byte {
	return b.table
}

func (b *base) Key() []byte {
	return b.key
}

func (b *base) GetResultChan() chan RPCResult {
	b.resultchLock.Lock()
	if b.resultch == nil {
		// Buffered channels, so that if a writer thread sends a message (or
		// reports an error) after the deadline it doesn't block due to the
		// requesting thread having moved on.
		b.resultch = make(chan RPCResult, 1)
	}
	b.resultchLock.Unlock()
	return b.resultch
}

// Families is used as a parameter for request creation. Adds families constraint to a request.
func Families(fam map[string][]string) func(Call) error {
	return func(g Call) error {
		return g.SetFamilies(fam)
	}
}

// Filters is used as a parameter for request creation. Adds filters constraint to a request.
func Filters(fl filter.Filter) func(Call) error {
	return func(g Call) error {
		return g.SetFilter(fl)
	}
}

// Cell is the smallest level of granularity in returned results.
// Represents a single cell in HBase (a row will have one cell for every qualifier).
type Cell pb.Cell

// Result holds a slice of Cells as well as miscellaneous information about the response.
type Result struct {
	Cells  []*Cell
	Exists *bool
	Stale  *bool
	// Any other variables we want to include.
}

// ToLocalResult takes a protobuf Result type and converts it to our own
// Result type in constant time.
func ToLocalResult(pbr *pb.Result) *Result {
	if pbr == nil {
		return &Result{}
	}
	return &Result{
		// Should all be O(1) operations.
		Cells:  toLocalCells(pbr),
		Exists: pbr.Exists,
		Stale:  pbr.Stale,
	}
}

func toLocalCells(pbr *pb.Result) []*Cell {
	return *(*[]*Cell)(unsafe.Pointer(pbr))
}

// We can now define any helper functions on Result that we want.
