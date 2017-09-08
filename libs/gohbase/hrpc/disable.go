// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"

	"bfs/libs/gohbase/pb"

	"github.com/golang/protobuf/proto"
)

// DisableTable represents a DisableTable HBase call
type DisableTable struct {
	tableOp
}

// NewDisableTable creates a new DisableTable request that will disable the
// given table in HBase. For use by the admin client.
func NewDisableTable(ctx context.Context, table []byte) *DisableTable {
	dt := &DisableTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
	return dt
}

// GetName returns the name of this RPC call.
func (dt *DisableTable) GetName() string {
	return "DisableTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (dt *DisableTable) Serialize() ([]byte, error) {
	dtreq := &pb.DisableTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: dt.table,
		},
	}
	return proto.Marshal(dtreq)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DisableTable) NewResponse() proto.Message {
	return &pb.DisableTableResponse{}
}
