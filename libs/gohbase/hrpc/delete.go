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

// DeleteTable represents a DeleteTable HBase call
type DeleteTable struct {
	tableOp
}

// NewDeleteTable creates a new DeleteTable request that will delete the
// given table in HBase. For use by the admin client.
func NewDeleteTable(ctx context.Context, table []byte) *DeleteTable {
	dt := &DeleteTable{
		tableOp{base{
			table: table,
			ctx:   ctx,
		}},
	}
	return dt
}

// GetName returns the name of this RPC call.
func (dt *DeleteTable) GetName() string {
	return "DeleteTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (dt *DeleteTable) Serialize() ([]byte, error) {
	dtreq := &pb.DeleteTableRequest{
		TableName: &pb.TableName{
			Namespace: []byte("default"),
			Qualifier: dt.table,
		},
	}
	return proto.Marshal(dtreq)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (dt *DeleteTable) NewResponse() proto.Message {
	return &pb.DeleteTableResponse{}
}
