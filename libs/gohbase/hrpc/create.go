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

// CreateTable represents a CreateTable HBase call
type CreateTable struct {
	tableOp

	columns []string
}

// NewCreateTable creates a new CreateTable request that will create the given
// table in HBase. For use by the admin client.
func NewCreateTable(ctx context.Context, table []byte, columns []string) *CreateTable {
	ct := &CreateTable{
		tableOp: tableOp{base{
			table: table,
			ctx:   ctx,
		}},
		columns: columns,
	}
	return ct
}

// GetName returns the name of this RPC call.
func (ct *CreateTable) GetName() string {
	return "CreateTable"
}

// Serialize will convert this HBase call into a slice of bytes to be written to
// the network
func (ct *CreateTable) Serialize() ([]byte, error) {
	pbcols := make([]*pb.ColumnFamilySchema, len(ct.columns))
	for i, col := range ct.columns {
		pbcols[i] = &pb.ColumnFamilySchema{
			Name: []byte(col),
		}
	}
	ctable := &pb.CreateTableRequest{
		TableSchema: &pb.TableSchema{
			TableName: &pb.TableName{
				Namespace: []byte("default"),
				Qualifier: ct.table,
			},
			ColumnFamilies: pbcols,
		},
	}
	return proto.Marshal(ctable)
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (ct *CreateTable) NewResponse() proto.Message {
	return &pb.CreateTableResponse{}
}
