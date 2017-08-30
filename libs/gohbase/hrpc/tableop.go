// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"errors"

	"bfs/libs/gohbase/filter"
)

// tableOp represents an administrative operation on a table.
type tableOp struct {
	base
}

// SetFilter always returns an error.
func (to *tableOp) SetFilter(filter.Filter) error {
	// Doesn't make sense on this kind of RPC.
	return errors.New("Cannot set filter on admin operations.")
}

// SetFamilies always returns an error.
func (to *tableOp) SetFamilies(map[string][]string) error {
	// Doesn't make sense on this kind of RPC.
	return errors.New("Cannot set families on admin operations.")
}
