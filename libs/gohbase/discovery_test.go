// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"testing"

	"bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/pb"
	"bfs/libs/gohbase/regioninfo"
	"time"
)

func TestRegionDiscovery(t *testing.T) {

	client := newClient(standardClient, conf.NewConf([]string{"~invalid.quorum"}, "", "", "", 30*time.Second, 0, 0, 0))

	reg := client.getRegionFromCache([]byte("test"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %#v even though the cache was empty?!", reg)
	}

	// Inject a "test" table with a single region that covers the entire key
	// space (both the start and stop keys are empty).
	family := []byte("info")
	metaRow := &pb.GetResponse{
		Result: &pb.Result{Cell: []*pb.Cell{
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("regioninfo"),
				Value: []byte("PBUF\b\xc4\xcd\xe9\x99\xe0)\x12\x0f\n\adefault\x12\x04test" +
					"\x1a\x00\"\x00(\x000\x008\x00"),
			},
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("seqnumDuringOpen"),
				Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
			},
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("server"),
				Value:     []byte("localhost:50966"),
			},
			&pb.Cell{
				Row:       []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
				Family:    family,
				Qualifier: []byte("serverstartcode"),
				Value:     []byte("\x00\x00\x01N\x02\x92R\xb1"),
			},
		}}}

	reg, _, _, err := client.parseMetaTableResponse(metaRow)
	if err != nil {
		t.Fatalf("Failed to discover region: %s", err)
	}
	client.regions.put(reg.RegionName, reg)

	reg = client.getRegionFromCache([]byte("test"), []byte("theKey"))
	if reg == nil {
		t.Fatal("Region not found even though we injected it in the cache.")
	}
	expected := &regioninfo.Info{
		Table:      []byte("test"),
		RegionName: []byte("test,,1234567890042.56f833d5569a27c7a43fbf547b4924a4."),
		StartKey:   []byte(""),
		StopKey:    []byte(""),
	}
	if !bytes.Equal(reg.Table, expected.Table) ||
		!bytes.Equal(reg.RegionName, expected.RegionName) ||
		!bytes.Equal(reg.StartKey, expected.StartKey) ||
		!bytes.Equal(reg.StopKey, expected.StopKey) {
		t.Errorf("Found region %#v \nbut expected %#v", reg, expected)
	}

	reg = client.getRegionFromCache([]byte("notfound"), []byte("theKey"))
	if reg != nil {
		t.Errorf("Found region %#v even though this table doesn't exist", reg)
	}
}
