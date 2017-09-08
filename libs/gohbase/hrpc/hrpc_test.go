// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"bfs/libs/gohbase/filter"
	"bfs/libs/gohbase/regioninfo"
)

func TestNewGet(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	key := "45"
	keyb := []byte(key)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	get, err := NewGet(ctx, tableb, keyb)
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, nil, nil) {
		t.Errorf("Get1 didn't set attributes correctly.")
	}
	get, err = NewGetStr(ctx, table, key)
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, nil, nil) {
		t.Errorf("Get2 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Families(fam))
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, fam, nil) {
		t.Errorf("Get3 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, nil, filter1) {
		t.Errorf("Get4 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1), Families(fam))
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, fam, filter1) {
		t.Errorf("Get5 didn't set attributes correctly.")
	}
	get, err = NewGet(ctx, tableb, keyb, Filters(filter1))
	err = Families(fam)(get)
	if err != nil || !confirmGetAttributes(get, ctx, tableb, keyb, fam, filter1) {
		t.Errorf("Get6 didn't set attributes correctly.")
	}

}

func confirmGetAttributes(g *Get, ctx context.Context, table, key []byte,
	fam map[string][]string, filter1 filter.Filter) bool {
	if g.GetContext() != ctx ||
		!bytes.Equal(g.Table(), table) ||
		!bytes.Equal(g.Key(), key) ||
		!reflect.DeepEqual(g.GetFamilies(), fam) ||
		reflect.TypeOf(g.GetFilter()) != reflect.TypeOf(filter1) {
		return false
	}
	return true
}

func TestNewScan(t *testing.T) {
	ctx := context.Background()
	table := "test"
	tableb := []byte(table)
	fam := make(map[string][]string)
	fam["info"] = []string{"c1"}
	filter1 := filter.NewFirstKeyOnlyFilter()
	start := "0"
	stop := "100"
	startb := []byte("0")
	stopb := []byte("100")
	scan, err := NewScan(ctx, tableb)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, nil, nil, nil, nil) {
		t.Errorf("Scan1 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, startb, stopb, nil, nil) {
		t.Errorf("Scan2 didn't set attributes correctly.")
	}
	scan, err = NewScanStr(ctx, table)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, nil, nil, nil, nil) {
		t.Errorf("Scan3 didn't set attributes correctly.")
	}
	scan, err = NewScanRangeStr(ctx, table, start, stop)
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, startb, stopb, nil, nil) {
		t.Errorf("Scan4 didn't set attributes correctly.")
	}
	scan, err = NewScanRange(ctx, tableb, startb, stopb, Families(fam), Filters(filter1))
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, startb, stopb, fam, filter1) {
		t.Errorf("Scan5 didn't set attributes correctly.")
	}
	scan, err = NewScan(ctx, tableb, Filters(filter1), Families(fam))
	if err != nil || !confirmScanAttributes(scan, ctx, tableb, nil, nil, fam, filter1) {
		t.Errorf("Scan6 didn't set attributes correctly.")
	}
}

func confirmScanAttributes(s *Scan, ctx context.Context, table, start, stop []byte,
	fam map[string][]string, filter1 filter.Filter) bool {
	if s.GetContext() != ctx ||
		!bytes.Equal(s.Table(), table) ||
		!bytes.Equal(s.GetStartRow(), start) ||
		!bytes.Equal(s.GetStopRow(), stop) ||
		!reflect.DeepEqual(s.GetFamilies(), fam) ||
		reflect.TypeOf(s.GetFilter()) != reflect.TypeOf(filter1) {
		return false
	}
	return true
}

func BenchmarkMutateSerializeWithNestedMaps(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := map[string]map[string][]byte{
			"cf": map[string][]byte{
				"a": []byte{10},
				"b": []byte{20},
				"c": []byte{30, 0},
				"d": []byte{40, 0, 0, 0},
				"e": []byte{50, 0, 0, 0, 0, 0, 0, 0},
				"f": []byte{60},
				"g": []byte{70},
				"h": []byte{80, 0},
				"i": []byte{90, 0, 0, 0},
				"j": []byte{100, 0, 0, 0, 0, 0, 0, 0},
				"k": []byte{0, 0, 220, 66},
				"l": []byte{0, 0, 0, 0, 0, 0, 94, 64},
				"m": []byte{0, 0, 2, 67, 0, 0, 0, 0},
				"n": []byte{0, 0, 0, 0, 0, 128, 97, 64, 0, 0, 0, 0, 0, 0, 0, 0},
				"o": []byte{150},
				"p": []byte{4, 8, 15, 26, 23, 42},
				"q": []byte{1, 1, 3, 5, 8, 13, 21, 34, 55},
				"r": []byte("This is a test string."),
			},
		}
		mutate, err := NewPutStr(context.Background(), "", "", data)
		if err != nil {
			b.Errorf("Error creating mutate: %v", err)
		}
		mutate.SetRegion(&regioninfo.Info{})
		mutate.Serialize()
	}
}

func BenchmarkMutateSerializeWithReflection(b *testing.B) {
	b.ReportAllocs()

	type teststr struct {
		AnInt       int        `hbase:"cf:a"`
		AnInt8      int8       `hbase:"cf:b"`
		AnInt16     int16      `hbase:"cf:c"`
		AnInt32     int32      `hbase:"cf:d"`
		AnInt64     int64      `hbase:"cf:e"`
		AnUInt      uint       `hbase:"cf:f"`
		AnUInt8     uint8      `hbase:"cf:g"`
		AnUInt16    uint16     `hbase:"cf:h"`
		AnUInt32    uint32     `hbase:"cf:i"`
		AnUInt64    uint64     `hbase:"cf:j"`
		AFloat32    float32    `hbase:"cf:k"`
		AFloat64    float64    `hbase:"cf:l"`
		AComplex64  complex64  `hbase:"cf:m"`
		AComplex128 complex128 `hbase:"cf:n"`
		APointer    *int       `hbase:"cf:o"`
		AnArray     [6]uint8   `hbase:"cf:p"`
		ASlice      []uint8    `hbase:"cf:q"`
		AString     string     `hbase:"cf:r"`
	}

	number := 150
	for i := 0; i < b.N; i++ {
		str := teststr{
			AnInt:       10,
			AnInt8:      20,
			AnInt16:     30,
			AnInt32:     40,
			AnInt64:     50,
			AnUInt:      60,
			AnUInt8:     70,
			AnUInt16:    80,
			AnUInt32:    90,
			AnUInt64:    100,
			AFloat32:    110,
			AFloat64:    120,
			AComplex64:  130,
			AComplex128: 140,
			APointer:    &number,
			AnArray:     [6]uint8{4, 8, 15, 26, 23, 42},
			ASlice:      []uint8{1, 1, 3, 5, 8, 13, 21, 34, 55},
			AString:     "This is a test string.",
		}
		mutate, err := NewPutStrRef(context.Background(), "", "", str)
		if err != nil {
			b.Errorf("Error creating mutate: %v", err)
		}
		mutate.SetRegion(&regioninfo.Info{})
		mutate.Serialize()
	}
}
