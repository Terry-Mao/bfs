// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// +build integration

package gohbase_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"bfs/libs/gohbase"
	"bfs/libs/gohbase/hrpc"
	"bfs/libs/gohbase/test"
)

var host = flag.String("HBase Host", "localhost", "The location where HBase is running")

const table = "test1"

func TestMain(m *testing.M) {
	err := test.CreateTable(*host, table, []string{"cf", "cf2"})
	if err != nil {
		panic(err)
	}
	res := m.Run()
	err = test.DeleteTable(*host, table)
	if err != nil {
		panic(err)
	}

	os.Exit(res)
}

func TestGet(t *testing.T) {
	key := "row1"
	val := []byte("1")
	headers := map[string][]string{"cf": nil}
	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)
	err := insertKeyValue(c, key, "cf", val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Cells[0].Value
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v",
			val, rsp_value)
	}

	get.ExistsOnly()
	rsp, err = c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	} else if !*rsp.Exists {
		t.Error("Get claimed that our row didn't exist")
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)
	get, err = hrpc.NewGetStr(ctx, table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	_, err = c.Get(get)
	if err != gohbase.ErrDeadline {
		t.Errorf("Get ignored the deadline")
	}
}

func TestGetDoesntExist(t *testing.T) {
	key := "row1.5"
	c := gohbase.NewClient(*host)
	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	} else if results := len(rsp.Cells); results != 0 {
		t.Errorf("Get expected 0 cells. Received: %d", results)
	}

	get.ExistsOnly()
	rsp, err = c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	} else if *rsp.Exists {
		t.Error("Get claimed that our non-existent row exists")
	}
}

func TestGetBadColumnFamily(t *testing.T) {
	key := "row1.625"
	c := gohbase.NewClient(*host)
	err := insertKeyValue(c, key, "cf", []byte("Bad!"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	families := map[string][]string{"badcf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(families))
	rsp, err := c.Get(get)
	if err == nil {
		t.Errorf("Get didn't return an error! (It should have)")
	}
	if rsp != nil {
		t.Errorf("Get expected no result. Received: %v", rsp)
	}
}

func TestGetMultipleCells(t *testing.T) {
	key := "row1.75"
	c := gohbase.NewClient(*host, gohbase.FlushInterval(time.Millisecond*2))
	err := insertKeyValue(c, key, "cf", []byte("cf"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	err = insertKeyValue(c, key, "cf2", []byte("cf2"))
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	families := map[string][]string{"cf": nil, "cf2": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(families))
	rsp, err := c.Get(get)
	cells := rsp.Cells
	num_results := len(cells)
	if num_results != 2 {
		t.Errorf("Get expected 2 cells. Received: %d", num_results)
	}
	for _, cell := range cells {
		if !bytes.Equal(cell.Family, cell.Value) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				cell.Family, cell.Value)
		}
	}
}

func TestPut(t *testing.T) {
	key := "row2"
	values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	if host == nil {
		t.Fatal("Host is not set!")
	}
	c := gohbase.NewClient(*host)
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		t.Errorf("NewPutStr returned an error: %v", err)
	}
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)
	putRequest, err = hrpc.NewPutStr(ctx, table, key, values)
	_, err = c.Put(putRequest)
	if err != gohbase.ErrDeadline {
		t.Errorf("Put ignored the deadline")
	}
}

func TestPutReflection(t *testing.T) {
	key := "row2.25"
	number := 150
	data := struct {
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
	}{
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

	if host == nil {
		t.Fatal("Host is not set!")
	}

	c := gohbase.NewClient(*host)
	putRequest, err := hrpc.NewPutStrRef(context.Background(), table, key, data)
	if err != nil {
		t.Errorf("NewPutStrRef returned an error: %v", err)
	}

	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	if err != nil {
		t.Fatalf("Failed to create Get request: %s", err)
	}
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}

	expected := map[string][]byte{
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
	}

	for _, cell := range rsp.Cells {
		want, ok := expected[string(cell.Qualifier)]
		if !ok {
			t.Errorf("Unexpected qualifier: %q in %#v", cell.Qualifier, rsp)
		} else if !bytes.Equal(cell.Value, want) {
			t.Errorf("qualifier %q didn't match: wanted %q, but got %q",
				cell.Qualifier, want, cell.Value)
		}
	}

}

func TestPutMultipleCells(t *testing.T) {
	key := "row2.5"
	values := map[string]map[string][]byte{"cf": map[string][]byte{}, "cf2": map[string][]byte{}}
	values["cf"]["a"] = []byte("a")
	values["cf"]["b"] = []byte("b")
	values["cf2"]["a"] = []byte("a")
	c := gohbase.NewClient(*host)
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	_, err = c.Put(putRequest)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	families := map[string][]string{"cf": nil, "cf2": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(families))
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	cells := rsp.Cells
	if len(cells) != 3 {
		t.Errorf("Get expected 3 cells. Received: %d", len(cells))
	}
	for _, cell := range cells {
		if !bytes.Equal(cell.Qualifier, cell.Value) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
				cell.Qualifier, cell.Value)
		}
	}

}

func TestMultiplePutsGetsSequentially(t *testing.T) {
	const num_ops = 100
	keyPrefix := "row3"
	headers := map[string][]string{"cf": nil}
	c := gohbase.NewClient(*host, gohbase.FlushInterval(time.Millisecond))
	err := performNPuts(keyPrefix, num_ops)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}
	for i := num_ops - 1; i >= 0; i-- {
		key := keyPrefix + fmt.Sprintf("%d", i)
		get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
		rsp, err := c.Get(get)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
		}
		if len(rsp.Cells) != 1 {
			t.Errorf("Incorrect number of cells returned by Get: %d", len(rsp.Cells))
		}
		rsp_value := rsp.Cells[0].Value
		if !bytes.Equal(rsp_value, []byte(fmt.Sprintf("%d", i))) {
			t.Errorf("Get returned an incorrect result. Expected: %v, Got: %v",
				[]byte(fmt.Sprintf("%d", i)), rsp_value)
		}
	}
}

func TestMultiplePutsGetsParallel(t *testing.T) {
	const num_ops = 1000
	keyPrefix := "row3.5"
	headers := map[string][]string{"cf": nil}
	c := gohbase.NewClient(*host)
	// TODO: Currently have to CheckTable before initiating the N requests
	// 	 otherwise we face runaway client generation - one for each request.
	c.CheckTable(context.Background(), table)
	var wg sync.WaitGroup
	for i := 0; i < num_ops; i++ {
		wg.Add(1)
		go func(client gohbase.Client, key string) {
			defer wg.Done()
			err := insertKeyValue(client, key, "cf", []byte(key))
			if err != nil {
				t.Errorf("(Parallel) Put returned an error: %v", err)
			}
		}(c, keyPrefix+fmt.Sprintf("%d", i))
	}
	wg.Wait()
	// All puts are complete. Now do the same for gets.
	for i := num_ops - 1; i >= 0; i-- {
		wg.Add(1)
		go func(client gohbase.Client, key string) {
			defer wg.Done()
			get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
			rsp, err := c.Get(get)
			if err != nil {
				t.Errorf("(Parallel) Get returned an error: %v", err)
			} else {
				rsp_value := rsp.Cells[0].Value
				if !bytes.Equal(rsp_value, []byte(key)) {
					t.Errorf("Get returned an incorrect result.")
				}
			}
		}(c, keyPrefix+fmt.Sprintf("%d", i))
	}
	wg.Wait()
}

func TestTimestampIncreasing(t *testing.T) {
	key := "row4"
	c := gohbase.NewClient(*host)
	var oldTime uint64 = 0
	headers := map[string][]string{"cf": nil}
	for i := 0; i < 10; i++ {
		insertKeyValue(c, key, "cf", []byte("1"))
		get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
		rsp, err := c.Get(get)
		if err != nil {
			t.Errorf("Get returned an error: %v", err)
			break
		}
		newTime := *rsp.Cells[0].Timestamp
		if newTime <= oldTime {
			t.Errorf("Timestamps are not increasing. Old Time: %v, New Time: %v",
				oldTime, newTime)
		}
		oldTime = newTime
		time.Sleep(time.Millisecond)
	}
}

func TestAppend(t *testing.T) {
	key := "row7"
	c := gohbase.NewClient(*host)
	// Inserting "Hello"
	insertErr := insertKeyValue(c, key, "cf", []byte("Hello"))
	if insertErr != nil {
		t.Errorf("Put returned an error: %v", insertErr)
	}
	// Appending " my name is Dog."
	values := map[string]map[string][]byte{"cf": map[string][]byte{}}
	values["cf"]["a"] = []byte(" my name is Dog.")
	appRequest, err := hrpc.NewAppStr(context.Background(), table, key, values)
	appRsp, err := c.Append(appRequest)
	if err != nil {
		t.Errorf("Append returned an error: %v", err)
	}
	if appRsp == nil {
		t.Errorf("Append doesn't return updated value.")
	}
	// Verifying new result is "Hello my name is Dog."
	result := appRsp.Cells[0].Value
	if !bytes.Equal([]byte("Hello my name is Dog."), result) {
		t.Errorf("Append returned an incorrect result. Expected: %v, Receieved: %v",
			[]byte("Hello my name is Dog."), result)
	}

	// Make sure the change was actually committed.
	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	rsp, err := c.Get(get)
	cells := rsp.Cells
	if len(cells) != 1 {
		t.Errorf("Get expected 1 cells. Received: %d", len(cells))
	}
	result = cells[0].Value
	if !bytes.Equal([]byte("Hello my name is Dog."), result) {
		t.Errorf("Append returned an incorrect result. Expected: %v, Receieved: %v",
			[]byte("Hello my name is Dog."), result)
	}
}

func TestIncrement(t *testing.T) {
	c := gohbase.NewClient(*host)
	key := "row102"

	// test incerement
	incRequest, err := hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 1)
	result, err := c.Increment(incRequest)
	if err != nil {
		t.Fatalf("Increment returned an error: %v", err)
	}

	if result != 1 {
		t.Fatalf("Increment's result is %d, want 1", result)
	}

	incRequest, err = hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 5)
	result, err = c.Increment(incRequest)
	if err != nil {
		t.Fatalf("Increment returned an error: %v", err)
	}

	if result != 6 {
		t.Fatalf("Increment's result is %d, want 6", result)
	}
}

func TestIncrementParallel(t *testing.T) {
	c := gohbase.NewClient(*host)
	key := "row102.5"

	// TODO: Currently have to CheckTable before initiating N requests
	// 	otherwise we face runaway client generation - one for each request.
	c.CheckTable(context.Background(), table)

	numParallel := 10

	// test incerement
	var wg sync.WaitGroup
	for i := 0; i < numParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			incRequest, err := hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 1)
			_, err = c.Increment(incRequest)
			if err != nil {
				t.Errorf("Increment returned an error: %v", err)
			}
		}()
	}
	wg.Wait()

	// do one more to check if there's a correct value
	incRequest, err := hrpc.NewIncStrSingle(context.Background(), table, key, "cf", "a", 1)
	result, err := c.Increment(incRequest)
	if err != nil {
		t.Fatalf("Increment returned an error: %v", err)
	}

	if result != int64(numParallel+1) {
		t.Fatalf("Increment's result is %d, want %d", result, numParallel+1)
	}

}

// Note: This function currently causes an infinite loop in the client throwing the error -
// 2015/06/19 14:34:11 Encountered an error while reading: Failed to read from the RS: EOF
func TestChangingRegionServers(t *testing.T) {
	key := "row8"
	val := []byte("1")
	headers := map[string][]string{"cf": nil}
	if host == nil {
		t.Fatal("Host is not set!")
	}
	c := gohbase.NewClient(*host)
	err := insertKeyValue(c, key, "cf", val)
	if err != nil {
		t.Errorf("Put returned an error: %v", err)
	}

	// RegionServer 1 hosts all the current regions.
	// Now launch servers 2,3
	test.LaunchRegionServers([]string{"2", "3"})

	// Now (gracefully) stop servers 1,2.
	// All regions should now be on server 3.
	test.StopRegionServers([]string{"1", "2"})
	get, err := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("Get returned an error: %v", err)
	}
	rsp_value := rsp.Cells[0].Value
	if !bytes.Equal(rsp_value, val) {
		t.Errorf("Get returned an incorrect result. Expected: %v, Received: %v",
			val, rsp_value)
	}

	// Clean up by re-launching RS1 and closing RS3
	test.LaunchRegionServers([]string{"1"})
	test.StopRegionServers([]string{"3"})
}

func BenchmarkPut(b *testing.B) {
	b.ReportAllocs()
	keyPrefix := "row9"
	err := performNPuts(keyPrefix, b.N)
	if err != nil {
		b.Errorf("Put returned an error: %v", err)
	}
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	keyPrefix := "row10"
	err := performNPuts(keyPrefix, b.N)
	if err != nil {
		b.Errorf("Put returned an error: %v", err)
	}
	c := gohbase.NewClient(*host)
	b.ResetTimer()
	headers := map[string][]string{"cf": nil}
	for i := 0; i < b.N; i++ {
		key := keyPrefix + fmt.Sprintf("%d", i)
		get, _ := hrpc.NewGetStr(context.Background(), table, key, hrpc.Families(headers))
		c.Get(get)
	}
}

// Helper function. Given a key_prefix, num_ops, performs num_ops.
func performNPuts(keyPrefix string, num_ops int) error {
	c := gohbase.NewClient(*host)
	for i := 0; i < num_ops; i++ {
		key := keyPrefix + fmt.Sprintf("%d", i)
		err := insertKeyValue(c, key, "cf", []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper function. Given a client, key, columnFamily, value inserts into the table under column 'a'
func insertKeyValue(c gohbase.Client, key, columnFamily string, value []byte) error {
	values := map[string]map[string][]byte{columnFamily: map[string][]byte{}}
	values[columnFamily]["a"] = value
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	_, err = c.Put(putRequest)
	return err
}
