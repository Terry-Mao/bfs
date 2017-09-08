// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/hrpc"
	"bfs/libs/gohbase/pb"
	"bfs/libs/gohbase/region"
	"bfs/libs/gohbase/regioninfo"
	"bfs/libs/gohbase/zk"

	"github.com/cznic/b"
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
)

// Constants
var (
	// Name of the meta region.
	metaTableName = []byte("hbase:meta")

	infoFamily = map[string][]string{
		"info": nil,
	}

	//
	NoMServer = errors.New("no meta/master")

	// ErrDeadline is returned when the deadline of a request has been exceeded
	ErrDeadline = errors.New("deadline exceeded")

	// TableNotFound is returned when attempting to access a table that
	// doesn't exist on this cluster.
	TableNotFound = errors.New("table not found")

	// Default timeouts

	// How long to wait for a region lookup (either meta lookup or finding
	// meta in ZooKeeper).  Should be greater than or equal to the ZooKeeper
	// session timeout.
	regionLookupTimeout = 30 * time.Second

	backoffStart = 16 * time.Millisecond
)

const (
	standardClient = iota
	adminClient
)

const DefaultBatchCalls = 2

type Option func(*client)

type newRegResult struct {
	Client *region.Client
	Err    error
}

type CallResult struct {
	Result *hrpc.Result
	Err    error
}

// region -> client cache.
type regionClientCache struct {
	m sync.Mutex

	regionClientMap map[*regioninfo.Info]*region.Client

	// Used to quickly look up all the regioninfos that map to a specific client
	clientRegionsMap map[*region.Client][]*regioninfo.Info
}

func (rcc *regionClientCache) get(r *regioninfo.Info) *region.Client {
	rcc.m.Lock()
	c := rcc.regionClientMap[r]
	rcc.m.Unlock()
	return c
}

func (rcc *regionClientCache) put(r *regioninfo.Info, c *region.Client) {
	rcc.m.Lock()
	rcc.regionClientMap[r] = c
	lst := rcc.clientRegionsMap[c]
	var exist bool
	for _, ri := range lst {
		if ri == r {
			// same one
			exist = true
			break
		}
	}
	if !exist {
		rcc.clientRegionsMap[c] = append(lst, r)
	}
	rcc.m.Unlock()
}

func (rcc *regionClientCache) del(r *regioninfo.Info) {
	rcc.m.Lock()
	c := rcc.regionClientMap[r]

	if c != nil {
		// c can be nil if the regioninfo is not in the cache
		// e.g. it's already been deleted.
		delete(rcc.regionClientMap, r)

		var index int
		for i, reg := range rcc.clientRegionsMap[c] {
			if reg == r {
				index = i
			}
		}
		rcc.clientRegionsMap[c] = append(
			rcc.clientRegionsMap[c][:index],
			rcc.clientRegionsMap[c][index+1:]...)
	}
	rcc.m.Unlock()
}

func (rcc *regionClientCache) regionClientDown(reg *regioninfo.Info) []*regioninfo.Info {
	var c *region.Client
	rcc.m.Lock()
	c = rcc.regionClientMap[reg]
	// left for rcc.clientDown to release the lock
	return rcc.clientDown(c, true)
}

func (rcc *regionClientCache) clientDown(c *region.Client, havingLock bool) []*regioninfo.Info {
	if !havingLock {
		rcc.m.Lock()
	}
	var downRegions []*regioninfo.Info
	for _, sharedReg := range rcc.clientRegionsMap[c] {
		succ := sharedReg.MarkUnavailable()
		delete(rcc.regionClientMap, sharedReg)
		if succ {
			downRegions = append(downRegions, sharedReg)
		}
	}
	delete(rcc.clientRegionsMap, c)
	rcc.m.Unlock()
	return downRegions
}

// for test
func (rcc *regionClientCache) allClientDown() {
	rcc.m.Lock()
	for _, c := range rcc.regionClientMap {
		var downregions []*regioninfo.Info
		for _, sharedReg := range rcc.clientRegionsMap[c] {
			succ := sharedReg.MarkUnavailable()
			delete(rcc.regionClientMap, sharedReg)
			if succ {
				downregions = append(downregions, sharedReg)
			}
		}
		delete(rcc.clientRegionsMap, c)
	}
	rcc.m.Unlock()
}

func (rcc *regionClientCache) checkForClient(host string, port uint16) *region.Client {
	rcc.m.Lock()
	for client := range rcc.clientRegionsMap {
		if client.Host() == host && client.Port() == port {
			rcc.m.Unlock()
			return client
		}
	}
	rcc.m.Unlock()
	return nil
}

// key -> region cache.
type keyRegionCache struct {
	m sync.Mutex

	// Maps a []byte of a region start key to a *regioninfo.Info
	regions *b.Tree
}

func (krc *keyRegionCache) get(key []byte) ([]byte, *regioninfo.Info) {
	// When seeking - "The Enumerator's position is possibly after the last item in the tree"
	// http://godoc.org/github.com/cznic/b#Tree.Set
	krc.m.Lock()
	enum, ok := krc.regions.Seek(key)
	k, v, err := enum.Prev()
	if err == io.EOF && krc.regions.Len() > 0 {
		// We're past the end of the tree. Return the last element instead.
		// (Without this code we always get a cache miss and create a new client for each req.)
		k, v = krc.regions.Last()
		err = nil
	} else if !ok {
		k, v, err = enum.Prev()
	}
	// TODO: It would be nice if we could do just enum.Get() to avoid the
	// unnecessary cost of seeking to the next entry.
	krc.m.Unlock()
	if err != nil {
		return nil, nil
	}
	return k.([]byte), v.(*regioninfo.Info)
}

func (krc *keyRegionCache) put(key []byte, reg *regioninfo.Info) *regioninfo.Info {
	krc.m.Lock()
	// As split case if not that frequent(at least compare to put), we prefer the lazy way -
	// remove and update out-of-region when meet error
	// Author: We need to remove all the entries that are overlap with the range
	// of the new region being added here, if any.
	oldV, _ := krc.regions.Put(key, func(interface{}, bool) (interface{}, bool) {
		return reg, true
	})
	krc.m.Unlock()
	if oldV == nil {
		return nil
	}
	return oldV.(*regioninfo.Info)
}

func (krc *keyRegionCache) del(key []byte) bool {
	krc.m.Lock()
	success := krc.regions.Delete(key)
	krc.m.Unlock()
	return success
}

// A Client provides access to an HBase cluster.
type client struct {
	clientType int

	zkquorum []string

	regions keyRegionCache

	// used when:
	// 1. mark region as unavailable
	// 2. get region from cache
	regionsLock sync.Mutex

	// Maps a *regioninfo.Info to the *region.Client that we think currently
	// serves it.
	clients regionClientCache

	metaRegionInfo *regioninfo.Info
	metaClient     *region.Client

	adminRegionInfo *regioninfo.Info
	adminClient     *region.Client

	// The maximum size of the RPC queue in the region client
	rpcQueueSize int

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration

	// The timeout used when dial to region server. 0 means no-timeout.
	dialTimeout time.Duration

	zkClient *zk.ZKClient
}

func (c *client) Close() error {
	c.regionsLock.Lock()
	defer c.regionsLock.Unlock()
	if c.metaClient != nil {
		c.metaClient.Close()
	}
	if c.adminClient != nil {
		c.adminClient.Close()
	}
	for _, rc := range c.clients.regionClientMap {
		if rc != nil {
			rc.Close()
		}
	}
	return nil
}

func (c *client) SetServer(resourceType int, ms *zk.ServerInfo) {
	log.Info("SetServer for type (%d) to (%v)", resourceType, ms)
	switch resourceType {
	case zk.ResourceTypeMaster:
		mc := c.adminClient
		c.adminClient = nil
		go mc.Close()
	case zk.ResourceTypeMeta:
		mc := c.metaClient
		c.metaClient = nil
		go mc.Close()
	default:
		log.Infof("unrecognized resourceType: %d", resourceType)
	}
}

// Client a regular HBase client
type Client interface {
	CheckTable(ctx context.Context, table string) error
	Scan(s *hrpc.Scan) ([]*hrpc.Result, error)
	Get(g *hrpc.Get) (*hrpc.Result, error)
	Put(p *hrpc.Mutate) (*hrpc.Result, error)
	Delete(d *hrpc.Mutate) (*hrpc.Result, error)
	Append(a *hrpc.Mutate) (*hrpc.Result, error)
	Increment(i *hrpc.Mutate) (int64, error)
	// Calls can only used for general call which means call.CallType().GeneralCall() == true
	// notice that the total cost-time is proportional to len(gets) as sequential-execution
	Calls(cs []hrpc.Call) []CallResult
	// ConCalls will do the cs.Calls concurrently with given concurrency.
	// 0 concurrency will result an auto-concurrency: len(cs.Calls) / DefaultBatchCalls, at least 1
	// -1 concurrency is not supported
	Go(cs *hrpc.Calls) []CallResult
	Close() error
}

// AdminClient to perform admistrative operations with HMaster
type AdminClient interface {
	CreateTable(t *hrpc.CreateTable) (*hrpc.Result, error)
	DeleteTable(t *hrpc.DeleteTable) (*hrpc.Result, error)
	EnableTable(t *hrpc.EnableTable) (*hrpc.Result, error)
	DisableTable(t *hrpc.DisableTable) (*hrpc.Result, error)
}

// NewClient creates a new HBase client.
// master or meta being empty string means use default zk-path
func NewClient(c *conf.Conf, options ...Option) Client {
	return newClient(standardClient, c, options...)
}

// NewAdminClient creates an admin HBase client.
// master or meta being empty string means use default zk-path
func NewAdminClient(c *conf.Conf, options ...Option) AdminClient {
	return newClient(adminClient, c, options...)
}

func newClient(clientType int, c *conf.Conf, options ...Option) *client {
	log.Infof("Creating new client. Host: %v", c.Zkquorum)
	if c.FlushInterval <= 0 && c.RpcQueueSize < 0 {
		log.Errorf("flushInterval (%d) <= 0 and queueSize (%d) < 0", int64(c.FlushInterval), c.RpcQueueSize)
		return nil
	}
	cl := &client{
		clientType: clientType,
		regions:    keyRegionCache{regions: b.TreeNew(regioninfo.CompareGeneric)},
		clients: regionClientCache{
			regionClientMap:  make(map[*regioninfo.Info]*region.Client),
			clientRegionsMap: make(map[*region.Client][]*regioninfo.Info),
		},
		zkquorum: c.Zkquorum,
		//rpcQueueSize:  100,
		rpcQueueSize: c.RpcQueueSize,
		//flushInterval: 5 * time.Millisecond,  //XXX allow for configuring
		flushInterval: c.FlushInterval, //XXX allow for configuring
		dialTimeout:   c.DialTimeout,
		metaRegionInfo: &regioninfo.Info{
			Table:      []byte("hbase:meta"),
			RegionName: []byte("hbase:meta,,1"),
			StopKey:    []byte{},
		},
		adminRegionInfo: &regioninfo.Info{},
	}
	for _, option := range options {
		option(cl)
	}
	var (
		useMaster, useMeta bool
		resourceType       int
		serverWatcher      zk.ServerWatcher
	)
	if clientType == adminClient {
		useMaster, useMeta = true, false
		resourceType = zk.ResourceTypeMaster
	} else {
		useMaster, useMeta = false, true
		resourceType = zk.ResourceTypeMeta
	}
	zkClient, err := zk.NewZKClient(c.Zkquorum, c.ZkRoot, c.Master, c.Meta, useMaster, useMeta, c.ZkTimeout)
	if err != nil {
		log.Errorf("zk.NewZKClient(%v, %s, %s) failed, err is (%v)", c.Zkquorum, c.Master, c.Meta, err)
		return nil
	}
	zkClient.WatchServer(resourceType, serverWatcher)
	cl.zkClient = zkClient
	return cl
}

// RpcQueueSize will return an option that will set the size of the RPC queues
// used in a given client
func RpcQueueSize(size int) Option {
	return func(c *client) {
		c.rpcQueueSize = size
	}
}

// FlushInterval will return an option that will set the timeout for flushing
// the RPC queues used in a given client
func FlushInterval(interval time.Duration) Option {
	return func(c *client) {
		c.flushInterval = interval
	}
}

// CheckTable returns an error if the given table name doesn't exist.
func (c *client) CheckTable(ctx context.Context, table string) error {
	getStr, err := hrpc.NewGetStr(ctx, table, "theKey")
	if err == nil {
		_, err = c.SendRPC(getStr)
	}
	return err
}

// call can only used for general call which means call.CallType().GeneralCall() == true
func (c *client) call(ca hrpc.Call) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(ca)
	if err != nil {
		return nil, err
	}
	ct := ca.CallType()
	var result *pb.Result
	switch {
	case ct == hrpc.CallTypeGet:
		r, ok := pbmsg.(*pb.GetResponse)
		if !ok {
			return nil, fmt.Errorf("sendRPC returned not a GetResponse")
		}
		result = r.Result
	case ct.IsMutate():
		r, ok := pbmsg.(*pb.MutateResponse)
		if !ok {
			return nil, fmt.Errorf("sendRPC returned not a MutateResponse")
		}
		result = r.Result
	}
	return hrpc.ToLocalResult(result), nil
}

// for test case
func (c *client) clearAllRegions() {
	oldMetaClient := c.metaClient
	c.metaClient = nil // 不能 markUnAvailable
	go oldMetaClient.Close()
	c.regionsLock.Lock()
	regions := c.regions
	regions.m.Lock()
	regions.regions.Clear()
	regions.m.Unlock()
	c.regionsLock.Unlock()
}

// Scan retrieves the values specified in families from the given range.
func (c *client) Scan(s *hrpc.Scan) ([]*hrpc.Result, error) {
	var (
		results []*pb.Result
		scanres *pb.ScanResponse
		rpc     *hrpc.Scan
		err     error
		res     proto.Message
	)
	ctx := s.GetContext()
	table := s.Table()
	families := s.GetFamilies()
	filters := s.GetFilter()
	startRow := s.GetStartRow()
	stopRow := s.GetStopRow()
	limit := s.Limit()
	for {
		// Make a new Scan RPC for this region
		if rpc != nil {
			// If it's not the first region, we want to start at whatever the
			// last region's StopKey was
			startRow = rpc.GetRegionStop()
		}

		rpc, err = hrpc.NewScanRange(ctx, table, startRow, stopRow,
			hrpc.Families(families), hrpc.Filters(filters))
		if err != nil {
			return nil, err
		}

		res, err = c.sendRPC(rpc)
		if err != nil {
			return nil, err
		}
		scanres = res.(*pb.ScanResponse)
		results = append(results, scanres.Results...)

		// TODO: The more_results field of the ScanResponse object was always
		// true, so we should figure out if there's a better way to know when
		// to move on to the next region than making an extra request and
		// seeing if there were no results
		enough := false
		for len(scanres.Results) != 0 {
			rpc = hrpc.NewScanFromID(ctx, table, *scanres.ScannerId, rpc.Key())

			res, err = c.sendRPC(rpc)
			if err != nil {
				return nil, err
			}
			scanres = res.(*pb.ScanResponse)
			results = append(results, scanres.Results...)
			if limit > 0 && len(results) >= limit {
				enough = true
				break
			}
		}
		//if scanres != nil && len(scanres.Results) != 0 {
		//	// means scan is not finished (but has satisfied the requirement)
		//	rpc = hrpc.NewCloseFromID(ctx, table, *scanres.ScannerId, rpc.Key())
		//	res, err = c.sendRPC(rpc)
		//	// new version hbase will close scanner after iterating and thus close rpc may return "UnknownScannerException" error
		//	// thus here we do not check err
		//	// if err != nil {
		//	// 	return nil, err
		//	// }
		//}
		// but for some hbase versions, it seems that scanner should be closed manually. WTF
		rpc = hrpc.NewCloseFromID(ctx, table, *scanres.ScannerId, rpc.Key())
		res, _ = c.sendRPC(rpc)
		if enough {
			break
		}

		// Check to see if this region is the last we should scan (either
		// because (1) it's the last region or (3) because its stop_key is
		// greater than or equal to the stop_key of this scanner provided
		// that (2) we're not trying to scan until the end of the table).
		// (1)
		if len(rpc.GetRegionStop()) == 0 ||
			// (2)                (3)
			len(stopRow) != 0 && bytes.Compare(stopRow, rpc.GetRegionStop()) <= 0 {
			break
		}
	}
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	// Do we want to be returning a slice of Result objects or should we just
	// put all the Cells into the same Result object?
	localResults := make([]*hrpc.Result, len(results))
	for idx, result := range results {
		localResults[idx] = hrpc.ToLocalResult(result)
	}
	return localResults, nil
}

func (c *client) Get(g *hrpc.Get) (*hrpc.Result, error) {
	return c.call(g)
}

func (c *client) Calls(cs []hrpc.Call) (res []CallResult) {
	callsNum := len(cs)
	if callsNum == 0 {
		return
	}
	res = make([]CallResult, callsNum)
	for i, ca := range cs {
		var (
			callResult *hrpc.Result
			err        error
		)
		if ca.CallType().GeneralCall() {
			callResult, err = c.call(ca)
		} else {
			err = hrpc.NotGeneralCallErr
		}
		res[i] = CallResult{
			Result: callResult,
			Err:    err,
		}
	}
	return
}

func (c *client) Go(cs *hrpc.Calls) (res []CallResult) {
	type chanElem struct {
		res    CallResult
		offset int
	}
	callsNum := len(cs.Calls)
	var perNum int
	if callsNum == 0 {
		return
	}
	concurrency := 1 + (callsNum-1)/DefaultBatchCalls
	perNum = DefaultBatchCalls
	resChan := make(chan chanElem, callsNum)
	res = make([]CallResult, callsNum)
	fetcher := func(start int, calls []hrpc.Call, ch chan<- chanElem) {
		for i, curCall := range calls {
			var (
				callResult *hrpc.Result
				err        error
			)
			if curCall.CallType().GeneralCall() {
				callResult, err = c.call(curCall)
			} else {
				err = hrpc.NotGeneralCallErr
			}
			ch <- chanElem{
				offset: start + i,
				res: CallResult{
					Result: callResult,
					Err:    err,
				},
			}
		}
	}
	start := 0
	for i := 0; i < concurrency-1; i, start = i+1, start+perNum {
		go fetcher(start, cs.Calls[start:start+perNum], resChan)
	}
	go fetcher(start, cs.Calls[start:], resChan)
	for i := 0; i < callsNum; i++ {
		select {
		case curRes := <-resChan:
			res[curRes.offset] = curRes.res
		case <-cs.Ctx.Done():
			log.Infof("timeout when do Go for (%d) calls", callsNum)
			return
		}
	}
	return
}

func (c *client) Put(p *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(p)
}

func (c *client) Delete(d *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(d)
}

func (c *client) Append(a *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(a)
}

func (c *client) Increment(i *hrpc.Mutate) (int64, error) {
	r, err := c.mutate(i)
	if err != nil {
		return 0, err
	}

	if len(r.Cells) != 1 {
		return 0, fmt.Errorf("Increment returned %d cells, but we expected exactly one.",
			len(r.Cells))
	}

	val := binary.BigEndian.Uint64(r.Cells[0].Value)
	return int64(val), nil
}

func (c *client) mutate(m *hrpc.Mutate) (*hrpc.Result, error) {
	return c.call(m)
}

func (c *client) CreateTable(t *hrpc.CreateTable) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return nil, err
	}

	_, ok := pbmsg.(*pb.CreateTableResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a CreateTableResponse")
	}

	return &hrpc.Result{}, nil
}

func (c *client) DeleteTable(t *hrpc.DeleteTable) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return nil, err
	}

	_, ok := pbmsg.(*pb.DeleteTableResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a DeleteTableResponse")
	}

	return &hrpc.Result{}, nil
}

func (c *client) EnableTable(t *hrpc.EnableTable) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return nil, err
	}

	_, ok := pbmsg.(*pb.EnableTableResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a EnableTableResponse")
	}

	return &hrpc.Result{}, nil
}

func (c *client) DisableTable(t *hrpc.DisableTable) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(t)
	if err != nil {
		return nil, err
	}

	_, ok := pbmsg.(*pb.DisableTableResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a DisableTableResponse")
	}

	return &hrpc.Result{}, nil
}

// Could be removed in favour of above
func (c *client) SendRPC(rpc hrpc.Call) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(rpc)

	var rsp *hrpc.Result
	switch r := pbmsg.(type) {
	case *pb.GetResponse:
		rsp = hrpc.ToLocalResult(r.Result)
	case *pb.MutateResponse:
		rsp = hrpc.ToLocalResult(r.Result)
	}

	return rsp, err
}

func (c *client) sendRPC(rpc hrpc.Call) (proto.Message, error) {
	// Check the cache for a region that can handle this request
	reg := c.getRegionFromCache(rpc.Table(), rpc.Key())
	if reg != nil {
		//log.Info("found reg: %v for %s-%s", reg, string(rpc.Table()), string(rpc.Key()))
		return c.sendRPCToRegion(rpc, reg)
	} else {
		return c.findRegionAndSendRPC(rpc)
	}
}

func (c *client) sendRPCToRegion(rpc hrpc.Call, reg *regioninfo.Info) (proto.Message, error) {
	// On the first sendRPC to the meta or admin regions, a goroutine must be
	// manually kicked off for the meta or admin region client
	if c.adminClient == nil && reg == c.adminRegionInfo && !c.adminRegionInfo.IsUnavailable() ||
		c.metaClient == nil && reg == c.metaRegionInfo && !c.metaRegionInfo.IsUnavailable() {
		c.regionsLock.Lock()
		if reg == c.metaRegionInfo && !c.metaRegionInfo.IsUnavailable() ||
			reg == c.adminRegionInfo && !c.adminRegionInfo.IsUnavailable() {
			log.Info("reestablish region (%v)", reg)
			reg.MarkUnavailable()
			go c.reestablishRegion(reg)
		}
		c.regionsLock.Unlock()
	}
	// The region was in the cache, check
	// if the region is marked as available
	if reg.IsUnavailable() {
		return c.waitOnRegion(rpc, reg)
	}

	rpc.SetRegion(reg)

	// Queue the RPC to be sent to the region
	client := c.clientFor(reg)
	var err error
	if client == nil {
		err = errors.New("no client for this region")
	} else {
		err = client.QueueRPC(rpc)
	}

	if err != nil {
		// if the err is UnrecoverableErr
		if _, ok := err.(region.UnrecoverableError); ok {
			// If it was an unrecoverable error, the region client is
			// considered dead.
			log.Errorf("met UnrecoverableError (%v) when access region (%v)", err, reg)
			if reg == c.metaRegionInfo || reg == c.adminRegionInfo {
				// If this is the admin client or the meta table, mark the
				// region as unavailable and start up a goroutine to
				// reconnect if it wasn't already marked as such.
				first := reg.MarkUnavailable()
				if first {
					go c.reestablishRegion(reg)
				}
			} else {
				// Else this is a normal region. Mark all the regions
				// sharing this region's client as unavailable, and start
				// a goroutine to reconnect for each of them.
				downRegions := c.clients.regionClientDown(reg)
				for _, downReg := range downRegions {
					go c.reestablishRegion(downReg)
				}
			}
		} else {
			log.Errorf("met error (%v) when clientFor reg or queueRPC, try to reestablish region (%v)", err, reg)
			// There was an error queueing the RPC.
			// Mark the region as unavailable.
			first := reg.MarkUnavailable()
			// If this was the first goroutine to mark the region as
			// unavailable, start a goroutine to reestablish a connection
			if first {
				go c.reestablishRegion(reg)
			}
		}

		// Block until the region becomes available.
		return c.waitOnRegion(rpc, reg)
	}

	// Wait for the response
	var res hrpc.RPCResult
	select {
	case res = <-rpc.GetResultChan():
	case <-rpc.GetContext().Done():
		return nil, ErrDeadline
	}

	// Check for errors
	if _, ok := res.Error.(region.RetryableError); ok {
		// There's an error specific to this region, but
		// our region client is fine. Mark this region as
		// unavailable (as opposed to all regions sharing
		// the client), and start a goroutine to reestablish
		// it.
		log.Errorf("met RetryableError (%v) when access region (%v)", res.Error, reg)
		first := reg.MarkUnavailable()
		if first {
			go c.reestablishRegion(reg)
		}
		if reg != c.metaRegionInfo && reg != c.adminRegionInfo {
			// The client won't be in the cache if this is the
			// meta or admin region
			// but the reg in regions is left for establishRegion method to del
			c.clients.del(reg)
		}
		return c.waitOnRegion(rpc, reg)
	} else if _, ok := res.Error.(region.UnrecoverableError); ok {
		// If it was an unrecoverable error, the region client is
		// considered dead.
		log.Errorf("met UnrecoverableError (%v) when access region (%v)", res.Error, reg)
		if reg == c.metaRegionInfo || reg == c.adminRegionInfo {
			// If this is the admin client or the meta table, mark the
			// region as unavailable and start up a goroutine to
			// reconnect if it wasn't already marked as such.
			first := reg.MarkUnavailable()
			if first {
				go c.reestablishRegion(reg)
			}
		} else {
			// Else this is a normal region. Mark all the regions
			// sharing this region's client as unavailable, and start
			// a goroutine to reconnect for each of them.
			downRegions := c.clients.regionClientDown(reg)
			for _, downReg := range downRegions {
				go c.reestablishRegion(downReg)
			}
		}

		// Fall through to the case of the region being unavailable,
		// which will result in blocking until it's available again.
		return c.waitOnRegion(rpc, reg)
	} else {
		// RPC was successfully sent, or an unknown type of error
		// occurred. In either case, return the results.
		return res.Msg, res.Error
	}
}

// actually not wait on region but wait on region client after sendRPC
func (c *client) waitOnRegion(rpc hrpc.Call, reg *regioninfo.Info) (proto.Message, error) {
	ch := reg.GetAvailabilityChan()
	if ch == nil {
		// WTF, this region is available? Maybe it was marked as such
		// since waitOnRegion was called.
		return c.sendRPC(rpc)
	}
	// The region is unavailable. Wait for it to become available,
	// or for the deadline to be exceeded.
	select {
	case <-ch:
		return c.sendRPC(rpc) // rather than sendRPCToRegion as perhaps we should use another region
	case <-rpc.GetContext().Done():
		return nil, ErrDeadline
	}
}

// The region was not in the cache, it must be looked up in the meta table
func (c *client) findRegionAndSendRPC(rpc hrpc.Call) (proto.Message, error) {

	backoff := backoffStart
	ctx := rpc.GetContext()
	for {
		// Look up the region in the meta table
		reg, host, port, err := c.locateRegion(ctx, rpc.Table(), rpc.Key())

		if err != nil {
			if err == TableNotFound {
				return nil, err
			}
			// There was an error with the meta table. Let's sleep for some
			// backoff amount and retry.
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return nil, err
			}
			continue
		}

		// Check that the region wasn't added to
		// the cache while we were looking it up.
		// If not add it to cache.
		c.regionsLock.Lock()

		if existing := c.getRegionFromCache(rpc.Table(), rpc.Key()); existing != nil {
			// The region was added to the cache while we were looking it
			// up. Send the RPC to the region that was in the cache.
			c.regionsLock.Unlock()
			return c.sendRPCToRegion(rpc, existing)
		}

		log.Infof("region cache miss for (%q) (%q)", rpc.Table(), rpc.Key())

		// The region wasn't added to the cache while we were looking it
		// up. Mark this one as unavailable and add it to the cache.
		reg.MarkUnavailable()
		c.regions.put(reg.RegionName, reg) // NOTE not all NEW regions are added here as we may find a region from cache
		// and then it turns out to be invalid and replaced by another new one which it is fetched from establish-locate

		c.regionsLock.Unlock()

		// Start a goroutine to connect to the region
		go c.establishRegion(reg, host, port)

		// Wait for the new region to become
		// available, and then send the RPC
		return c.waitOnRegion(rpc, reg)
	}
}

// Searches in the regions cache for the region hosting the given row.
func (c *client) getRegionFromCache(table, key []byte) *regioninfo.Info {
	if c.clientType == adminClient {
		return c.adminRegionInfo
	} else if bytes.Equal(table, metaTableName) {
		return c.metaRegionInfo
	}
	regionName := createRegionSearchKey(table, key)
	regionKey, region := c.regions.get(regionName)
	if region == nil || !isCacheKeyForTable(table, regionKey) {
		return nil
	}

	if len(region.StopKey) != 0 &&
		// If the stop key is an empty byte array, it means this region is the
		// last region for this table and this key ought to be in that region.
		bytes.Compare(key, region.StopKey) >= 0 {
		return nil
	}

	return region
}

// Checks whether or not the given cache key is for the given table.
func isCacheKeyForTable(table, cacheKey []byte) bool {
	// Check we found an entry that's really for the requested table.
	for i := 0; i < len(table); i++ {
		if table[i] != cacheKey[i] {
			// This table isn't in the map, we found
			return false // a key which is for another table.
		}
	}

	// Make sure we didn't find another key that's for another table
	// whose name is a prefix of the table name we were given.
	return cacheKey[len(table)] == ','
}

// Creates the META key to search for in order to locate the given key.
func createRegionSearchKey(table, key []byte) []byte {
	metaKey := make([]byte, 0, len(table)+len(key)+3)
	metaKey = append(metaKey, table...)
	metaKey = append(metaKey, ',')
	metaKey = append(metaKey, key...)
	metaKey = append(metaKey, ',')
	// ':' is the first byte greater than '9'.  We always want to find the
	// entry with the greatest timestamp, so by looking right before ':'
	// we'll find it.
	metaKey = append(metaKey, ':')
	return metaKey
}

// Returns the client currently known to hose the given region, or NULL.
func (c *client) clientFor(region *regioninfo.Info) *region.Client {
	if c.clientType == adminClient {
		return c.adminClient
	}
	if region == c.metaRegionInfo {
		return c.metaClient
	}
	return c.clients.get(region)
}

// Locates the region in which the given row key for the given table is.
// all NEW REGIONs are "made" here
func (c *client) locateRegion(ctx context.Context,
	table, key []byte) (*regioninfo.Info, string, uint16, error) {

	log.Infof("locate region for table (%q), key (%q)", table, key) // for test

	metaKey := createRegionSearchKey(table, key)
	rpc, err := hrpc.NewGetBefore(ctx, metaTableName, metaKey, hrpc.Families(infoFamily))
	if err != nil {
		return nil, "", 0, err
	}
	rpc.SetRegion(c.metaRegionInfo)
	resp, err := c.sendRPC(rpc)

	if err != nil {
		ch := c.metaRegionInfo.GetAvailabilityChan()
		if ch != nil {
			select {
			case <-ch:
				return c.locateRegion(ctx, table, key)
			case <-rpc.GetContext().Done():
				return nil, "", 0, ErrDeadline
			}
		} else {
			return nil, "", 0, err
		}
	}

	metaRow := resp.(*pb.GetResponse)
	if metaRow.Result == nil {
		return nil, "", 0, TableNotFound
	}

	reg, host, port, err := c.parseMetaTableResponse(metaRow)
	if err != nil {
		log.Errorf("get meta for metaKey (%s) met error(%v)", metaKey, err)
		return nil, "", 0, err
	}
	if !bytes.Equal(table, reg.Table) {
		// This would indicate a bug in HBase.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong table!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	} else if len(reg.StopKey) != 0 &&
		bytes.Compare(key, reg.StopKey) >= 0 {
		// This would indicate a hole in the meta table.
		return nil, "", 0, fmt.Errorf("WTF: Meta returned an entry for the wrong region!"+
			"  Looked up table=%q key=%q got region=%s", table, key, reg)
	}
	log.Infof("locate region for table (%q), key (%q), found: (%v), (%s), (%d)", table, key, reg, host, port) // for test
	return reg, host, port, nil
}

// parseMetaTableResponse parses the contents of a row from the meta table.
// It's guaranteed to return a region info and a host/port OR return an error.
func (c *client) parseMetaTableResponse(metaRow *pb.GetResponse) (
	*regioninfo.Info, string, uint16, error) {

	var reg *regioninfo.Info
	var host string
	var port uint16

	for _, cell := range metaRow.Result.Cell {
		switch string(cell.Qualifier) {
		case "regioninfo":
			var err error
			reg, err = regioninfo.InfoFromCell(cell)
			if err != nil {
				return nil, "", 0, err
			}
		case "server":
			value := cell.Value
			if len(value) == 0 {
				continue // Empty during NSRE.
			}
			colon := bytes.IndexByte(value, ':')
			if colon < 1 {
				// Colon can't be at the beginning.
				return nil, "", 0,
					fmt.Errorf("broken meta: no colon found in info:server %q", cell)
			}
			host = string(value[:colon])
			portU64, err := strconv.ParseUint(string(value[colon+1:]), 10, 16)
			if err != nil {
				return nil, "", 0, err
			}
			port = uint16(portU64)
		default:
			// Other kinds of qualifiers: ignore them.
			// TODO: If this is the parent of a split region, there are two other
			// KVs that could be useful: `info:splitA' and `info:splitB'.
			// Need to investigate whether we can use those as a hint to update our
			// regions_cache with the daughter regions of the split.
		}
	}

	if reg == nil {
		// There was no regioninfo in the row in meta, this is really not
		// expected.
		err := fmt.Errorf("Meta seems to be broken, there was no regioninfo in %s",
			metaRow)
		log.Error(err.Error())
		return nil, "", 0, err
	} else if port == 0 {
		// Either both `host' and `port' are set, or both aren't.
		return nil, "", 0, fmt.Errorf("Meta doesn't have a server location in %s",
			metaRow)
	}

	return reg, host, port, nil
}

func (c *client) reestablishRegion(reg *regioninfo.Info) {
	c.establishRegion(reg, "", 0)
}

// ensure the region is valid (bind to a regionclient)
// when used to establish, host/port will be valid; and when used to reestablish, host/port will be zero/empty
func (c *client) establishRegion(originalReg *regioninfo.Info, host string, port uint16) {
	log.Infof("establishRegion(%v, %s, %d)", originalReg, host, port)
	originalReg.Park4Establish()
	var err error
	reg := originalReg
	backoff := backoffStart

	for {
		ctx, _ := context.WithTimeout(context.Background(), regionLookupTimeout)
		if port != 0 && err == nil {
			reg.DupExtInfo(originalReg)
			// If this isn't the admin or meta region, check if a client
			// for this host/port already exists
			if c.clientType != adminClient && reg != c.metaRegionInfo {
				client := c.clients.checkForClient(host, port)
				if client != nil && client.GetSendErr() == nil {
					// There's already a client, add it to the
					// cache and mark the new region as available.
					c.clients.put(reg, client)
					c.regions.put(reg.RegionName, reg)
					if !reg.Equals(originalReg) {
						// new region
						log.Info("originalReg: (%v), reg: (%v), update to c.regions", originalReg, reg)
						c.regions.del(originalReg.RegionName)
						c.clients.del(originalReg)
					}
					originalReg.MarkAvailable()
					return
				}
			}
			// Make this channel buffered so that if we time out we don't
			// block the newRegion goroutine forever.
			ch := make(chan newRegResult, 1)
			var clientType region.ClientType
			if c.clientType == standardClient {
				clientType = region.RegionClient
			} else {
				clientType = region.MasterClient
			}
			log.Info("newRegionClient(..., clientType(%v), host(%s), port(%d), rpcQueueSize(%d), flushInterval(%v), dialTimeout(%v))", clientType, host, port, c.rpcQueueSize, c.flushInterval, c.dialTimeout)
			go newRegionClient(ctx, ch, clientType, host, port, c.rpcQueueSize, c.flushInterval, c.dialTimeout)

			select {
			case res := <-ch:
				if res.Err == nil {
					if c.clientType == adminClient {
						c.adminClient = res.Client
					} else if reg == c.metaRegionInfo {
						c.metaClient = res.Client
					} else {
						c.clients.put(reg, res.Client)
						c.regions.put(reg.RegionName, reg)
						if !reg.Equals(originalReg) {
							// Here `reg' is guaranteed to be available, so we
							// must publish the region->client mapping first,
							// because as soon as we add it to the key->region
							// mapping here, concurrent readers are gonna want
							// to find the client.
							log.Info("originalReg: (%v), reg: (%v), update to c.regions", originalReg, reg)
							c.regions.del(originalReg.RegionName)
							c.clients.del(originalReg)
						}
					}
					originalReg.MarkAvailable()
					return
				} else {
					err = res.Err
				}
			case <-ctx.Done():
				log.Infof("region lookup timeout for clientType(%d), host(%d), port(%d)", clientType, host, port)
				err = ErrDeadline
			}
		}
		if err != nil {
			log.Errorf("met error (%v)", err)
			if err == TableNotFound {
				c.regions.del(originalReg.RegionName)
				c.clients.del(originalReg)
				originalReg.MarkAvailable()
				return
			}
			// This will be hit if either there was an error locating the
			// region, or the region was located but there was an error
			// connecting to it.
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				continue
			}
		}
		if c.clientType == adminClient {
			host, port, err = c.zkLookup(ctx, zk.ResourceTypeMaster)
		} else if reg == c.metaRegionInfo {
			host, port, err = c.zkLookup(ctx, zk.ResourceTypeMeta)
		} else {
			reg, host, port, err = c.locateRegion(ctx, originalReg.Table, originalReg.StartKey)
		}
	}
}

func sleepAndIncreaseBackoff(ctx context.Context, backoff time.Duration) (time.Duration, error) {
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return 0, ErrDeadline
	}
	// TODO: Revisit how we back off here.
	if backoff < 5000*time.Millisecond {
		return backoff * 2, nil
	} else {
		return backoff + 5000*time.Millisecond, nil
	}
}

func newRegionClient(ctx context.Context, ret chan newRegResult, clientType region.ClientType,
	host string, port uint16, queueSize int, queueTimeout, dialTimeout time.Duration) {
	c, e := region.NewClient(host, port, clientType, queueSize, queueTimeout, dialTimeout)
	select {
	case ret <- newRegResult{c, e}:
	// Hooray!
	case <-ctx.Done():
		// We timed out, too bad, nobody expects this client anymore, ditch it.
		c.Close()
	}
}

// zkResult contains the result of a ZooKeeper lookup (when we're looking for
// the meta region or the HMaster).
type zkResult struct {
	host string
	port uint16
	err  error
}

// Asynchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookup(ctx context.Context, resourceType int) (string, uint16, error) {
	// We make this a buffered channel so that if we stop waiting due to a
	// timeout, we won't block the zkLookupSync() that we start in a
	// separate goroutine.
	reschan := make(chan *zk.ServerInfo, 1)
	go c.zkLookupSync(resourceType, reschan)
	select {
	case res := <-reschan:
		if res == nil {
			return "", 0, NoMServer
		} else {
			return res.Host, res.Port, nil
		}
	case <-ctx.Done():
		return "", 0, ErrDeadline
	}
}

// Synchronously looks up the meta region or HMaster in ZooKeeper.
func (c *client) zkLookupSync(resourceType int, reschan chan<- *zk.ServerInfo) {
	// This is guaranteed to never block as the channel is always buffered.
	reschan <- c.zkClient.LocateResource(resourceType)
}
