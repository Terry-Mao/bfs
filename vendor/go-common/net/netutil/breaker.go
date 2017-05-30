package netutil

import (
	"sync"
	"sync/atomic"
	"time"

	"go-common/conf"
)

type window struct {
	lock       sync.RWMutex
	buckets    []bucket
	bucketTime int64
	lastAccess int64
	cur        *bucket
}

func newWindow(wt time.Duration, wb int) *window {
	buckets := make([]bucket, wb)
	bucket := &buckets[0]
	for i := 1; i < wb; i++ {
		bucket.next = &buckets[i]
		bucket = bucket.next
	}
	bucket.next = &buckets[0]
	bucketTime := time.Duration(wt.Nanoseconds() / int64(wb))
	return &window{
		cur:        &buckets[0],
		buckets:    buckets,
		bucketTime: int64(bucketTime),
		lastAccess: time.Now().UnixNano(),
	}
}

func (w *window) lastBucket() (b *bucket) {
	var (
		i       int
		elapsed int64
		now     = time.Now().UnixNano()
	)
	b = w.cur
	if elapsed = now - w.lastAccess; elapsed <= w.bucketTime {
		return
	}
	// Reset the buckets between now and number of buckets ago. If
	// that is more that the existing buckets, reset all.
	if i = int(elapsed / w.bucketTime); i > len(w.buckets) {
		i = len(w.buckets)
	}
	for i > 0 {
		b = b.next
		b.Reset()
		i--
	}
	w.lastAccess = now
	w.cur = b
	return
}

func (w *window) Success() {
	w.lock.Lock()
	w.lastBucket().Success()
	w.lock.Unlock()
}

func (w *window) Fail() {
	w.lock.Lock()
	w.lastBucket().Fail()
	w.lock.Unlock()
}

// Stat get window total requests.
func (w *window) Stat() (uint64, float32) {
	var (
		b           *bucket
		total, fail uint64
	)
	w.lock.RLock()
	defer w.lock.RUnlock()
	for i := 0; i < len(w.buckets); i++ {
		b = &w.buckets[i]
		total += b.success + b.failure
		fail += b.failure
	}
	if total == 0 {
		return 0, 0.0
	}
	return total, float32(fail) / float32(total)
}

// Reset reset window counter.
func (w *window) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()
	for i := 0; i < len(w.buckets); i++ {
		w.buckets[i].Reset()
	}
}

type bucket struct {
	failure uint64
	success uint64
	next    *bucket
}

func (b *bucket) Success() {
	b.success++
}

func (b *bucket) Fail() {
	b.failure++
}

func (b *bucket) Reset() {
	b.success = 0
	b.failure = 0
}

const (
	// StateOpen when circuit breaker open, request not allowed, after sleep
	// some duration, allow one single request for testing the health, if ok
	// then state reset to closed, if not continue the step.
	StateOpen int32 = iota
	// StateClosed when circuit breaker closed, request allowed, the breaker
	// calc the succeed ratio, if request num greater request setting and
	// ratio lower than the setting ratio, then reset state to open.
	StateClosed
)

// Breaker is a CircuitBreaker pattern.
type Breaker struct {
	count   *window
	state   int32
	last    int64
	ratio   float32
	request uint64
	sleep   int64

	// State specifies an optional callback function that is
	// called when a breaker changes state. See the
	// associated constants for details.
	State func(int32)
}

// NewBreaker new a breaker.
func NewBreaker(c *conf.Breaker) *Breaker {
	return &Breaker{
		count:   newWindow(time.Duration(c.Window), c.Bucket),
		state:   StateClosed,
		last:    time.Now().UnixNano(),
		ratio:   c.Ratio,
		request: c.Request,
		sleep:   int64(time.Duration(c.Sleep)),
	}
}

// Allow this takes into account the half-open logic which allows some requests
// through when determining if it should be closed again.
func (b *Breaker) Allow() bool {
	return !b.isOpen() || b.allowSingle()
}

func (b *Breaker) allowSingle() bool {
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&b.last)
	if now-last > b.sleep {
		return atomic.CompareAndSwapInt64(&b.last, last, now)
	}
	return false
}

// isOpen whether the circuit is currently open (tripped).
func (b *Breaker) isOpen() bool {
	if atomic.LoadInt32(&b.state) == StateOpen {
		return true
	}
	if t, r := b.count.Stat(); t < b.request || r < b.ratio {
		return false
	}
	if atomic.CompareAndSwapInt32(&b.state, StateClosed, StateOpen) {
		atomic.StoreInt64(&b.last, time.Now().UnixNano())
		if b.State != nil {
			b.State(StateOpen)
		}
	}
	return true
}

// reset reset the breaker.
func (b *Breaker) reset() {
	if atomic.CompareAndSwapInt32(&b.state, StateOpen, StateClosed) {
		b.count.Reset()
		if b.State != nil {
			b.State(StateClosed)
		}
	}
}

// Success records a success in the current bucket.
func (b *Breaker) Success() {
	if atomic.LoadInt32(&b.state) != StateOpen {
		// only closed incr success
		b.count.Success()
	} else {
		// if half-open then success then reset the counter
		b.reset()
	}
}

// Fail records a failure in the current bucket.
func (b *Breaker) Fail() {
	b.count.Fail()
}
