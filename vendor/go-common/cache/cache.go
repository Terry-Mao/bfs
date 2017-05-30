package cache

import (
	"errors"

	"go-common/stat"
)

var (
	// ErrFull cache internal chan full.
	ErrFull = errors.New("cache chan full")
)

// Cache async save data by chan.
type Cache struct {
	ch    chan func()
	Stats stat.Stat
}

// New new a cache struct.
func New(size int) *Cache {
	c := &Cache{
		ch: make(chan func(), size),
	}
	go c.proc()
	return c
}

func (c *Cache) proc() {
	for {
		if f := <-c.ch; f != nil {
			f()
		}
	}
}

// Save save a callback cache func.
func (c *Cache) Save(f func()) (err error) {
	select {
	case c.ch <- f:
	default:
		err = ErrFull
		if c.Stats != nil {
			c.Stats.State("cache_channel", int64(len(c.ch)))
		}
	}
	return
}
