package memcache

import (
	"errors"
)

var (
	// ErrNotFound not found
	ErrNotFound = errors.New("gomemcache: key not found")
	// ErrExists exists
	ErrExists = errors.New("gomemcache: key exists")
	// ErrNotStored not stored
	ErrNotStored = errors.New("gomemcache: key not stored")

	// ErrPoolExhausted is returned from a pool connection method (Store, Get,
	// Delete, IncrDecr, Err) when the maximum number of database connections
	// in the pool has been reached.
	ErrPoolExhausted = errors.New("gomemcache: connection pool exhausted")
	// ErrPoolClosed pool closed
	ErrPoolClosed = errors.New("gomemcache: connection pool closed")
	// ErrConnClosed conn closed
	ErrConnClosed = errors.New("gomemcache: connection closed")
)
