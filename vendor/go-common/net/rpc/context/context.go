package context

import (
	ctx "context"
	"time"
)

// Context web context interface
type Context interface {
	ctx.Context
	Now() time.Time
	Seq() uint64
	ServiceMethod() string
	User() string
}

// rpcCtx only used in srpc.
type rpcCtx struct {
	ctx.Context
	now           time.Time
	seq           uint64
	serviceMethod string
	user          string
}

// NewContext new a rpc context.
func NewContext(c ctx.Context, u, m string, s uint64) Context {
	rc := &rpcCtx{Context: c, now: time.Now(), seq: s, serviceMethod: m, user: u}
	return rc
}

// Seq implement Context method Seq.
func (c *rpcCtx) Seq() uint64 {
	return c.seq
}

// ServiceMethod implement Context method ServiceMethod.
func (c *rpcCtx) ServiceMethod() string {
	return c.serviceMethod
}

// Now get current time.
func (c *rpcCtx) Now() time.Time {
	return c.now
}

// User get client user
func (c *rpcCtx) User() string {
	return c.user
}
