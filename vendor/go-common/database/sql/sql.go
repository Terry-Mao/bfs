package sql

import (
	"context"
	xsql "database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go-common/net/trace"
	"go-common/stat"
)

const (
	_family = "sql_client"
)

var (
	// ErrStmtNil prepared stmt error
	ErrStmtNil = errors.New("prepare failed and stmt nil")
	// ErrNoRows is returned by Scan when QueryRow doesn't return a row.
	// In such a case, QueryRow returns a placeholder *Row value that defers
	// this error until a Scan.
	ErrNoRows = xsql.ErrNoRows
	// ErrTxDone transaction done.
	ErrTxDone = xsql.ErrTxDone
)

// DB database.
type DB struct {
	*xsql.DB
	Stats stat.Stat
	addr  string
}

// Tx transaction.
type Tx struct {
	db   *DB
	tx   *xsql.Tx
	t    *trace.Trace2
	addr string
}

// Row row.
type Row struct {
	row *xsql.Row
	t   *trace.Trace2
}

// Rows rows.
type Rows struct {
	*xsql.Rows
}

// Stmt prepared stmt.
type Stmt struct {
	db    *DB
	stmt  atomic.Value
	query string
	t     *trace.Trace2
	tx    bool
	addr  string
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a database
// name and connection information.
func Open(driverName, dataSourceName string) (*DB, error) {
	d, err := xsql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: d}, nil
}

// Begin starts a transaction. The isolation level is dependent on the driver.
func (db *DB) Begin(c context.Context) (tx *Tx, err error) {
	var txi *xsql.Tx
	if txi, err = db.DB.Begin(); err != nil {
		return
	}
	t, ok := trace.FromContext2(c)
	if ok {
		t = t.Fork(_family, "begin", db.addr)
		t.Client("")
	}
	return &Tx{tx: txi, t: t, addr: db.addr}, nil
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) Exec(c context.Context, query string, args ...interface{}) (res xsql.Result, err error) {
	if t, ok := trace.FromContext2(c); ok {
		t = t.Fork(_family, "exec", db.addr)
		t.Client(query)
		defer t.Done(&err)
	}
	if db.Stats != nil {
		now := time.Now()
		defer func() {
			db.Stats.Timing("mysql:exec", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = db.DB.Exec(query, args...)
	return
}

// Ping verifies a connection to the database is still alive, establishing a
// connection if necessary.
func (db *DB) Ping(c context.Context) (err error) {
	if t, ok := trace.FromContext2(c); ok {
		t = t.Fork(_family, "ping", db.addr)
		t.Client("")
		defer t.Done(&err)
	}
	err = db.DB.Ping()
	return
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepare(query string) (*Stmt, error) {
	stmt, err := db.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	st := &Stmt{query: query, addr: db.addr, db: db}
	st.stmt.Store(stmt)
	return st, nil
}

// Prepared creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepared(query string) (stmt *Stmt) {
	stmt = &Stmt{query: query, addr: db.addr, db: db}
	s, err := db.DB.Prepare(query)
	if err == nil {
		stmt.stmt.Store(s)
		return
	}
	go func() {
		for {
			s, err := db.DB.Prepare(query)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			stmt.stmt.Store(s)
			return
		}
	}()
	return
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (db *DB) Query(c context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	var rrows *xsql.Rows
	if t, ok := trace.FromContext2(c); ok {
		t = t.Fork(_family, "query", db.addr)
		t.Client(query)
		defer t.Done(&err)
	}
	if db.Stats != nil {
		now := time.Now()
		defer func() {
			db.Stats.Timing("mysql:query", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	if rrows, err = db.DB.Query(query, args...); err == nil {
		rows = &Rows{rrows}
	}
	return
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (db *DB) QueryRow(c context.Context, query string, args ...interface{}) *Row {
	t, ok := trace.FromContext2(c)
	if ok {
		t = t.Fork(_family, "queryrow", db.addr)
		t.Client(query)
	}
	if db.Stats != nil {
		now := time.Now()
		defer func() {
			db.Stats.Timing("mysql:queryRow", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	row := db.DB.QueryRow(query, args...)
	return &Row{row: row, t: t}
}

// Close closes the statement.
func (s *Stmt) Close() error {
	var (
		ok   bool
		stmt *xsql.Stmt
	)
	if stmt, ok = s.stmt.Load().(*xsql.Stmt); ok {
		return stmt.Close()
	}
	return nil
}

// Exec executes a prepared statement with the given arguments and returns a
// Result summarizing the effect of the statement.
func (s *Stmt) Exec(c context.Context, args ...interface{}) (res xsql.Result, err error) {
	var (
		ok   bool
		stmt *xsql.Stmt
	)
	if s.tx {
		if s.t != nil {
			s.t.Annotation(s.query)
		}
	} else {
		if t, ok := trace.FromContext2(c); ok {
			t = t.Fork(_family, "exec", s.addr)
			t.Client(s.query)
			defer t.Done(&err)
		}
	}
	if s.db.Stats != nil {
		now := time.Now()
		defer func() {
			s.db.Stats.Timing("mysql:stmt:exec", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	if stmt, ok = s.stmt.Load().(*xsql.Stmt); !ok {
		err = ErrStmtNil
		return
	}
	res, err = stmt.Exec(args...)
	return
}

// Query executes a prepared query statement with the given arguments and
// returns the query results as a *Rows.
func (s *Stmt) Query(c context.Context, args ...interface{}) (rows *Rows, err error) {
	var (
		ok    bool
		t     *trace.Trace2
		stmt  *xsql.Stmt
		rrows *xsql.Rows
	)
	if s.tx {
		if s.t != nil {
			s.t.Annotation(s.query)
		}
	} else {
		if t, ok = trace.FromContext2(c); ok {
			t = t.Fork(_family, "query", s.addr)
			t.Client(s.query)
			defer t.Done(&err)
		}
	}
	if stmt, ok = s.stmt.Load().(*xsql.Stmt); !ok {
		err = ErrStmtNil
		return
	}
	if s.db.Stats != nil {
		now := time.Now()
		defer func() {
			s.db.Stats.Timing("mysql:stmt:query", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	if rrows, err = stmt.Query(args...); err == nil {
		rows = &Rows{rrows}
	}
	return
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error will
// be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
func (s *Stmt) QueryRow(c context.Context, args ...interface{}) (r *Row) {
	var (
		ok   bool
		stmt *xsql.Stmt
	)
	r = &Row{}
	if s.tx {
		if s.t != nil {
			s.t.Annotation(s.query)
		}
	} else {
		if t, ok := trace.FromContext2(c); ok {
			t = t.Fork(_family, "queryrow", s.addr)
			t.Client(s.query)
			r.t = t
		}
	}
	if s.db.Stats != nil {
		now := time.Now()
		defer func() {
			s.db.Stats.Timing("mysql:stmt:queryrow", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	if stmt, ok = s.stmt.Load().(*xsql.Stmt); ok {
		r.row = stmt.QueryRow(args...)
	}
	return
}

// Commit commits the transaction.
func (tx *Tx) Commit() (err error) {
	if tx.t != nil {
		defer tx.t.Done(&err)
	}
	err = tx.tx.Commit()
	return
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() (err error) {
	if tx.t != nil {
		defer tx.t.Done(&err)
	}
	err = tx.tx.Rollback()
	return
}

// Exec executes a query that doesn't return rows. For example: an INSERT and
// UPDATE.
func (tx *Tx) Exec(query string, args ...interface{}) (res xsql.Result, err error) {
	if tx.t != nil {
		tx.t.Annotation(fmt.Sprintf("%s %s", "exec", query))
	}
	if tx.db.Stats != nil {
		now := time.Now()
		defer func() {
			tx.db.Stats.Timing("mysql:tx:Exec", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = tx.tx.Exec(query, args...)
	return
}

// Query executes a query that returns rows, typically a SELECT.
func (tx *Tx) Query(query string, args ...interface{}) (rows *Rows, err error) {
	var rrows *xsql.Rows
	if tx.t != nil {
		tx.t.Annotation(fmt.Sprintf("%s %s", "query", query))
	}
	if tx.db.Stats != nil {
		now := time.Now()
		defer func() {
			tx.db.Stats.Timing("mysql:tx:Query", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	if rrows, err = tx.tx.Query(query, args...); err == nil {
		rows = &Rows{rrows}
	}
	return
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (tx *Tx) QueryRow(query string, args ...interface{}) (row *xsql.Row) {
	if tx.t != nil {
		tx.t.Annotation(fmt.Sprintf("%s %s", "queryrow", query))
	}
	if tx.db.Stats != nil {
		now := time.Now()
		defer func() {
			tx.db.Stats.Timing("mysql:tx:QueryRow", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	row = tx.tx.QueryRow(query, args...)
	return
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	var (
		ok bool
		as *xsql.Stmt
	)
	if as, ok = stmt.stmt.Load().(*xsql.Stmt); !ok {
		// TODO
	}
	ts := tx.tx.Stmt(as)
	st := &Stmt{query: stmt.query, tx: true, t: tx.t, addr: tx.addr}
	st.stmt.Store(ts)
	return st
}

// Prepare creates a prepared statement for use within a transaction.
// The returned statement operates within the transaction and can no longer be
// used once the transaction has been committed or rolled back.
// To use an existing prepared statement on this transaction, see Tx.Stmt.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	if tx.t != nil {
		tx.t.Annotation(fmt.Sprintf("%s %s", "prepare", query))
	}
	stmt, err := tx.tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	st := &Stmt{query: query, tx: true, t: tx.t, addr: tx.addr}
	st.stmt.Store(stmt)
	return st, nil
}

// Scan copies the columns from the matched row into the values pointed at by dest.
func (r *Row) Scan(dest ...interface{}) (err error) {
	if r.t != nil {
		defer r.t.Done(&err)
	}
	if r.row != nil {
		err = r.row.Scan(dest...)
	} else {
		err = ErrStmtNil
	}
	return
}
