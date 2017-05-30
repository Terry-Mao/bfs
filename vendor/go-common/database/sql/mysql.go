package sql

import (
	"go-common/conf"
	"go-common/log"

	// go-sql-driver/mysql
	_ "github.com/go-sql-driver/mysql"
)

// NewMySQL new db and retry connection when has error.
func NewMySQL(c *conf.MySQL) (db *DB) {
	var err error
	if db, err = Open("mysql", c.DSN); err != nil {
		log.Error("open mysql error(%v)", err)
		panic(err)
	}
	db.addr = c.Addr
	db.SetMaxOpenConns(c.Active)
	db.SetMaxIdleConns(c.Idle)
	return
}
