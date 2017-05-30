package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"runtime"
	"sync"

	log "golang/log4go"
)

var (
	sys    *Syslog
	logger log.Logger
	elk    *ELK
	bufP   sync.Pool
)

type Config struct {
	Dir    string
	ELK    *ELKConfig
	Syslog *SysConfig
}

// Init init xlog log4go or elk
// if dir is not empty, then log4go init into dir.
// if elk config is not nil, then will send udp to elk.
func Init(c *Config) {
	if c.Dir != "" {
		logger = log.Logger{}
		log.LogBufferLength = 10240
		// new info writer
		iw := log.NewFileLogWriter(path.Join(c.Dir, "info.log"), false)
		iw.SetRotateDaily(true)
		iw.SetRotateSize(math.MaxInt32)
		iw.SetFormat("[%D %T] [%L] [%S] %M")
		logger["info"] = &log.Filter{
			Level:     log.INFO,
			LogWriter: iw,
		}
		// new warning writer
		ww := log.NewFileLogWriter(path.Join(c.Dir, "warning.log"), false)
		ww.SetRotateDaily(true)
		ww.SetRotateSize(math.MaxInt32)
		ww.SetFormat("[%D %T] [%L] [%S] %M")
		logger["warning"] = &log.Filter{
			Level:     log.WARNING,
			LogWriter: ww,
		}
		// new error writer
		ew := log.NewFileLogWriter(path.Join(c.Dir, "error.log"), false)
		ew.SetRotateDaily(true)
		ew.SetRotateSize(math.MaxInt32)
		ew.SetFormat("[%D %T] [%L] [%S] %M")
		logger["error"] = &log.Filter{
			Level:     log.ERROR,
			LogWriter: ew,
		}
	}
	if c.ELK != nil {
		bufP.New = func() interface{} { return bytes.NewBuffer([]byte{}) }
		elk = NewELK(c.ELK)
	}
	if c.Syslog != nil {
		bufP.New = func() interface{} { return bytes.NewBuffer([]byte{}) }
		sys = NewSysLog(c.Syslog)
	}
}

// Close close resource.
func Close() {
	if logger != nil {
		logger.Close()
	}
	if elk != nil {
		elk.Close()
	}
	if sys != nil {
		sys.Close()
	}
}

// Info write info log to file or elk.
func Info(format string, args ...interface{}) {
	if logger != nil {
		logger.Info(format, args...)
	}
	if elk != nil {
		elk.Info("-", funcName(), escape(fmt.Sprintf(format, args...)), 0)
	}
	if sys != nil {
		sys.Info("-", funcName(), escape(fmt.Sprintf(format, args...)), 0)
	}
}

// Warn write warn log to file or elk.
func Warn(format string, args ...interface{}) {
	if logger != nil {
		logger.Warn(format, args...)
	}
	if elk != nil {
		elk.Warn("-", funcName(), escape(fmt.Sprintf(format, args...)), 0)
	}
	if sys != nil {
		sys.Warn("-", funcName(), escape(fmt.Sprintf(format, args...)), 0)
	}
}

// Error write error log to file or elk.
func Error(format string, args ...interface{}) {
	if logger != nil {
		logger.Error(format, args...)
	}
	if elk != nil {
		elk.Error("-", funcName(), escape(fmt.Sprintf(format, args...)), 0)
	}
	if sys != nil {
		sys.Error("-", funcName(), escape(fmt.Sprintf(format, args...)), 0)
	}
}

// InfoTrace write info log to file or elk with traceid.
func InfoTrace(traceID, path, msg string, tm float64) {
	if logger != nil {
		logger.Info("traceid:%s path:%s msg:%s time:%f", traceID, path, msg, tm)
	}
	if elk != nil {
		elk.Info(traceID, path, msg, tm)
	}
	if sys != nil {
		sys.Info(traceID, path, msg, tm)
	}
}

// funcName get func name.
func funcName() (fname string) {
	if pc, _, lineno, ok := runtime.Caller(2); ok {
		fname = fmt.Sprintf("%s:%d", runtime.FuncForPC(pc).Name(), lineno)
	}
	return
}

// escape escape html characters.
func escape(src string) (dst string) {
	buf, ok := bufP.Get().(*bytes.Buffer)
	if !ok {
		return
	}
	json.HTMLEscape(buf, []byte(src))
	dst = buf.String()
	buf.Reset()
	bufP.Put(buf)
	return
}
