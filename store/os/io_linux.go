// +build linux
package os

import (
	"syscall"
)

const (
	O_NOATIME = syscall.O_NOATIME
)
