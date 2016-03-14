package os

import (
	los "os"
)

// Exist check a file exist or not.
func Exist(filename string) bool {
	var err error
	_, err = los.Stat(filename)
	return err == nil || los.IsExist(err)
}
