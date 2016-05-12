package main

import (
	"flag"
	"strings"
)

func dfs_recur(bucket, dirname string) {
	var (
		exit  bool
		dir   string
		file  string
		files []string
		dirs  []string
	)

	dirs, files, _ = scanDir(bucket, dirname)
	for _, dir = range dirs {
		dir = dirname + dir
		dfs_recur(bucket, dir)
	}
	for _, file = range files {
		delFile(bucket, file)
	}
	return
}

func delFile(bucket, filename string) (err error) {
	if strings.HasSufix(filename, "/") {
		s.bfs.Delete(bucket, filename)
	}
}

func scanDir(bucket, filename string) (keys []string, err error) {

}

func main() {
	var (
		bucket   string
		filename string
	)
	flag.Parse()
	bucket = "test"
	filename = "aaaa"

	dfs(bucket, filename, delFile, scanDir)
}
