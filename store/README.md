# Store
Store is part of bfs, it's for small files stored service.

## Features
* crash safe and fast recovery meta data by index file or block file.
* add/append (many)/del/get files;
* compress block when has many del files (logic delete);
* bulk block when block broken we can copy from another small file in another machine, then replace;

## Architechure
### Needles
needle is the raw data of small file in the disk, they are stored one by one in disk file. it's aligned with 8 bytes, we used a uint32 stored offset to the file, so the max needle size is 8 * 4GB = 32GB.
Needle stored int super block, aligned to 8bytes.                           
                                                                            
needle file format:  

| Filed  | explanation  | 
|:------------- |:---------------|
| magic     | header magic number used for checksum  | 
| cookie     | random number to mitigate brute force lookups        | 
| key | 64bit photo id         |
| flag |   signifies deleted status       |
| size | data size        |
| data | the actual photo data        |
| magic | footer magic number used for checksum      |
| checksum | used to check integrity        |
| padding | total needle size is aligned to 8 bytes   |

### Needle Cache
needle cache saved the offset & size for a photo id. so it can fast get small file meta info without any io operations. needle cache is a map[int64]NeedleCache, NeedleCache also is a int64, high 32 bit is offset, low 32 bit is size.
 
### Superblock
superblock contains many needles, it's the needles container. when store crash, we can recovery from the original block file.                                              
 ---------------                                                            
| super   block |                                                           
 ---------------                                                            
|     needle    |                              
|     needle    |                                     
|     ......    |                                 
|     ......    |                                 
 ---------------

### Index
index is for fast recovery needle cahce. original block file always very big (32GB), if scan block file may cost long time to recovery needle cache, index only contain key, offset, size, it's a 16byte one by one in disk.

index file format:

| Filed  | explanation  | 
|:------------- |:---------------|
| key     | needle key (photo id)  | 
| offset     | needle offset in super block (aligned) | 
| size | needle data size |

### Volume
store has many volumes, volume has a unique id in one store server. one volume has one block and one index. we call add/write/get/del all cross volume struct. volume merge all del opertion and sort in memory by offset. volume also contains the needle cache map. the block in volume ensure only one writer can write needle, the reader is lock-free, so we can get photo by many readers.


## Installation
* bfs/store development files are required.
* golang 1.5.1

just pull `Terry-Mao/bfs` from github using `go get`:

```sh
$ go get github.com/Terry-Mao/bfs
$ cd $GOPATH/github.com/Terry-Mao/store
$ go build
```

## Config
store use yaml as a config file.

config file:

```yaml
# pprof
pprof:
  # enable golang pprof
  enable: true
  # pprof http addr
  addr: localhost:6060

# stat http addr
# stat api: http://http_stat_addr/stat
stat: localhost:6061

# store index for find volumes.
index: /tmp/hijohn.idx

# zookeeper address.
zk: ["1", "2"]
```

index file contains volume block path, index path and volume id.

```sh
$> cat /tmp/store.idx
/tmp/hijohn_2,/tmp/hijohn_2.idx,2
/tmp/hijohn_2,/tmp/hijohn_2.idx,2
```

## Benchmark & Test

```sh
# test
$ cd $GOPATH/github.com/Terry-Mao/bfs/store
$ go test -v
# benchmark
go test -v -bench=. -benchtime=10s
```

## Run

```sh
$ go install
$ cd $GOPATH/bin
$ ./store -c ./store.yaml
```
the command flags:

```sh
$ ./store -h
Usage of ./store:
  -alsologtostderr
    	log to standard error as well as files
  -c string
    	set config file path (default "./store.yaml")
  -log_backtrace_at value
    	when logging hits line file:N, emit a stack trace (default :0)
  -log_dir string
    	If non-empty, write log files in this directory
  -logtostderr
    	log to standard error instead of files
  -stderrthreshold value
    	logs at or above this threshold go to stderr
  -v value
    	log level for V logs
  -vmodule value
    	comma-separated list of pattern=N settings for file-filtered logging
```

## Stat

stat let us get and statistics about the server in a json format that is simple 
to parse by computers and easy to read by humans.
The optional parameter can be used to select a specific section of information:

* server: general information about the store server;
* volumes: general statistics about volume;

```sh
# the http addr can config in store.yaml
$ curl http://localhost:6061/info
```

Have Fun!
