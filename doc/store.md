# Store
Store is part of bfs, it's for small files stored service.

Table of Contents
=================

* [Store](#store)
* [Features](#features)
* [Architechure](#architechure)
	* [Needles](#needles)
    * [Needle Cache](#needle-cache)
    * [Superblock](#superblock)
    * [Index](#index)
    * [Volume](#volume)
* [Installation](#installation)
* [Config](#config)
* [Benchmark and Test](#benchmark-and-test)
* [Run](#run)
* [API](#api)
	* [Get](#get)
    * [Upload](#upload)
    * [Uploads](#uploads)
    * [Delete](#delete)
    * [Deletes](#deletes)
    * [Response](#apiresponse)
* [Admin](#admin)
    * [AddFreeVolume](#addfreevolume)
    * [AddVolume](#addvolume)
    * [BulkVolume](#bulkvolume)
    * [CompactVolume](#compactvolume)
    * [Response](#adminresponse)

* [Stat](#stat)

## Features
* crash safe and fast recovery meta data by index file or block file.
* add/append (many)/del/get files;
* compress block when has many del files (logic delete);
* bulk block when block broken we can copy from another small file in another machine, then replace;

[Back to TOC](#table-of-contents)

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
superblock contains a header and many needles, it's the needles container. superblock header contains magic(4 bytes) version(1 bytes) and padding(3bytes). when store crash, we can recovery from the original block file.                                              
 ---------------                                                            
| super   block |                                                           
 --------------- 
|     magic     |
|     version   |
|     padding   |                                        
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

[Back to TOC](#table-of-contents)

## Installation
* bfs/store development files are required.
* golang 1.5.1

just pull `Terry-Mao/bfs` from github using `go get`:

```sh
$ go get github.com/Terry-Mao/bfs
$ cd $GOPATH/github.com/Terry-Mao/store
$ go build
```

[Back to TOC](#table-of-contents)

## Config
store use yaml as a config file.

config file:

```yaml
# This is a TOML document. Boom.

# store golang pprof
Pprof = true
PprofListen  = "localhost:6060"

# store stat listen
StatListen   = "localhost:6061"

# api listen, get/upload/delete
ApiListen    = "localhost:6062"

# admin listen, add/del volume
AdminListen  = "localhost:6063"

# needle(pic) max size
NeedleMaxSize  = 10485760

# max batch upload 
BatchMaxNum    = 9

[Store]
# volume meta index
VolumeIndex      = "/tmp/volume.idx"

# free volume meta index
FreeVolumeIndex  = "/tmp/free_volume.idx"

[Volume]
# sync delete operation after N delete
SyncDelete  = 1024

# sync delete delay duration
SyncDeleteDelay  = "10s"

[Block]
# sync write operation after N write
SyncWrite      = 1

# use new kernel syscall syncfilerange
Syncfilerange  = true

[Index]
# index bufio size
BufferSize = 4096

# merge delay duration
MergeDelay  =  "10s"

# merge write after N write
MergeWrite  = 1024

# ring buffer cache
RingBuffer  = 10240

# sync write operation after N write
SyncWrite   = 1024

# use new kernel syscall syncfilerange
Syncfilerange = true

[Zookeeper]
# zookeeper root path.
Root  =  "/rack"

# store machine in which rack.
Rack  =  "bfs-test"

# serverid for store server, must unique in cluster
ServerId  = "47E273ED-CD3A-4D6A-94CE-554BA9B195EB"

# zookeeper cluster addrs
Addrs = [
    "localhost:2181"
]

# zookeeper heartbeat timeout.
Timeout = "1s"

```

index file contains volume block path, index path and volume id.

```sh
$> cat /tmp/store.idx
/tmp/hijohn_2,/tmp/hijohn_2.idx,2
/tmp/hijohn_2,/tmp/hijohn_2.idx,2
```

[Back to TOC](#table-of-contents)

## Benchmark and Test

```sh
# test
$ cd $GOPATH/github.com/Terry-Mao/bfs/store
$ go test -v
# benchmark
go test -v -bench=. -benchtime=10s
```

[Back to TOC](#table-of-contents)

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

[Back to TOC](#table-of-contents)

## API

### Get 

get a file

**URL**

http://DOMAIN/get

***HTTP Method***

GET

***Query String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |
| key       | true  | int64  | file key |
| cookie       | true  | int64  | file cookie |

### Upload

upload a file

**URL**

http://DOMAIN/upload

***HTTP Method***

POST multipart/form-data

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |
| key       | true  | int64  | file key |
| cookie       | true  | int64  | file cookie |


### Uploads

upload files, max upload files is 9 one time

**URL**

http://DOMAIN/uploads

***HTTP Method***

POST multipart/form-data

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |
| keys       | true  | string  | file keys (ie. 1,2,3) |
| cookies       | true  | string  | file cookies (ie. 1,2,3) |

### Delete

delete a file

**URL**

http://DOMAIN/del

***HTTP Method***

POST application/x-www-form-urlencoded

***Query String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |
| key       | true  | int64  | file key |

### Deletes

delete files, max delete files is 9 one time

**URL**

http://DOMAIN/dels

***HTTP Method***

POST application/x-www-form-urlencoded

***Query String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |
| keys       | true  | string  | file keys (ie. 1,2,3) |

### ApiResponse

response a json:

```json
{"ret": 1}
```

| error code | description |
| :---- | :----         |
| 1      | Succeed       |
| 65534 | param error |
| 65535   | internal error |

for more error code, see the [errors.go](https://github.com/Terry-Mao/bfs/blob/master/store/errors.go)

exmaples:

```shell
$ cd test
$ ./test.sh
```

[Back to TOC](#table-of-contents)

## Admin
### AddFreeVolume 

add a free(empty) volume

**URL**

http://DOMAIN/add\_free\_volume

***HTTP Method***

POST application/x-www-form-urlencoded

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| n        | true  | int32  | add volume number |
| bdir       | true  | int64  | block file dir |
| idir       | true  | int64  | index file dir |

### AddVolume 

add a volume with specified volume id, this method will find a free volume to use.

**URL**

http://DOMAIN/add\_volume

***HTTP Method***

POST application/x-www-form-urlencoded

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |

### CompactVolume 

compact a volume for save disk space, after compact block file all duplicated and deleted needles will ignore write to new block file, this method will find a free volume to use. (ONLINE)

**URL**

http://DOMAIN/compact\_volume

***HTTP Method***

POST application/x-www-form-urlencoded

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |


### BulkVolume 

bulk a volume from specified block file and index for recovery a new store machine.

**URL**

http://DOMAIN/bulk\_volume

***HTTP Method***

POST application/x-www-form-urlencoded

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| vid        | true  | int32  | volume id |
| bfile        | true  | string  | block file path |
| ifile        | true  | string  | index file path |


### AdminResponse

response a json:

```json
{"ret": 1}
```

| error code | description |
| :---- | :----         |
| 1      | Succeed       |
| 65534 | param error |
| 65535   | internal error |

for more error code, see the [errors.go](https://github.com/Terry-Mao/bfs/blob/master/store/errors.go)

exmaples:

```shell
$ cd test
$ ./test.sh
```


[Back to TOC](#table-of-contents)

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

[Back to TOC](#table-of-contents)