## Terry-Mao/gosnowflake

`Terry-Mao/gosnowflake` is a network service for generating unique ID numbers at high scale with some simple guarantees (golang).

## Requeriments

golang 1.2 is required.

zookeeper is required.

## Installation

Just pull `Terry-Mao/gosnowflake` from github using `go get`:

```sh
# install golang from https://code.google.com/p/go/downloads/list
# here golang 1.2(linux-amd64)
$ wget https://go.googlecode.com/files/go1.2.linux-amd64.tar.gz
$ tar -xvf go1.2.linux-amd64.tar.gz
$ cp -R go /usr/local/
$ vim /etc/profile.d/golang.sh
# add below to golang.sh
# export GOROOT=/usr/local/go
# export PATH=$PATH:$GOROOT/bin
# export GOPATH=/data/apps/go
$ source /etc/profile.d/golang.sh
# download the code
$ go get -u github.com/Terry-Mao/gosnowflake
# find the dir
$ cd $GOPATH/src/github.com/Terry-Mao/gosnowflake
# compile
$ go install
$ cp ./gosnowflake-example.conf $GOPATH/bin/gosnowflake.conf
$ cd $GOPATH/bin
# run
$ ./gosnowflake -conf=./gosnowflake.conf
# for help
$ ./gosnowflake -h
# test
$ cd $GOPATH/src/github.com/Terry-Mao/gosnowflake/client
$ go test -conf=./test-examples.conf
```

## Document
```sh
# gosnowflake configuration file example

# Note on units: when memory size is needed, it is possible to specify
# it in the usual form of 1k 5GB 4M and so forth:
#
# 1kb => 1024 bytes
# 1mb => 1024*1024 bytes
# 1gb => 1024*1024*1024 bytes
#
# units are case insensitive so 1GB 1Gb 1gB are all the same.

# Note on units: when time duration is needed, it is possible to specify
# it in the usual form of 1s 5M 4h and so forth:
#
# 1s => 1000 * 1000 * 1000 nanoseconds
# 1m => 60 seconds
# 1h => 60 minutes
#
# units are case insensitive so 1h 1H are all the same.

[base]
# When running daemonized, gosnowflake writes a pid file in 
# /tmp/gosnowflake.pid by default. You can specify a custom pid file 
# location here.
pid /tmp/gosnowflake.pid

# Sets the maximum number of CPUs that can be executing simultaneously.
# This call will go away when the scheduler improves. By default the number of 
# logical CPUs is set.
# 
# maxproc 4

# By default gosnowflake listens for connections from all the network interfaces
# available on the server on 8080 port. It is possible to listen to just one or 
# multiple interfaces using the "rpc.bind" configuration directive, 
# followed by one or more IP addresses and port.
#
# Examples:
#
# Note this directive is only support "golang rpc" protocol
# rpc.bind 192.168.1.100:8080,10.0.0.1:8080
# rpc.bind 127.0.0.1:8080
# rpc.bind :8080

# By default gosnowflake thrift listens for connections from all the network interfaces
# available on the server on 8080 port. It is possible to listen to just one or 
# multiple interfaces using the "rpc.bind" configuration directive, 
# followed by one or more IP addresses and port.
#
# Examples:
#
# Note this directive is only support "thrift" protocol
# thrift.bind 192.168.1.100:8080,10.0.0.1:8080
# thrift.bind 127.0.0.1:8080
# thrift.bind :8080

# This is used by gosnowflake service profiling (pprof).
# By default gosnowflake pprof listens for connections from local interfaces on 6971
# port. It's not safty for listening internet IP addresses.
#
# Examples:
#
# pprof.bind 192.168.1.100:6971,10.0.0.1:6971
# pprof.bind 127.0.0.1:6971
# pprof.bind :6971

# This is used by gosnowflake service get stat info by http.
# By default gosnowflake pprof listens for connections from local interfaces on 6972
# port. It's not safty for listening internet IP addresses.
#
# Examples:
#
# stat.bind 192.168.1.100:6971,10.0.0.1:6971
# stat.bind 127.0.0.1:6971
# stat.bind :6971

# The working directory.
#
# The log will be written inside this directory, with the filename specified
# above using the 'logfile' configuration directive.
#  
# Note that you must specify a directory here, not a file name.
dir ./

# Log4go configuration path
log ./log.xml

################################## ZOOKEEPER ##################################
[zookeeper]
# The zookeeper cluster section. When gosnowflake start, it will register data 
# in the zookeeper cluster and create a ephemeral node. When gosnowflake died, 
# the node will drop by zookeeper cluster. 

# Zookeeper cluster addresses. Mutiple address split by a ",".
# Examples:
#
# addr 192.168.1.100:2181,10.0.0.1:2181
# addr 127.0.0.1:2181
# addr 10.20.216.122:2181

# Zookeeper cluster session idle timeout seconds. Zookeeper will close the 
# connection after a client is idle for N seconds.
# Examples:
#
# timeout 30s
# timeout 15s
timeout 30s

# gosnowflake zookeeper root path.
path /gosnowflake-servers

################################## GOSNOWFLAKE ################################
[snowflake]
# snowflake must set a datacenter [0, 31], must be unique in all datacenter.
# Examples:
#
# datacenter 0
# datacenter 1
datacenter 0

# register which worker, must be unique in one datacenter.
# multiple worker id register can split by a ",".
# Examples:
#
# worker 0
# worker 0,1,2
worker 0,1,2

```

## RPC API

`SnowflakeRPC.NextId`: generate a snowflake id.

`SnowflakeRPC.DatacenterId`: get gosnowflake service's datacenterId.

`SnowflakeRPC.Timestamp`: get gosnowflake service's current timestamp.

`SnowflakeRPC.Ping`: get gosnowflake service's status.

## Usage

```go
if err := Init(MyConf.ZKServers, MyConf.ZKPath, MyConf.ZKTimeout); err != nil {
    panic(err)
}
c := NewClient(MyConf.WorkerId)                                             
defer c.Close()
defer c.Destroy()
time.Sleep(1 * time.Second)                                             
id, err := c.Id()                                                       
if err != nil {                                                         
    panic(err)
}                                                                       
fmt.Printf("gosnwoflake id: %d\n", id)                                  
```

## Highly Available

use `heartbeat` or `keepalived` apply a VIP for the client.

a easy keepalived configration: [here](https://github.com/Terry-Mao/gosnowflake/tree/master/keepalived)

(1.0.1)


use zookeeper for client failover

golang client sdk: [here](https://github.com/Terry-Mao/gosnowflake/tree/master/client)

(1.1)

## LICENSE

`gosnowflake` is is distributed under the terms of the GNU General Public License, version 3.0 [GPLv3](http://www.gnu.org/licenses/gpl.txt)
