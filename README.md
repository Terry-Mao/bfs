bfs
==============
`bfs` 是基于facebook haystack 用golang实现的小文件存储系统。

---------------------------------------
  * [特性](#特性)
  * [安装](#安装)
  * [集群](#集群)
  * [API](#API)
  * [更多](#更多)

---------------------------------------

## 特性
 * 高吞吐量和低延迟
 * 容错性
 * 高效
 * 维护简单

## 安装

### 一、安装hbase、zookeeper

 * 参考hbase官网. 安装、启动请查看[这里](https://hbase.apache.org/).
 * 参考zookeeper官网. 安装、启动请查看[这里](http://zookeeper.apache.org/).

### 二、搭建golang、python环境

 * 参考golang官网. 安装请查看[这里](https://golang.org/doc/install).
 * 参考python官网. 安装请查看[这里]
(https://www.python.org/)

### 三、安装gosnowflake

 * 参考[这里](https://github.com/Terry-Mao/gosnowflake)

### 四、部署
1.下载bfs及依赖包
```sh
$ go get -u github.com/Terry-Mao/bfs
$ cd /data/apps/go/src/github.com/Terry-Mao/bfs
$ go get ./...
```

2.安装directory、store、pitchfork、proxy模块(配置文件请依据实际机器环境配置)
```sh
$ cd $GOPATH/src/github.com/Terry-Mao/bfs/directory
$ go install
$ cp directory.toml $GOPATH/bin/directory.toml
$ cd ../store/
$ go install
$ cp store.toml $GOPATH/bin/store.toml
$ cd ../pitchfork/
$ go install
$ cp pitchfork.toml $GOPATH/bin/pitchfork.toml
$ cd ../proxy
$ go install
$ cp proxy.toml $GOPATH/bin/proxy.toml

```
到此所有的环境都搭建完成！

### 五、启动
```sh
$ cd /$GOPATH/bin
$ nohup $GOPATH/bin/directory -c $GOPATH/bin/directory.toml &
$ nohup $GOPATH/bin/store -c $GOPATH/bin/store.toml &
$ nohup $GOPATH/bin/pitchfork -c $GOPATH/bin/pitchfork.toml &
$ nohup $GOPATH/bin/proxy -c $GOPATH/bin/proxy.toml &
$ cd $GOPATH/github.com/Terry-Mao/bfs/ops
$ nohup python runserver.py &
```

### 六、测试
 * bfs初始化，分配存储空间，请查看[这里](https://github.com/Terry-Mao/bfs/doc/ops.md)
 * 请求bfs，请查看[这里](https://github.com/Terry-Mao/bfs/doc/proxy.md)

## 集群

![Aaron Swartz](http://i0.hdslb.com/bfs/active/bfs_server.png)

### directory

 * directory主要负责请求的均匀调度和元数据管理，元数据存放在hbase，由gosnowflake产生文件key

### store

 * store主要负责文件的物理存储

### pitchfork

 * pitchfork负责监控store的服务状态、可用性和磁盘状态

### proxy

 * proxy作为bfs存储的代理以及维护bucket相关

### ops

 * ops作为bfs的后台管理界面，负责分配存储、扩容、压缩等维护工作
 
## API
[api文档](https://github.com/Terry-Mao/bfs/blob/master/doc/api.md)

## 更多

 * [bfs-image-server](https://github.com/YonkaFang/bfs-image-server) 
