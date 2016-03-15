bfs
==============
`Terry-Mao/bfs` 是基于facebook haystack 用golang实现的小文件存储系统）。

---------------------------------------
  * [特性](#特性)
  * [安装](#安装)
  * [配置](#配置)
  * [例子](#例子)
  * [文档](#文档)
  * [集群](#集群)
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

### 二、搭建golang环境

参考golang官网. 安装请查看[这里](https://golang.org/doc/install).

### 三、安装gosnowflake

参考[这里](https://github.com/Terry-Mao/gosnowflake)

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
$ python runserver.py &
```

### 六、测试



## 配置


## 集群

### directory

xxx

### store

xxx

### pitchfork

xxx

### proxy

xxx

### ops

xxx
