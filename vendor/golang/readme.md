### Golang

### Version 2.10.0
> 1. 去除rpcx

#### Version 2.9.1

> 1. 临时加回rpcx，TODO remove
> 2. 去掉golang.org/x/context，使用标准库context

#### Version 2.9.0

> 1. 去除了rpcx

#### Version 2.8.0

> 1. 新增了gomemcache Touch/Get/Gets方法，去掉了callback修复了超时问题

#### Version 2.7.0

> 1.去掉了marmot的timer

#### Version 2.6.0

> 1.新增rpcx替换标准库net/rpc

#### Version 2.5.3

> 1.修复磁盘满导致goroutine hang的bug  
