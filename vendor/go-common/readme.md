### Go-Common

### Version 6.12.0

> 1.stat支持prometheus功能，实现统计和监控  

### Version 6.11.0
> 1. 对reids进行了修改，以后不依赖conf包了，配置直接写在redis本包
 
### Version 6.10.0
> 1. 增加rpc sharding

### Version 6.9.0

> 1.配置中心client 启动参数增加token字段，区分应用和环境

### Version 6.8.0

> 1.依赖zookeeper的rpc client由连接池改为单连接  
> 2.breaker新增了callback，通知状态变更

### Version 6.7.2
> 1.修改zookeeper注册参数  

### Version 6.7.1

> 1.配置中心增加获得配置文件路径方法

### Version 6.7.0

1.fix rpc权重为0时，client不创建长连接  
2.rpc增加配置是否注册zookeeper  

### Version 6.6.4

> 1.fix mc expire max ttl

### Version 6.6.3

> 1. 将配置中心启动参数设置成和disconf的一样

### Version 6.6.2

> 1. 优化了net/http Client的buffer过小导致的syscall过多

### Version 6.6.1

> 1.fix http client超时设置不准确的问题，去掉了读包体和反序列化的时间  

### Version 6.6.0
> 1.rpc Broadcast 添加reply参数,支持对任意方法进行广播  

### Version 6.5.2

> 1.fix 新版配置中心和老版本init冲突问题

### Version 6.5.1

> 1.fix rpc Boardcast的bug  

### Version 6.5.0
> 1. 新版本配置中心conf/Client  

### Version 6.4.1
> 1. 修复remoteip获取  

### Version 6.4.0
> 1. 去除rpcx  

### Version 6.3.1

> 1.fix配置文件名覆盖的问题  

### Version 6.3.0
> 1. net/rpc支持了Boardcast广播调用

### Version 6.2.5
> 1. net/rpc支持了group路由策略

### Version 6.2.4
> 1. 优化了statsd批量发包

### Version 6.2.3
> 1. 修复了trace comment 在annocation的bug

### Version 6.2.2
> 1. 优化了net/rpc反射带来的性能问题
> 2. net/rpc内置了ping

### Version 6.2.1
> 1. 临时加回net/rpcx, TODO remove
> 2. net/trace.Trace2 奔溃和race修复

### Version 6.2.0
> 1. 去除了net/rpcx

### Version 6.1.3
> 1. 新增了memcache Get2/Gets

### Version 6.1.2
> 1. net/rpc使用CPU个数建立连接

### Version 6.1.1
> 1. 兼容net/rpc server的Client trace传递

### Version 6.1.0
> 1. 升级databus sdk，注意配置文件有变更

#### Version 6.0.0

> 1. xtime->time, xlog->log perf->net/http/perf
> 2. rpc支持设置方法级别超时
> 3. rpc支持breaker熔断
> 4. database 修复Row和标准库不兼容，使用database Rows替换标准库的Rows使用
> 5. 新的rpc框架net/rpc
> 6. net/trace支持Family初始化

#### Version 5.2.2

> 1.Zone结构体加json tag  

#### Version 5.2.0

> 1.更改http包名和路径  
> 2.增加http单元测试  
> 3.statd去掉hostname  
> 4.ip结构体增加isp字段  

#### Version 5.1.2

> 1.xip改为支持对象访问，去掉全局对象和函数  

#### Version 5.1.1

> 1.修复上报trace的位置  

#### Version 5.1.0

> 1.支持熔断  
> 2.rpc server判断zk是否注册  
> 3.修复Infoc连接重连  
> 4.xhttp xrpc xweb改为httpx rpcx webx  
> 5.修复trace level的bug  

#### Version 5.0.0

> 0.注意一定要使用Go1.7及以上版本  
> 1.用golang/rpcx替换官方库  
> 2.使用go1.7的context包  
> 3.增加traceon业务监控上报  
> 4.xhttp中ip方法挪到xip包  
> 5.rpc服务暴露close接口  
> 6.修复ugc配置中心等待30s的bug  
> 7.修复rpc client因权重变更导致panic的bug  
> 8.使用context.WithTimeout替代timer  

#### Version 4.4.1

> 1.日志新增按文件大小rotate  

#### Version 4.4.0

> 1.infoc支持udp和tcp方式  
> 2.去掉stdout、stderr输出到syslog的逻辑  

#### Version 4.3.2

> 1.fix rpc timeout连接泄露的bug  
> 2.rpc单连接改为多连接  

#### Version 4.3.1

> 1.支持从环境变量获取配置  
> 2.syslog支持打印标准输出和错误  

#### Version 4.3.0

> 1.支持配置中心  

#### Version 4.2.0

> 1.修复xredis keys的bug  
> 2.修复xmemcache批量删除bug  
> 3.新增 databus v2 客户端   

#### Version 4.1.3

> 1.trace 优化  
> 2.去掉sp 运营商字段  

#### Version 4.1.2

> 1.trace id改为int64  
> 2.trace http client增加host  
> 3.ip新增运营商字段  

#### Version 4.1.1

> 1.fix kafka monitor  

#### Version 4.1.0

> 1.去掉ecode和router  

### Version 4.0.0

> 1.business移到go-business  
> 2.新增InternalIp()获取本机ip  
> 3.rpc ping加超时  
> 4.增加ecode配置  
> 5.新增支持syslog  

#### Version 3.6.6

> 1.修复xip边界值时死循环问题  

#### Version 3.6.5

> 1.space接口只保留s_img、l_img  
> 2.archive-service新增viewPage的rpc方法  

#### Version 3.6.4

> 1.VIP相关接口及错误码  

### Version 3.6.3

> 1.修复ip递归查找导致的栈溢出  

#### Version 3.6.2

> 1.account-service profile的http接口、批量获取relation接口  
> 2.账号新增official_verify字段  

#### Version 3.6.1

> 1.修复degrade中变量名错误  
> 2.简化redis的auth逻辑，使用option  
