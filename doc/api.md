#BFS上传服务接入文档
##基本概念
###1.bucket
业务级别的存储桶，由bfs平台分配和管理。实现了业务隔离。也是读写权限控制的基本单位。
###2.object
对象或文件（file），存储的基本单位，业务方进行上传下载的基本单位。
###3.AccessKey
访问授权，包括AccessKeyId和AccessKeySecret。
##接口定义
###1.上传(PUT)
| appkey           | true  |
| sign             | true  |
| ts               | true  |
###2.下载(GET)

###3.删除(DELETE)

###4.删除(HEAD)
