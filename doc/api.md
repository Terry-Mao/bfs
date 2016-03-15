#BFS上传服务接入文档
##基本概念
###1.bucket
业务级别的存储桶，由bfs平台分配和管理。实现了业务隔离，也是读写权限控制的基本单位。
###2.object
对象或文件（file），存储的基本单位，业务方进行上传下载的基本单位。
###3.AccessKey
访问授权，包括AccessKeyId和AccessKeySecret。
##接口定义
###1.上传
|    |    |
| :-----           | :---  |
| 接口功能           | 上传  |
| 请求模块           | bfs-proxy  |
| 请求方法           | PUT  |
| 请求路径           | /${bucketname}/${filename}  |
| 请求HOST           | $host  |
| 请求参数           | 无  |
| 请求头             | PUT /${bucketname}/${filename} HTTP/1.1<br>Host: $host<br> Date: ${GMT date}<br> Authorization:accesskey+':'+urlsafe_b64encode(hmac-sha1(accessSecret, 'request.method\nbucketname\nfilename\nexpire\n'))+':'+expire<br> Content-Type: filetype |
| 备注               | expire为时间戳；filename可为空，但不能带"/"，且不支持中文  |
| 请求内容           | data bytes  |
| 响应码             | 200  |
| 响应头             | Code: 200<br> ETag:xxxxxxxxxx(sha1sum值)<br> Location: ${location}<br> |
| 响应内容           | 无  |
| 示例               | PUT /live/my-image.jpg HTTP/1.1<br> Host: $host<br> Date: ${GMT date}<br> Authorization:ak_live:NsPFsxwMyYwLX4cXKnN1cD_34sg=:1387948120<br> Content-Type: image/jpg<br>  |
###2.下载
|    |    |
| :-----           | :---  |
| 接口功能           | 下载  |
| 请求模块           | bfs-proxy  |
| 请求方法           | GET  |
| 请求路径           | /bfs/${bucketname}/${filename}  |
| 请求HOST           | $host  |
| 请求参数           | token (如不放参数，放请求头Authorization)  |
| 请求头             | GET /bfs/${bucketname}/${filename} HTTP/1.1<br>Host: $host<br> Date: ${GMT date}<br> Authorization:accesskey+':'+urlsafe_b64encode(hmac-sha1(accessSecret, 'request.method\nbucketname\nfilename\nexpire\n'))+':'+expire |
| 备注               | expire为时间戳  |
| 请求内容           | 无  |
| 响应码             | 200  |
| 响应头             | Server: Bfs |
| 响应内容           | data bytes  |
| 示例               | GET /bfs/live/my-image.jpg HTTP/1.1<br> Host: $host<br> Date: ${GMT date}<br>  |
###3.删除
|    |    |
| :-----           | :---  |
| 接口功能           | 删除  |
| 请求模块           | bfs-proxy  |
| 请求方法           | DELETE  |
| 请求路径           | /${bucketname}/${filename}  |
| 请求HOST           | $host  |
| 请求参数           | 无  |
| 请求头             | DELETE /${bucketname}/${filename} HTTP/1.1<br>Host: $host<br> Date: ${GMT date}<br> Authorization:accesskey+':'+urlsafe_b64encode(hmac-sha1(accessSecret, 'request.method\nbucketname\nfilename\nexpire\n'))+':'+expire|
| 备注               | expire为时间戳；filename可为空，但不能带"/"，且不支持中文  |
| 请求内容           | 无  |
| 响应码             | 200  |
| 响应头             | Server: Bfs  |
| 响应内容           | 无  |
| 示例               | DELETE /live/my-image.jpg HTTP/1.1<br> Host: $host<br> Date: ${GMT date}<br> Authorization:ak_live:NsPFsxwMyYwLX4cXKnN1cD_34sg=:1387948120<br>  |
###4.HEAD
|    |    |
| :-----           | :---  |
| 接口功能           | 下载  |
| 请求模块           | bfs-proxy  |
| 请求方法           | HEAD  |
| 请求路径           | /bfs/${bucketname}/${filename}  |
| 请求HOST           | $host  |
| 请求参数           | token (如不放参数，放请求头Authorization)  |
| 请求头             | HEAD /bfs/${bucketname}/${filename} HTTP/1.1<br>Host: $host<br> Date: ${GMT date}<br> Authorization:accesskey+':'+urlsafe_b64encode(hmac-sha1(accessSecret, 'request.method\nbucketname\nfilename\nexpire\n'))+':'+expire |
| 备注               | expire为时间戳  |
| 请求内容           | 无  |
| 响应码             | 200  |
| 响应头             | Server: Bfs |
| 响应内容           | 无  |
| 示例               | HEAD /bfs/live/my-image.jpg HTTP/1.1<br> Host: $host<br> Date: ${GMT date}<br>  |
