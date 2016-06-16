# bfsops
The background management interface of BFS.

## 初始化流程(请参照test/ops_initialization.py)：

###step 1
启动store、directory、proxy、pitchfork
store启动后，zookeeper看到/rack 有store节点

###step 2:
调用space()函数，初始化store，调用完成后，磁盘空间会被分配；bfs存储目录看到生成的volume文件。

###step 3:
调用groups()函数，store分组，调用完成后，zookeeper看到/group/ 有组节点

###step 4:
调用volumes()函数， 生效volume，调用完成后，zookeeper看到/volume/有volume节点

### Done