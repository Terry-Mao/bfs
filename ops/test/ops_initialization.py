#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
import json

# initialize bfs space
def space():
	url = 'http://127.0.0.1:9000/bfsops/initialization'
	# ips 机器的ip列表
	# dirs 磁盘目录
	# size 每块磁盘的空间大小
	value = {"ips":"xx.xx.xx.xx, yy.yy.yy.yy","dirs":"/data1/bfsdata/,/data2/bfsdata/","size":"10T"}

	jdata = json.dumps(value)
	req = urllib2.Request(url, jdata, headers = {"Content-type":"application/json"})
	response = urllib2.urlopen(req)
	return response.read()
# initialize groups
def groups():
	url = 'http://127.0.0.1:9000/bfsops/groups'
	# ips 要分组的ip列表
	# copys 副本数（包括本身）
	# racks 跨机架 默认1即可，具体请参考ops代码
	value = {"ips":"xx.xx.xx.xx, yy.yy.yy.yy", "copys":2, "rack":1}

	jdata = json.dumps(value)
	req = urllib2.Request(url, jdata, headers = {"Content-type":"application/json"})
	response = urllib2.urlopen(req)
	return response.read()

# initialize volumes
def volumes():
	url = 'http://127.0.0.1:9000/bfsops/volumes'
	# groups 生效某个group
	value = {"groups":"2"}

	jdata = json.dumps(value)
	req = urllib2.Request(url, jdata, headers = {"Content-type":"application/json"})
	response = urllib2.urlopen(req)
	return response.read()

# store启动后，zookeeper看到/rack 有store节点

# 初始化流程：
# 分别依次调用3个函数，分别完成：初始化store；store分组；生效volume.
#step 1:
#space()
#调用完成后，磁盘空间会被分配；bfs存储目录看到生成的volume文件。

#step 2:
#groups()
#调用完成后，zookeeper看到/group/ 有组节点

#step 3:
#volumes()
#调用完成后，zookeeper看到/volume/有volume节点


