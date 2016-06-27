#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import config
from global_var import *
from blogging import logger
import config

from kazoo.client import KazooClient
from kazoo.protocol.paths import join
#from kazoo.exceptions import (KazooException, NoNodeException)

zk_client = KazooClient(hosts=config.zk_hosts)
zk_client.start()
#zk_client.add_auth("digest", "test:test")


def getRack():
	try:
		def watcher(event):
			logger.info("/rack children changed, need update memory")
			getRack()
		zk_client.get('/rack', watcher)

		children = zk_client.get_children('/rack')
		for child in children:
			rack_name = child.encode('utf-8')
			RACK_STORE[rack_name] = []
			path1 = join('/rack', rack_name)
			children1 = zk_client.get_children(path1)
			for child1 in children1:
				store_id = child1.encode('utf-8')
				RACK_STORE[rack_name].append(store_id)
				path2 = join(path1, store_id)
				data, stat = zk_client.get(path2)
				if data:
					parsed_data = json.loads(data)
					ip = parsed_data['stat'].split(':')[0].encode('utf-8')
					STORE_TO_IP[store_id] = ip
					IP_TO_STORE[ip] = store_id
					STORE_RACK[store_id] = rack_name
					STORE_INFO[FREE_VOLUME_KEY+store_id] = -1
					STORE_INFO[VOLUME_KEY+store_id] = 0
				else:
					logger.warn("getRack() called   zk data is None  path: %s", path2)
					return False
		return True
	except Exception as ex:
		logger.error("getRack() called   error: %s", str(ex))
		return False


def addVolumeStore(volume_id, store_id):
	try:
		if zk_client.exists('/volume') is None:
			zk_client.create('/volume')
		path = '/volume/' + str(volume_id)
		if zk_client.exists(path) is None:
			zk_client.create(path)
		path1 = path + '/' + str(store_id)
		if zk_client.exists(path1) is None:
			zk_client.create(path1)
		return True
	except Exception as ex:
		logger.error("addVolumeStore() called   error: %s", str(ex))
		return False


def getAllVolume():
	global MAX_VOLUME_ID
	try:
		if zk_client.exists('/volume') is None:
			return True
		children = zk_client.get_children('/volume')
		for child in children:
			volume_id = child
			if int(volume_id) > MAX_VOLUME_ID:
				MAX_VOLUME_ID = int(volume_id)
			path1 = join('/volume', volume_id)
			children1 = zk_client.get_children(path1)
			for child1 in children1:
				store_id = child1
				if not STORE_VOLUME.has_key(store_id):
					STORE_VOLUME[store_id] = []
				STORE_VOLUME[store_id].append(volume_id)
                        print "max:",MAX_VOLUME_ID
                        logger.error("你好")
		return True
	except Exception as ex:
		logger.error("getAllVolume() called   error: %s", str(ex))
		return False


def addGroupStore(group_id, store_id):
	try:
		if zk_client.exists('/group') is None:
			zk_client.create('/group')
		path = '/group/' + str(group_id)
		if zk_client.exists(path) is None:
			zk_client.create(path)
		path1 = path + '/' + str(store_id)
		if zk_client.exists(path1) is None:
			zk_client.create(path1)
		return True
	except Exception as ex:
		logger.error("addGroupStore() called   error: %s", str(ex))
		return False


def getAllGroup():
	global MAX_GROUP_ID
	try:
		if zk_client.exists('/group') is None:
			return True
		children = zk_client.get_children('/group')
		for child in children:
			group_id = child.encode('utf-8')
			if int(group_id) > MAX_GROUP_ID:
				MAX_GROUP_ID = int(group_id)
			path1 = join('/group', group_id)
			children1 = zk_client.get_children(path1)
			for child1 in children1:
				store_id = child1.encode('utf-8')
				STORE_GROUP[store_id] = group_id
				if not GROUP_STORE.has_key(group_id):
					GROUP_STORE[group_id] = []
				GROUP_STORE[group_id].append(store_id)
                print GROUP_STORE
		return True
	except Exception as ex:
		logger.error("getAllGroup() called   error: %s", str(ex))	
		return False


def initFromZk():
	if getRack():
		logger.info("getRack() called success")
	else:
		logger.error("getRack() called failed, need check now")
		return False

	if getAllVolume():
		logger.info("getAllVolume() called success")
	else:
		logger.info("getAllVolume() called failed, need check now")
		return False

	if getAllGroup():
		logger.info("getAllGroup() called success")
	else:
		logger.info("getAllGroup() called failed, need check now")
		return False

	return True

