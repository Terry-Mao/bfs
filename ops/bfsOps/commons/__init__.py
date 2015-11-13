#!/usr/bin/env python
# -*- coding: utf-8 -*-

from global_var import *
from blogging import logger


def parseSize(string_size):
	if string_size[-1] == 'G':
		return int(string_size[:-1])
	elif string_size[len(string_size)-2:] == 'GB':
		return int(string_size[:-2])
	elif string_size[-1] == 'T':
		return 1024 * int(string_size[:-1])
	elif string_size[len(string_size)-2:] == 'TB':
		return 1024 * int(string_size[:-2])
	else:
		return None

def grouping_store(ips, copys, rack):
	#group and set to zk
	#if ip has been grouped ,ignore
	#copys  +   rack   +   free_volumes
	#[a[i:i+n] for i in range(0, len(a), n)]   just a temporary plan
	group_stores = [ips[i:i+copys] for i in range(0, len(ips), copys)]
	return group_stores


from flask import Flask
def createApp():
    app = Flask(__name__, template_folder='../templates', static_folder='../static')
    return app

app = createApp()


def initBfsData():
	from zk_client import initFromZk
	if initFromZk():
		logger.info("initFromZk() is called, success")
	else:
		logger.error("initFromZk() is called, failed")
		return False

	from store_client import initFromStore
	for store_ip in IP_TO_STORE:
		if initFromStore(store_ip):
			logger.info("initFromStore() is called, success")
		else:
			logger.info("initFromStore() is called, failed")
			return False

	logger.info("initBfsData() is called, success")
	return True


if not initBfsData():
	logger.error("initBfsData() called, failed   QUIT NOW")
	import sys
	sys.exit()
