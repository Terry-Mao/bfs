#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import httplib
from commons import *
from commons.global_var import *

from flask import request,render_template,jsonify,session,redirect,url_for,abort


@app.route('/bfsops/initialization', methods = ["POST"])
#@login_required
def bfsopsInitPost():
	if not request.json:
		abort(400)

	try:
		ips = list(set(request.json['ips'].split(',')))
		dirs = list(set(request.json['dirs'].split(',')))
		size_G = int(parseSize(request.json['size']))
	except BaseException, e:
		logger.warn('Exception:%s', str(e))  # xxx
		abort(400)

	try:
		num_volumes = size_G / config.store_block_size
		for store_ip_u in ips:
			store_ip = store_ip_u.encode('utf-8')
			for store_dir in dirs:
				result = store_client.storeAddFreeVolume(store_ip, store_dir, num_volumes)
				if result is None:
					logger.error("storeAddFreeVolume() called, failed store_ip:%s, store_dir:%s", store_ip, store_dir)
					abort(500)
				if result['ret'] == 1:
					if result['succeed'] >= num_volumes -1:
						logger.info('storeAddFreeVolume() called, success    store_ip: %s,  base_dir: %s', store_ip, store_dir)
					else:
						logger.warn('storeAddFreeVolume() called, success, but not enough space  store_ip: %s,  base_dir: %s',
						 store_ip, store_dir)
					STORE_INFO[FREE_VOLUME_KEY+IP_TO_STORE[store_ip]] += result['succeed']
				else:
					logger.error('storeAddFreeVolume() called, failed    store_ip: %s,  base_dir: %s', store_ip, store_dir)
					return jsonify(status="failed", errorMsg="")
	except BaseException, e:
		logger.error('Exception:%s', str(e))
		abort(500)
	return jsonify(status="ok", errorMsg="")


@app.route('/bfsops/initialization', methods = ["GET"])
#@login_required
def bfsopsInitGet():
	try:
		initialization_stores = []
		for key in STORE_RACK.keys():
			if key not in STORE_GROUP:
				if STORE_INFO.has_key(FREE_VOLUME_KEY + key):
					initialization_stores.append(STORE_TO_IP[key])
		resp = {}
		resp_item = {}
		resp['status'] = "ok"
		resp_item['ips'] = ",".join(initialization_stores)
		resp['content'] = resp_item
		resp['errorMsg'] = ""

		resp_str = json.dumps(resp)
		logger.info("bfsopsInitGet() called, success, initialization: %s", resp_str)
		return resp_str
	except BaseException, e:
		logger.warn('Exception:%s', str(e))
		abort(500)


@app.route('/bfsops/groups', methods = ["POST"])
#@login_required
def bfsopsGroupsPost():
	if not request.json:
		abort(400)

	resp = {}
	resp['status'] = "ok"
	resp['errorMsg'] = ""
	resp['content'] = []
	
	need_break = False
	try:
		ips = list(set(request.json['ips'].split(',')))
		copys = int(request.json['copys'])
		rack = int(request.json['rack'])
		if rack not in [1, 2, 3] or copys not in [2, 3] or len(ips) % copys != 0:
			logger.error("bfsopsGroupsPost() called, failed, param error:  ips_length: %d copys: %d, rack: %d", len(ips), copys, rack)
			abort(400)
		for store_ip_u in ips:
			store_ip = store_ip_u.encode('utf-8')
			if IP_TO_STORE.has_key(store_ip) and IP_TO_STORE[store_ip] in STORE_GROUP:
				logger.error('grouping_store() called, failed   store_ip: %s  not exist or has been grouped', store_ip)
				abort(400)
	except BaseException, e:
		logger.warn('Exception:%s', str(e))
		abort(400)

    #机器分组
	grouping_store_result = grouping_store(ips, copys, rack)
	if grouping_store_result is None:
		logger.error("grouping_store() called, failed  errorMsg: ")
		abort(400)

	global MAX_GROUP_ID
	for group_item in grouping_store_result:
		group_id = MAX_GROUP_ID + 1
		GROUP_STORE[group_id] = []
		for store_ip in group_item:
			if not zk_client.addGroupStore(group_id, IP_TO_STORE[store_ip]):
				logger.error("addGroupStore() called, failed  store_ip: %s, group_id: %d", store_ip, group_id)
				need_break = True
				break

			STORE_GROUP[IP_TO_STORE[store_ip]] = group_id
			GROUP_STORE[group_id].append(IP_TO_STORE[store_ip])

		if need_break:
			resp['status'] = "failed"
			break
		MAX_GROUP_ID += 1
		logger.info("addGroupStore() called, success  group_id: %d",group_id)

		groups_result = {}
		groups_result['groupid'] = group_id
		groups_result['ips'] = ','.join(group_item)
		resp['content'].append(groups_result)

	resp_str = json.dumps(resp)
	logger.info("bfsopsGroupsPost() called, success, groups: %s", resp_str)
	return resp_str


@app.route('/bfsops/groups', methods = ["GET"])
#@login_required
def bfsopsGroupsGet():
	try:
		resp = {}
		resp['status'] = "ok"
		resp['errorMsg'] = ""
		resp['content'] = []

		status = 0

		for group_id in GROUP_STORE:
			stores = GROUP_STORE[group_id]
			for store_id in stores:
				if STORE_INFO[FREE_VOLUME_KEY+store_id] == 0:
					status = 1
			group_item = {}
			group_item['groupid'] = group_id
			group_item['ips'] = ','.join(stores)
			group_item['status'] = status
			resp['content'].append(group_item)

		resp_str = json.dumps(resp)
		logger.info("bfsopsGroupsGet() called, success, groups: %s", resp_str)
		return resp_str
	except BaseException, e:
		logger.warn('Exception:%s', str(e))
		abort(400)


@app.route('/bfsops/volumes', methods = ["POST"])
#@login_required
def bfsopsVolumesPost():
	if not request.json:
		abort(400)
	groups = list(set(request.json['groups'].split(',')))
	for group_id in groups:
		if not GROUP_STORE.has_key(group_id.encode('utf-8')):
			abort(400)

	resp = {}
	resp['status'] = "ok"
	resp['errorMsg'] = ""
	
	need_break = False
        global MAX_VOLUME_ID
	for group_id_u in groups:
		group_id = group_id_u.encode('utf-8')
		stores = GROUP_STORE[group_id]
		min_free_volume_id = 0
		for store_id in stores:
			if min_free_volume_id == 0 or min_free_volume_id > STORE_INFO[FREE_VOLUME_KEY+store_id]:
				min_free_volume_id = STORE_INFO[FREE_VOLUME_KEY+store_id]
		for i in range(min_free_volume_id-1):
			volume_id = MAX_VOLUME_ID + 1
			for store_id in stores:
				if not store_client.storeAddVolume(STORE_TO_IP[store_id], volume_id):
					logger.error("storeAddVolume() called, failed, store_ip: %s, volume_id: %d", STORE_TO_IP[store_id], volume_id)
					need_break = True
					break
				if not zk_client.addVolumeStore(volume_id, store_id):
					logger.error("addVolumeStore() called, failed, store_ip: %s, volume_id: %d", STORE_TO_IP[store_id], volume_id)
					need_break = True
					break
				if not STORE_VOLUME.has_key(store_id):
					STORE_VOLUME[store_id] = []
				STORE_VOLUME[store_id].append(volume_id)
				STORE_INFO[VOLUME_KEY+store_id] += 1

			if need_break:
				break
			MAX_VOLUME_ID += 1
			logger.info("storeAddVolume() called, success, volume_id: %d", volume_id)

		if need_break:
			resp['status'] = "failed"
			break
		logger.info("storeAddVolume() called, success, group_id: %d", int(group_id))

	resp_str = json.dumps(resp)
	logger.info("bfsopsVolumesPost() called, success, resp: %s", resp_str)
	return resp_str
