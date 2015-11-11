#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import httplib
from commons import *
from commons.global_var import *


from flask import request,render_template,jsonify,session,redirect,url_for,abort
#from bfsOps.decorates import login_required

@app.route('/bfsops/initialization', methods = ["POST"])
#@login_required
def bfsopsInitPost():
	if not request.json:
		abort(400)
	inits = request.json['initlist']
	if not isinstance(inits, list):
		abort(400)

	for init_item in inits:
		try:
			ips = list(set(init_item['ips'].split(',')))
			dirs = list(set(init_item['dirs'].split(',')))
			size_G = int(parseSize(init_item['size']))
			if size_G is None:
				logger.error("bfsopsInitPost() called, failed, param error: size: %s", init_item['size'])
				abort(400)
		except BaseException, e:
			logger.warn('Exception:%s', str(e))  # xxx
			abort(400)

		num_volumes = size_G / config.store_block_size
		for store_ip in ips:
			for store_dir in dirs:
				result = store_client.storeAddFreeVolume(store_ip, store_dir, num_volumes)
				if result['ret'] == 1:
					if result['succeed'] >= num_volumes -1:
						logger.info('storeAddFreeVolume() called, success    store_ip: %s,  base_dir: %s', store_ip, store_dir)
					else:
						logger.warn('storeAddFreeVolume() called, success, but not enough space  store_ip: %s,  base_dir: %s',
						 store_ip, store_dir)
						store_info[FREE_VOLUME_KEY+ip_store[store_ip]] += result['succeed']
				else:
					logger.error('storeAddFreeVolume() called, failed    store_ip: %s,  base_dir: %s', store_ip, store_dir)
					return jsonify(status="failed", errorMsg="")

	return jsonify(status="ok", errorMsg="")


@app.route('/bfsops/initialization', methods = ["GET"])
#@login_required
def bfsopsInitGet():
	try:
		initialization_stores = []
		for key in store_rack.keys():
			if key not in store_group:
				if store_info.has_key(FREE_VOLUME_KEY + key):
					initialization_stores.append(store_ip[key])
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
	iplist = request.json['iplist']
	if not isinstance(iplist, list):
		abort(400)

	resp = {}
	resp['status'] = "ok"
	resp['errorMsg'] = ""
	resp['content'] = []
	
	need_break = False
	for ip_item in iplist:
		try:
			ips = list(set(ip_item['ips'].split(',')))
			copys = int(ip_item['copys'])
			rack = int(ip_item['rack'])
			if rack not in [0, 2, 3] or copys not in [2, 3] or len(ips) % copys != 0:
				logger.error("bfsopsGroupsPost() called, failed, param error:  ips_length: %d copys: %d, rack: %d", len(ips), copys, rack)
				abort(400)
			for store_ip in ips:
				if ip_store[store_ip] in store_group:
					logger.error('grouping_store() called, failed   store_ip: %s has been grouped', store_ip)
					abort(400)
		except BaseException, e:
			logger.warn('Exception:%s', str(e))
			abort(400)

        #机器分组
		group_stores = grouping_store(ips, copys, rack)
		if group_stores is None:
			logger.error("grouping_store() called, failed  errorMsg: ")
			abort(400)

		global max_group_id
		for group_item in group_stores:
			group_id = max_group_id + 1
			group_store[group_id] = []
			for store_ip in group_item:
				if not zk_client.addGroupStore(group_id, ip_store(store_ip)):
					logger.error("addGroupStore() called, failed  store_ip: %s, group_id: %d", store_ip, group_id)
					need_break = True
					break

				store_group[ip_store(store_ip)] = group_id
				group_store[group_id].append(ip_store[store_ip])

			if need_break:
				resp['status'] = "failed"
				break
			max_group_id += 1
			logger.info("addGroupStore() called, success  group_id: %d",group_id)

			grouping_result['groupid'] = group_id
			grouping_result['ips'] = ','.join(group_item)
			resp['content'].append(grouping_result)

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

		group_item = {}
		status = 0

		for group_id in group_store:
			stores = group_store[group_id]
			for store_id in stores:
				if store_info[FREE_VOLUME_KEY+store_id] == 0:
					status = 1
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
	groups = list(set(request.json['groups']))
	if not isinstance(iplist, list):
		abort(400)

	resp = {}
	resp['status'] = "ok"
	resp['errorMsg'] = ""
	
	need_break = False
	global max_volume_id
	for group_id in groups:
		stores = group_store[group_id]
		min_free_volume_id = 0
		for store_id in stores:
			if min_free_volume_id == 0 or min_free_volume_id > store_info[FREE_VOLUME_KEY+store_id]:
				min_free_volume_id = store_info[FREE_VOLUME_KEY+store_id]
		for i in range(min_free_volume_id):
			volume_id = max_volume_id+1
			store_volume[store_id] = []
			for store_id in stores:
				if not store_client.storeAddVolume(store_ip[store_id], volume_id):
					logger.error("storeAddVolume() called, failed, store_ip: %s, volume_id: %d", store_ip[store_id], volume_id)
					need_break = True
					break
				if not zk_client.addVolumeStore(volume_id, store_id):
					logger.error("addVolumeStore() called, failed, store_ip: %s, volume_id: %d", store_ip[store_id], volume_id)
					need_break = True
					break
				store_volume[store_id].append(volume_id)
				store_info[VOLUME_KEY+store_id] += 1

			if need_break:
				break
			max_volume_id += 1
			logger.info("storeAddVolume() called, success, volume_id: %d", volume_id)

		if need_break:
			resp['status'] = "failed"
			break
		logger.info("storeAddVolume() called, success, group_id: %d", group_id)

	resp_str = json.dumps(resp)
	logger.info("bfsopsVolumesPost() called, success, resp: %s", resp_str)
	return resp_str

@app.route('/bfsops/volumes', methods = ["GET"])
#@login_required
def bfsopsVolumesGet():
	pass
