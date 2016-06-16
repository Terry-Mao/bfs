#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import httplib
import urllib
import hashlib

from blogging import logger
import config
from global_var import *

global_store_conn_pool = {}

class HttpConnection:
    DEFAULT_TIMEOUT = 60

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.http_conn = None

    def request(self, method, url, body=None, headers={}):
        '''return [True|False], status, data'''
        try:
            print url,body
            if self.http_conn is None:
                self.http_conn = httplib.HTTPConnection(self.host, self.port, timeout=HttpConnection.DEFAULT_TIMEOUT)

            self.http_conn.request(method, url, body, headers)
            conn_resp = self.http_conn.getresponse()
            status = conn_resp.status
            data = conn_resp.read()
            
            return (True, status, data)
        except Exception as ex:
            self.http_conn = None
            logger.warn('HttpConnection.request, %s %s, except %s', method, url, str(ex))

        return (False, None, None)


def genHttpHeaders():
    return {"Content-Type":"application/x-www-form-urlencoded"}


def genStoreConnKey(store_ip, store_port):
    return hashlib.sha1('%s/%d'%(store_ip, store_port)).hexdigest()


def genStoreConn(store_ip, store_port):
    key = genStoreConnKey(store_ip, store_port)
    if global_store_conn_pool.has_key(key):
        store_conn = global_store_conn_pool[key]
    else:
        store_conn = HttpConnection(host=store_ip, port=store_port)
        global_store_conn_pool[key] = store_conn

    return store_conn


def storeAddFreeVolume(store_ip, base_dir, num_volumes):
    if num_volumes <= 0:
        return None
    print store_ip    
    store_conn = genStoreConn(store_ip, config.store_admin_port)
    url = "/add_free_volume"
    value = {}
    value['n'] = num_volumes
    value['bdir'] = base_dir
    value['idir'] = base_dir
    body = urllib.urlencode(value)

    retcode, status, data = store_conn.request('POST', url, body, headers=genHttpHeaders())
    if retcode and status >= 200 and status < 300:
        return json.loads(data)

    logger.error("storeAddFreeVolume() called failed: store_ip: %s, num_volumes: %d base_dir: %s",
     store_ip, num_volumes, base_dir)
    return None
    

def storeAddVolume(store_ip, volume_id):
    if volume_id <= 0:
        return None

    store_conn = genStoreConn(store_ip, config.store_admin_port)
    url = "/add_volume"
    value = {}
    value['vid'] = volume_id
    body = urllib.urlencode(value)

    retcode, status, data = store_conn.request('POST', url, body, headers=genHttpHeaders())
    if retcode and status >= 200 and status < 300:
        return json.loads(data)

    logger.error("storeAddVolume() called failed: status: %d, store_ip: %s, volume_id: %d", status, store_ip, volume_id)
    return None


def initFromStore(store_ip):
    store_conn = genStoreConn(store_ip, config.store_stat_port)
    url = "/info"

    retcode, status, data = store_conn.request('GET', url)
    if retcode and status >= 200 and status < 300:
        store_data = json.loads(data)
        if store_data['ret'] != 1:
            return False
        
        free_volumes_num = volumes_num = 0
        
        free_volumes = store_data['free_volumes']
        if free_volumes is not None:
            free_volumes_num = len(free_volumes) - 1
        STORE_INFO[FREE_VOLUME_KEY+IP_TO_STORE[store_ip]] = free_volumes_num

        volumes = store_data['volumes']
        if volumes is not None:
            volumes_num = len(volumes)
        STORE_INFO[VOLUME_KEY+IP_TO_STORE[store_ip]] = volumes_num
        return True

    logger.error("initFromStore() called failed: store_ip: %s", store_ip)
    return False


def storeBulkVolume(store_ip, body):
    pass


def storeCompactVolume(store_ip, body):
    pass
