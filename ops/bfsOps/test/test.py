#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
import json

def http_post():
	url = 'http://127.0.0.1:9000/bfsops/initialization'
	#url = 'http://127.0.0.1:9000/groups'
	#url = 'http://127.0.0.1:9000/volumes'
	value = {"ips":"123.56.108.22","dirs":"/tmp","size":"3G"}
	values = []
	values.append(value)
	values_d = {}
	values_d['initlist'] = values

	jdata = json.dumps(values_d)
	req = urllib2.Request(url, jdata, headers = {"Content-type":"application/json"})
	response = urllib2.urlopen(req)
	return response.read()

resp = http_post()

print resp