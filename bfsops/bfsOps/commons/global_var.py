#!/usr/bin/env python
# -*- coding: utf-8 -*-


rack_store = {}   #init from zk /rack
group_store = {}
max_group_id = 0

store_ip = {} # store server_id to ip
ip_store = {}

store_info = {}    #init from store /info
VOLUME_KEY = "volume"
FREE_VOLUME_KEY = "free_volume"

store_rack = {}
store_volume = {}
store_group = {}

max_volume_id = 0