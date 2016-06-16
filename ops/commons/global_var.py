#!/usr/bin/env python
# -*- coding: utf-8 -*-


RACK_STORE = {}   #init from zk /rack
GROUP_STORE = {}
MAX_GROUP_ID = 0

STORE_TO_IP = {} # store server_id to ip
IP_TO_STORE = {} # store ip to server_id

STORE_INFO = {}    #init from store /info
VOLUME_KEY = "volume"
FREE_VOLUME_KEY = "free_volume"

STORE_RACK = {}
STORE_VOLUME = {}
STORE_GROUP = {}

MAX_VOLUME_ID = 0