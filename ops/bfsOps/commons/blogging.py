#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging  
import logging.handlers  

handler = logging.handlers.RotatingFileHandler('bfs.log', maxBytes = 1024*1024, backupCount = 5) 
formatter = logging.Formatter(fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s' )

handler.setFormatter(formatter)
logger = logging.getLogger('bfs')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG) 

