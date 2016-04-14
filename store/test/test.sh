#!/bin/bash

# add free volume
curl -d "n=2&bdir=/tmp&idir=/tmp" http://localhost:6063/add_free_volume

# add volume
curl -d "vid=1" http://localhost:6063/add_volume
curl -d "vid=2" http://localhost:6063/add_volume

# uploads
curl -F 'file=@"./3.jpg"' -F 'file=@"./4.jpg"' -F "vid=1" -F "keys=13" -F "keys=14" -F "cookies=13" -F "cookies=14" http://localhost:6062/uploads

# upload
for i in {1..10}; do curl -F 'file=@"./'$i'.jpg"' -F "vid=2" -F "key=$i" -F "cookie=$i" http://localhost:6062/upload; done

# del
curl -d "key=13&vid=1" http://localhost:6062/del

# get
curl "http://localhost:6062/get?key=5&cookie=5&vid=2"
