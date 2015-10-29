#!/bin/bash

for i in {1..10}; do curl -F 'file=@"./'$i'.jpg"' -F "vid=2" -F "key=$i" -F "cookie=$i" http://localhost:6062/upload; done
