#!/bin/bash

curl -F 'file=@"./3.jpg"' -F 'file=@"./4.jpg"' -F "vid=2" -F "keys=13,14" -F "cookies=13,14" http://localhost:6062/uploads
