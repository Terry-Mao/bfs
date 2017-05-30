#!/bin/bash
# main
# start gosnowflake
nohup /data/apps/go/bin/gosnowflake -conf=/data/apps/go/bin/gosnowflake.conf -log_dir=/data/logs/gosnowflake/ -v=1 2>&1 > /data/logs/gosnowflake/panic.log &
# wait gosnowflake sanity check
sleep 10s
# update current role to master
echo -n "master" > /etc/keepalived/roles
