#!/bin/bash
log=/data/logs/keepalived/info.log
gosnowflake_pid=/tmp/gosnowflake.pid
# stop gosnowflake
function stop_gosnowflake {
    if test -f ${gosnowflake_pid}
    then
        kill $(cat ${gosnowflake_pid}) > /dev/null
    fi
}

# main
stop_gosnowflake
echo "[stop]" >> $log
date >> $log
