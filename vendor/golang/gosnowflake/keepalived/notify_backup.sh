#!/bin/bash
gosnowflake_pid=/tmp/gosnowflake.pid
# stop gosnowflake
function stop_gosnowflake {
    if test -f ${gosnowflake_pid}
    then
        kill $(cat ${gosnowflake_pid}) > /dev/null
    fi
}

# main
# if current role is backup, make sure gosnowflake is not started.
stop_gosnowflake
# update current role to backup
echo -n "backup" > /etc/keepalived/roles
