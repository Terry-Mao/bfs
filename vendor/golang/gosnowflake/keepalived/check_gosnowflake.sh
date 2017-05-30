#!/bin/bash -x
gosnowflake_pid=/tmp/gosnowflake.pid
keepalived_pid=/tmp/keepalived.pid
role=/etc/keepalived/roles

# kill the keepalived service
function stop_keepalived {
    if test -f ${keepalived_pid}
    then
        kill $(cat ${keepalived_pid}) > /dev/null
    fi
}

# check gosnowflake service alive
function check_run_gosnowflake {
    if test ! -f ${gosnowflake_pid}
    then
        return 1
    fi

    pid=$(cat ${gosnowflake_pid})
    ps -p ${pid} > /dev/null
    if test $? -ne 0
    then
        return 1
    fi

    return 0
}

# get current gosnowflake role
function get_role {
    if test ! -f ${role}
    then
        echo "backup"
    fi

    echo $(cat ${role})
}

# main
cur_role=$(get_role)
case "${cur_role}" in
# if current role is backup then succeed (backup: gosnowflake is sandby, not started)
"backup" )
    exit 0
;;
# if master check the gosnowflake service alive
"master" )
    check_run_gosnowflake    
    if test "$?" -ne 0
    then
        # kill keepalved, let leader selection
        stop_keepalived 
        exit 1
    fi
    exit 0
;;
"*" )
    # unknwon role, kill keepalived
    stop_keepalived 
    exit 1
;;
esac

exit 0
