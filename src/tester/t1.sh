#!/bin/bash

function send_cmd() {
    local cmd="$1"
    local output="$2"
    echo "$cmd"

    printf '%s;\n' "$cmd" >&3

    IFS= read -r line <&4
    if [[ "$line" != "0" && -n "$line" ]]; then
        echo "$line"
    fi

    if [ -n "$output" ]; then
        if [ -n "$line" ]; then
            while IFS= read -r line <&4 && [ -n "$line" ]; do
                echo "$line"
            done
        fi 
    fi
}

rm -rf /home/mpopov/data/*
rm inpipe outpipe errpipe > /dev/null 2>&1
mkfifo inpipe outpipe errpipe

debug/client -c config.ini <inpipe >outpipe 2>errpipe &
child_pid=$!

exec 3>inpipe
exec 4<outpipe
exec 5<errpipe

send_cmd 'LIST CATALOGS' 'show'
send_cmd 'CREATE CATALOG one'
send_cmd 'LIST CATALOGS' 'show'
send_cmd 'CREATE CATALOG two'
send_cmd 'LIST CATALOGS' 'show'
send_cmd 'LIST DATASETS one' 'show'
send_cmd 'CREATE DATASET one.aaa TYPE=i16 DIMENSION=64 COUNT=8'
send_cmd 'LIST DATASETS one' 'show'
send_cmd 'CREATE DATASET one.bbb TYPE=i16 DIMENSION=64 COUNT=8'
send_cmd 'LIST DATASETS one' 'show'
send_cmd 'DROP DATASET one.aaa'
send_cmd 'LIST DATASETS one' 'show'
send_cmd 'DROP CATALOG one'
send_cmd 'LIST CATALOGS' 'show'

printf 'quit;\n' >&3
wait "$child_pid"

exec 3>&- 4<&- 5<&-
rm inpipe outpipe errpipe
