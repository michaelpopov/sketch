#!/bin/bash

TEST="$1"
if [ $TEST == "" ]; then
    echo "Missing command-line argumet with a test name"
    exit 1;
fi

NO_REMOVE="$2"

OUTPUT=$TEST.out
RESULT=$TEST.result

#$PROJECT_HOME/bin-dbg/tester -e $TEST -ns -c config.ini < $TEST > $OUTPUT
$PROJECT_HOME/bin-dbg/tester -e $TEST -s -c config.ini < $TEST > $OUTPUT

DIFF_COUNT=`diff $OUTPUT $RESULT | wc -l`

if [ "$NO_REMOVE" != "norem" ]; then
    rm $OUTPUT
else
    cat $OUTPUT
fi

if [ $DIFF_COUNT != "0" ]; then
    echo $TEST failed
else
    echo $TEST succeeded
fi


