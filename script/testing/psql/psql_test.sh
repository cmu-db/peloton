#!/bin/bash

## ====================================================
## Peloton PSQL Test
## ====================================================

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/testing/psql/<this_file>
CODE_SOURCE_DIR=`dirname $0`
REF_OUT_FILE="$CODE_SOURCE_DIR/psql_ref_out.txt"
TEMP_OUT_FILE="/tmp/psql_out-$$"

PELOTON_DIR="$CODE_SOURCE_DIR/../../.."
PELOTON_PID=""
PELOTON_PORT="57721"
PELOTON_HOST="localhost"
PELOTON_USER="postgres"
PELOTON_PASS="postgres"
PELOTON_ARGS='sslmode=disable'

TEST_SCRIPT="../script/testing/dml/basic1.sql"

## ---------------------------------------------
## Exit Handler
## ---------------------------------------------
set -e
function cleanup {
    # delete test output file
    if [ -f $TEMP_OUT_FILE ]; then
        rm $TEMP_OUT_FILE
    fi

    # kill peloton
    if [ ! -x "$PELOTON_PID" ]; then
        kill -9 $PELOTON_PID
    fi
}
trap cleanup EXIT

## ---------------------------------------------
## MAIN
## ---------------------------------------------

# assume that build director is called 'build'
cd $PELOTON_DIR/build

# start peloton
bin/peloton -port $PELOTON_PORT > /dev/null &
PELOTON_PID=$!
sleep 3

# run psql
echo "\i $TEST_SCRIPT" | psql "$PELOTON_ARGS" -U $PELOTON_USER -h $PELOTON_HOST -p $PELOTON_PORT > $TEMP_OUT_FILE 2>&1

# compute diff against reference output
diff_res=$(diff $TEMP_OUT_FILE $REF_OUT_FILE 2>&1)
if [[ -z $diff_res ]]; then
    echo "PSQL TEST PASS"
    exit 0
else
    echo "***FAIL***"
    echo "$diff_res"
    exit -1
fi

