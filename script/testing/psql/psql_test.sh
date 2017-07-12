#!/bin/bash

## ====================================================
## Peloton PSQL Test
## ====================================================

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/testing/psql/<this_file>
CODE_SOURCE_DIR=`dirname $0`
OS_NAME=$(uname | tr "[:lower:]" "[:upper:]")
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
    # If this flag is not set to '1', then we know that
    # our diff failed
    if [ "$PASS" != "1" ]; then
        echo "***FAIL***"
        echo "$DIFF_RES"
    fi
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
if [ "$OS_NAME" == "DARWIN" ]; then
    ASAN_OPTIONS=detect_container_overflow=0 bin/peloton -port $PELOTON_PORT > /dev/null &
else
    bin/peloton -port $PELOTON_PORT > /dev/null &
fi
PELOTON_PID=$!
sleep 3

# run psql
echo "\i $TEST_SCRIPT" | psql "$PELOTON_ARGS" -U $PELOTON_USER -h $PELOTON_HOST -p $PELOTON_PORT > $TEMP_OUT_FILE 2>&1

# compute diff against reference output
# diff will return a non-zero status code if the files are different
# This will get trapped by our clean-up function, so we need to set
# a flag to let us know that we ended because of our diff failed
PASS=0
DIFF_RES=$(diff $TEMP_OUT_FILE $REF_OUT_FILE) # 2>&1)
if [[ -z $DIFF_RES ]]; then
    PASS=1
    echo "***PASS***"
    exit 0
fi
# We should never get here, but just in case....
exit -1

