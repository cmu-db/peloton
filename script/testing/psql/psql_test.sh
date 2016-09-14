# This script tests if psql is working correctly.
#!/bin/bash

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/testing/psql/<this_file>
CODE_SOURCE_DIR=`dirname $0`
PELOTON_DIR="$CODE_SOURCE_DIR/../../.."
REF_OUT_FILE="$CODE_SOURCE_DIR/psql_ref_out.txt"
TEMP_OUT_FILE="/tmp/psql_out"


# assume that build director is called 'build'
cd $PELOTON_DIR/build

# start peloton
bin/peloton > /dev/null &
PELOTON_PID=$!
sleep 3

# run psql
echo "\i ../script/testing/dml/basic1.sql" | psql "sslmode=disable" -U postgres -h 0.0.0.0 -p 5432 > $TEMP_OUT_FILE 2>&1

# compute diff against reference output
diff_res=$(diff $TEMP_OUT_FILE $REF_OUT_FILE 2>&1)
if [[ -z $diff_res ]]; then
    echo "PSQL TEST PASS"
    exit 0
else
    echo "$diff_res"
    exit -1
fi

# delete test output file
rm $TEMP_OUT_FILE

# kill peloton
kill -9 $PELOTON_PID
