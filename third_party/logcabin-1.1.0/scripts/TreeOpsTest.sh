#!/bin/sh

set -ex

# Basic test against Examples/TreeOps
# Usage: TreeOpsTest.sh /path/to/build/Examples/TreeOps --cluster 127.0.0.1

if [ $# -gt 0 ]; then
    CMD="$* --quiet"
else
    CMD="logcabin --quiet"
fi

$CMD rmdir /

# commands

echo "mkdir"
$CMD mkdir /hello/bash

echo "write"
echo -n yo | $CMD write /hello/bash/world
echo -n rawr | $CMD write /hello/trex

echo "list"
out=$($CMD list /hello)
test "$out" = "bash/
trex"

echo "dump"
out=$($CMD dump)
test "$out" = "/
/hello/
/hello/bash/
/hello/bash/world: 
    yo
/hello/trex: 
    rawr"
out=$($CMD dump /hello/bash)
test "$out" = "/hello/bash/
/hello/bash/world: 
    yo"

echo "read"
out=$($CMD read /hello/bash/world)
test "$out" = "yo"

echo "remove"
$CMD remove /hello/bash/world
out=$($CMD list /hello/bash)
test "$out" = ""

echo "rmdir"
$CMD rmdir /
out=$($CMD list /)
test "$out" = ""


# options

echo "--dir"
$CMD mkdir /hello/bash
out=$($CMD --dir /hello dump .)
test "$out" = "./
./bash/"


echo "--condition"
! $CMD --condition /x:3 read /x 2>/dev/null # expect failure
echo -n 3 | $CMD write /x
out=$($CMD --condition /x:3 read /x)
test "$out" = "3"

# TODO(ongaro): test --timeout somehow
