#!/bin/sh

# Tries to follow the steps of README.md closely, which helps to keep that file
# up-to-date.

set -ex

tmpdir=$(mktemp -d)

cat >logcabin-1.conf << EOF
serverId = 1
listenAddresses = 127.0.0.1:5254
storagePath=$tmpdir
EOF

cat >logcabin-2.conf << EOF
serverId = 2
listenAddresses = 127.0.0.1:5255
storagePath=$tmpdir
EOF

cat >logcabin-3.conf << EOF
serverId = 3
listenAddresses = 127.0.0.1:5256
storagePath=$tmpdir
EOF

mkdir -p debug

build/LogCabin --config logcabin-1.conf --bootstrap

build/LogCabin --config logcabin-1.conf --log debug/1 &
pid1=$!

build/LogCabin --config logcabin-2.conf --log debug/2 &
pid2=$!

build/LogCabin --config logcabin-3.conf --log debug/3 &
pid3=$!

ALLSERVERS=127.0.0.1:5254,127.0.0.1:5255,127.0.0.1:5256
build/Examples/Reconfigure --cluster=$ALLSERVERS set 127.0.0.1:5254 127.0.0.1:5255 127.0.0.1:5256

build/Examples/HelloWorld --cluster=$ALLSERVERS

echo -n hello | build/Examples/TreeOps --cluster=$ALLSERVERS write /world
build/Examples/TreeOps --cluster=$ALLSERVERS dump

kill $pid1
kill $pid2
kill $pid3

wait

rm -r $tmpdir
