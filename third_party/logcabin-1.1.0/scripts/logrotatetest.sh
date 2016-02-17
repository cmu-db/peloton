#!/bin/sh

# Test to make sure the LogCabin daemon rotates its logs when asked to.

set -ex

tmpdir=$(mktemp -d)

cat >logcabin-1.conf << EOF
serverId = 1
listenAddresses = 127.0.0.1:5254
storagePath=$tmpdir
EOF

mkdir -p debug

rm -rf debug/1
rm -rf debug/2

build/LogCabin --config logcabin-1.conf --bootstrap --log debug/1

rm -rf debug/1
rm -rf debug/2

build/LogCabin --config logcabin-1.conf --log debug/1 &
pid=$!

sleep 1

mv debug/1 debug/2

kill -USR2 $pid

sleep 1

kill $pid
wait

rm -r $tmpdir

grep 'Using config file' debug/2 || exit 1
grep ': rotating logs' debug/2 || exit 1
! grep ': done rotating logs' debug/2 || exit 1
! grep 'Exiting' debug/2 || exit 1

! grep 'Using config file' debug/1 || exit 1
! grep ': rotating logs' debug/1 || exit 1
grep ': done rotating logs' debug/1 || exit 1
grep 'Exiting' debug/1 || exit 1
