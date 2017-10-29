#!/bin/bash -x

./clear.sh

wget -O- -q "https://github.com/cmu-db/peloton-test/blob/master/deploy/peloton-test.tar?raw=true" | tar x
wget -O- -q http://pelotondb.io/files/data/sqlite-traces.tar.gz | tar xz

python trace-replay.py
