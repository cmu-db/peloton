#!/bin/bash

bash clear.sh

git clone https://github.com/lixupeng/peloton-test.git

wget -O- -q http://pelotondb.io/files/data/sqlite-traces.tar.gz | tar xzv

python test.py
