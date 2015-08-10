#!/bin/sh

rm -rf data

make -j4
sudo make install
initdb data
cp ../postgresql.conf data

pkill -9 peloton
pg_ctl -D data stop
rm data/pg_log/peloton.log
pg_ctl -D data start
sleep 1
createuser -r -s postgres
echo "create database ycsb;" | psql postgres
echo "create database tpcc;" | psql postgres
