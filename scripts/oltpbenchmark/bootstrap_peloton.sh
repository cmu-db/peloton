#!/bin/sh

# Clean up any existing peloton data directory
rm -rf data

# Rebuild and install
make -j4
sudo make install

# Setup new peloton data directory
initdb data

# Copy over the peloton configuration file into the directory
cp ../scripts/oltpbenchmark/postgresql.conf data

# Kill any existing peloton processes
pkill -9 peloton
pg_ctl -D data stop

# Clean up any existing peloton log
rm data/pg_log/peloton.log

# Start the peloton server
pg_ctl -D data start

# Wait for a moment for the server to start up...
sleep 1

# Create a "postgres" user
createuser -r -s postgres

# Create a default database for psql
createdb $USER

# Create YCSB and TPC-C databases

echo "create database ycsb;" | psql postgres
echo "create database tpcc;" | psql postgres
