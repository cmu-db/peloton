#!/bin/sh

###############################################
# SETUP PELOTON
###############################################

# TODO: This is from the old system when we were based on Postgres
# TODO: This needs to be re-written for our new configuration

# Clean up any existing peloton test database directory
# rm -rf peloton_test_database
# 
# # Setup new peloton data directory
# initdb peloton_test_database
# 
# # Copy over the peloton configuration file into the directory
# cp ./scripts/oltpbenchmark/postgresql.conf data
# 
# # Stop the peloton server
# pg_ctl -D peloton_test_database stop
# 
# # Clean up any existing peloton log
# rm peloton_test_database/pg_log/peloton.log
# 
# # Start the peloton server
# pg_ctl -D peloton_test_database start
# 
# # Wait for a moment for the server to start up...
# sleep 1
# 
# # Create a "postgres" user
# createuser -r -s postgres
# 
# # Create a default database for psql
# createdb $USER
# 
# # Create YCSB and TPC-C databases
# 
# echo "create database ycsb;" | psql postgres
# echo "create database tpcc;" | psql postgres

###############################################
# SETUP OLTPBENCH
###############################################

# Clean up any oltpbench git repository
rm -rf oltpbench

# Clone Git Repository
git clone https://github.com/oltpbenchmark/oltpbench.git

# Enter the directory
cd oltpbench

# Compile OLTP-Benchmark using the provided Ant script
ant

# Run the YCSB benchmark on Peloton
./oltpbenchmark -b ycsb -c config/jenkins_ycsb_config.xml --create=true --load=true --execute=true -s 5 -o outputfile

# check the outputfile
min_oltpbench_output=0
oltpbench_output=`sed -n '6p' outputfile.summary`;

# check if the throughput is non-zero
if [ 1 -eq "$(echo "${oltpbench_output} <= ${min_oltpbench_output}" | bc)" ]
then
	echo "oltpbench failed"
	exit 1 # terminate and indicate error
fi

echo "oltpbench success"

# Go outside the oltpbench directory
cd ..
