#!/bin/bash
# This script gathers experiment results of different number of clients for no-op queries
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 [target_db] [workload] [max number of client]"
else 
	echo $1
	for (( i=1; i<=$3; i++ ))
	do
        	./test_nop.sh $1 $2 $i 2> /dev/null
	done
fi
