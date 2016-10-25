#!/bin/bash
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 [target_db] [workload] [max number of client]"
else 
	echo $1
	for (( c=1; c<=$3; c++ ))
	do
        	./test_nop.sh $1 $2 $c 2> /dev/null
	done
fi
