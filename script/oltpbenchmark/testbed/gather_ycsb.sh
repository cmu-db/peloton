#!/bin/bash

for ratio in "100_0_0_0" "0_0_100_0" "10_0_90_0"; do
	echo $ratio
	for d in `ls | grep ycsb_collected | grep $ratio | sort -t '_' -k9,9 -g`; do
		b=`echo $d | cut -d'_' -f 9`
	    echo -n "$b "
	done
	echo ""
	for d in `ls | grep ycsb_collected | grep $ratio | sort -t '_' -k9,9 -g`; do
		#b=`echo $d | cut -d'-' -f 9`
		thrpt=`head -8 ${d}/*.res | tail -1 | cut -d ',' -f 2`
		echo -n "$thrpt "
	done
	echo ""
done
