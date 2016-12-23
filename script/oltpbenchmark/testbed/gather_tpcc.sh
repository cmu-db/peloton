#!/bin/bash
for d in `ls | grep tpcc_collected | sort -t '_' -k10,10 -g`; do
		b=`echo $d | cut -d'_' -f 10`
	    echo -n "$b "
done
echo ""
for d in `ls | grep tpcc_collected | sort -t '_' -k10,10 -g`; do
		#b=`echo $d | cut -d'-' -f 9`
		thrpt=`head -8 ${d}/*.res | tail -1 | cut -d ',' -f 2`
		echo -n "$thrpt "
done
echo ""
