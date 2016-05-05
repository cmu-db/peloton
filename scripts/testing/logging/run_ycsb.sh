#!/bin/bash
SCALE=1
WRITE=1.0
rm -rf pl_log0/*.log
rm -rf pl_checkpoint/*.log
for backend in $(seq 1 1 20)
do

	filename="gc_on_sync_${WRITE}.log"
  	sh ./ycsb -b ${backend} -l 1 -u $WRITE -x 1
  	sleep 1
	echo "-b $backend -l 1 -u ${WRITE} -x 1" >> $filename
  	tail -n 1 outputfile.summary >> $filename
        rm -rf pl_log0/*.log
        rm -rf pl_checkpoint/*.log

done
