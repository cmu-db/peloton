#!/bin/bash
SCALE=5
#$WRITE=100
rm -rf pl_log0/*.log
rm -rf pl_checkpoint/*.log
for backend in $(seq 1 1 20)
do
   for write in $(seq 0.50 0.40 0.990)
   do
        for size in $(seq 32 32 33)
        do
          for wait in $(seq 0 3 1)
          do
             for checkpoint in 0
             do
        #       ./rebuild.sh $pool
                sh ./ycsb -k $SCALE -u $write -b $backend -l 1 -s 1 -f $size -w $wait -p $chechpoint;
                sleep 1
                cat outputfile.summary >> baseline.log
                rm -rf pl_log0/*.log
                rm -rf pl_checkpoint/*.log



             done
          done
        done
   done
   # cleanup logs
   rm -rf pl_log0/*.log
   rm -rf pl_checkpoint/*.log
done
