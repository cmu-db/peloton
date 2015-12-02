#!/bin/bash

tuple_count=50
thread_number=$1
do_check=$2
file_dir=$3

for i in {1..9}
do
  tuples=$(($tuple_count / $thread_number))
  echo "-----------test with $tuples, thread $thread_number, ON /tmp/ ------------------"
  ./peloton/build/tests/aries_logging_test -t $tuples -b $thread_number -c $do_check
  ./peloton/build/tests/peloton_logging_test -t $tuples -b $thread_number -c $do_check
  ./peloton/build/tests/peloton_logging_test -t $tuples -b $thread_number -c $do_check -r 1
  echo "-----------test with $tuples, thread $thread_number, ON $file_dir------------------"
  ./peloton/build/tests/aries_logging_test -t $tuples -b $thread_number -c $do_check -d file_dir
  ./peloton/build/tests/peloton_logging_test -t $tuples -b $thread_number -c $do_check -d file_dir
  ./peloton/build/tests/peloton_logging_test -t $tuples -b $thread_number -c $do_check -r 1 -d file_dir
  tuple_count=$(($tuple_count * 4))
done

