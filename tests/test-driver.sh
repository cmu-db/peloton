#! /bin/sh
name=${1##*/}
echo $name
valgrind --error-exitcode=1 --leak-check=full --quiet --track-origins=yes --log-file=valgrind_${name}.log -v ./${name} --gtest_output="xml:"$test".xml"
#exec $1 --gtest_output="xml:"$1".xml"
