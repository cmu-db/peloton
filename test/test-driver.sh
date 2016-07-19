#! /bin/sh
# Extract the file name of the tes executable
name=${1##*/}
echo $name
echo $pwd
valgrind --error-exitcode=1 --leak-check=full --quiet --track-origins=yes --suppressions=./valgrind.supp --xml=yes --xml-file=valgrind_${name}.xml --log-file=valgrind_${name}.log -v ./${name} --gtest_output=xml:"unit_"${name}".xml"
#exec $1 --gtest_output="xml:"$1".xml"
