#! /bin/sh
exec $1 --gtest_output="xml:"$1".xml"
