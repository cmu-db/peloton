## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += logical_tile_test materialization_test concat_test

logical_tile_test_SOURCES = \
                            executor/executor_tests_util.cpp \
                            executor/logical_tile_test.cpp \
                            harness.cpp

materialization_test_SOURCES = \
                              executor/executor_tests_util.cpp \
                              executor/materialization_test.cpp \
                              harness.cpp

concat_test_SOURCES = \
                      executor/concat_test.cpp \
                      executor/executor_tests_util.cpp \
                      harness.cpp
