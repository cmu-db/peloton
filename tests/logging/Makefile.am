## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# LOGGING
######################################################################

check_PROGRAMS += checkpoint_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

checkpoint_test_SOURCES = \
							$(executor_tests_common) \ 
							logging/checkpoint_test.cpp