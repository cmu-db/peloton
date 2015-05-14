## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += \
				  logical_tile_test \
				  materialization_test \
				  mutate_test \
				  seq_scan_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

logical_tile_test_SOURCES = \
							$(executor_tests_common) \
							executor/logical_tile_test.cpp 

materialization_test_SOURCES = \
							   $(executor_tests_common) \
							   executor/materialization_test.cpp 

mutate_test_SOURCES = \
					  $(executor_tests_common) \
					  executor/mutate_test.cpp 

seq_scan_test_SOURCES = \
						$(executor_tests_common) \
						executor/seq_scan_test.cpp 
