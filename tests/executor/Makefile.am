## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += \
				  logical_tile_test \
				  materialization_test \
				  concat_test \
				  insert_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

logical_tile_test_SOURCES = \
							$(executor_tests_common) \
							executor/logical_tile_test.cpp 

materialization_test_SOURCES = \
                               $(executor_tests_common) \
							   executor/materialization_test.cpp 

concat_test_SOURCES = \
					  $(executor_tests_common) \
					  executor/concat_test.cpp

insert_test_SOURCES = \
					  $(executor_tests_common) \
					  executor/insert_test.cpp 
