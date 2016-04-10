## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# LOGGING
######################################################################

check_PROGRAMS += checkpoint_test \
				logging_test \
				buffer_pool_test
				
logging_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

buffer_pool_test_SOURCES = \
							$(executor_tests_common) \ 
							logging/buffer_pool_test.cpp 

checkpoint_test_SOURCES = \
							$(executor_tests_common) \ 
							logging/checkpoint_test.cpp 
							
logging_test_SOURCES = \
							$(executor_tests_common) \ 
							logging/logging_test.cpp 
