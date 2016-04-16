## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# LOGGING
######################################################################

check_PROGRAMS += logging_test \
				buffer_pool_test \
				recovery_test

logging_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp \
						logging/logging_tests_util.cpp

buffer_pool_test_SOURCES = \
							$(logging_tests_common) \
							logging/buffer_pool_test.cpp 
							
logging_test_SOURCES = \
							$(logging_tests_common) \
							logging/logging_test.cpp 
							
recovery_test_SOURCES = \
							$(logging_tests_common) \
							logging/recovery_test.cpp 