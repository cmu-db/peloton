## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# STATISTICS
######################################################################

check_PROGRAMS += \
		basic_stats_test
		
stats_test_common = \
		concurrency/transaction_tests_util.cpp \
		executor/executor_tests_util.cpp \
    harness.cpp

basic_stats_test_SOURCES = \
		statistics/basic_stats_test.cpp \
		$(stats_test_common)
		
		basic_stats_test_LDADD =  $(peloton_tests_common_ld)
