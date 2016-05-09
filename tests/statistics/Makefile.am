## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# STATISTICS
######################################################################

check_PROGRAMS += \
		stats_test
		
stats_test_common = \
		statistics/stats_tests_util.cpp \
		executor/executor_tests_util.cpp \
    harness.cpp

stats_test_SOURCES = \
		statistics/stats_test.cpp \
		$(stats_test_common)
		
		stats_test_LDADD =  $(peloton_tests_common_ld)
