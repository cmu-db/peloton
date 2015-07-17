## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

check_PROGRAMS += \
		transaction_test

transaction_test_SOURCES = \
						   concurrency/transaction_test.cpp \
						   harness.cpp

transaction_test_LDADD =  $(peloton_tests_common_ld)