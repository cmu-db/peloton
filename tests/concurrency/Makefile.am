## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

check_PROGRAMS += \
		transaction_test \
        isolation_level_test \
        # pessimistic_transaction_manager_test \
        # optimistic_transaction_manager_test 

transaction_test_common = \
                            concurrency/transaction_tests_util.cpp \
                            harness.cpp

transaction_test_SOURCES = \
						   concurrency/transaction_test.cpp \
						   $(transaction_test_common)

isolation_level_test_SOURCES = \
                           concurrency/isolation_level_test.cpp \
                           $(transaction_test_common)

# pessimistic_transaction_manager_test_SOURCES = \
#                           concurrency/pessimistic_transaction_manager_test.cpp \
#                           $(transaction_test_common)

# optimistic_transaction_manager_test_SOURCES = \
#                           concurrency/optimistic_transaction_manager_test.cpp \
#                           $(transaction_test_common)

transaction_test_LDADD =  $(peloton_tests_common_ld)
isolation_level_test_LDADD =  $(peloton_tests_common_ld)
# pessimistic_transaction_manager_test_LDADD =  $(peloton_tests_common_ld)
# optimistic_transaction_manager_test_LDADD =  $(peloton_tests_common_ld)
