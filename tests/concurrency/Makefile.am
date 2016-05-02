## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

check_PROGRAMS += \
		transaction_test \
        isolation_level_test \
        pessimistic_txn_manager_test \
        optimistic_txn_manager_test \
        speculative_read_txn_manager_test \
        eager_write_txn_manager_test \
        ts_order_txn_manager_test
#        ssi_txn_manager_test

transaction_test_common = \
                            concurrency/transaction_tests_util.cpp \
                            harness.cpp

transaction_test_SOURCES = \
						   concurrency/transaction_test.cpp \
						   $(transaction_test_common)

isolation_level_test_SOURCES = \
                           concurrency/isolation_level_test.cpp \
                           $(transaction_test_common)

pessimistic_txn_manager_test_SOURCES = \
                           concurrency/pessimistic_txn_manager_test.cpp \
                           $(transaction_test_common)

optimistic_txn_manager_test_SOURCES = \
                           concurrency/optimistic_txn_manager_test.cpp \
                           $(transaction_test_common)

speculative_read_txn_manager_test_SOURCES = \
                           concurrency/speculative_read_txn_manager_test.cpp \
                           $(transaction_test_common)

eager_write_txn_manager_test_SOURCES = \
                           concurrency/eager_write_txn_manager_test.cpp \
                           $(transaction_test_common)

ts_order_txn_manager_test_SOURCES = \
                           concurrency/ts_order_txn_manager_test.cpp \
                           $(transaction_test_common)

#ssi_txn_manager_test_SOURCES = \
#                           concurrency/ssi_txn_manager_test.cpp \
#                           $(transaction_test_common)


transaction_test_LDADD =  $(peloton_tests_common_ld)
isolation_level_test_LDADD =  $(peloton_tests_common_ld)
pessimistic_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
optimistic_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
speculative_read_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
eager_write_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
ts_order_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
#ssi_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
