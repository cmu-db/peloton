## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

check_PROGRAMS += \
		transaction_test \
        isolation_level_test \
        rpwp_txn_manager_test \
        rowo_txn_manager_test \
        spec_rowo_txn_manager_test
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

rpwp_txn_manager_test_SOURCES = \
                           concurrency/rpwp_txn_manager_test.cpp \
                           $(transaction_test_common)

rowo_txn_manager_test_SOURCES = \
                           concurrency/rowo_txn_manager_test.cpp \
                           $(transaction_test_common)

spec_rowo_txn_manager_test_SOURCES = \
                           concurrency/spec_rowo_txn_manager_test.cpp \
                           $(transaction_test_common)

#ssi_txn_manager_test_SOURCES = \
#                           concurrency/ssi_txn_manager_test.cpp \
#                           $(transaction_test_common)

transaction_test_LDADD =  $(peloton_tests_common_ld)
isolation_level_test_LDADD =  $(peloton_tests_common_ld)
rpwp_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
rowo_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
spec_rowo_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
#ssi_txn_manager_test_LDADD =  $(peloton_tests_common_ld)
