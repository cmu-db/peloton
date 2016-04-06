## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# CONCURRENCY
######################################################################

concurrency_FILES = \
    backend/concurrency/optimistic_txn_manager.cpp \
    backend/concurrency/pessimistic_txn_manager.cpp \
    backend/concurrency/ssi_txn_manager.cpp \
    backend/concurrency/eager_write_txn_manager.cpp \
    backend/concurrency/speculative_read_txn_manager.cpp \
    backend/concurrency/ts_order_txn_manager.cpp \
    backend/concurrency/transaction_manager.cpp \
    backend/concurrency/transaction.cpp \
    backend/concurrency/transaction_manager_factory.cpp
concurrency_INCLUDES = \
					   -I$(srcdir)/concurrency