## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# CATALOG
######################################################################

check_PROGRAMS += tuple_schema_test \
                  transaction_test

tuple_schema_test_SOURCES = catalog/tuple_schema_test.cpp
tuple_schema_test_LDADD = \
						  $(obj_DIR)/catalog/schema.o \
						  $(obj_DIR)/common/types.o 
						  
transaction_test_SOURCES = catalog/transaction_test.cpp
transaction_test_LDADD = \
						  $(obj_DIR)/catalog/transaction.o

						  
