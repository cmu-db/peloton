## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# CATALOG
######################################################################

check_PROGRAMS += catalog_test \
				  manager_test \
				  tuple_schema_test \
				  constraints_test

catalog_test_SOURCES = catalog/catalog_test.cpp

manager_test_SOURCES = catalog/manager_test.cpp

tuple_schema_test_SOURCES = catalog/tuple_schema_test.cpp

constraints_test_SOURCES =  catalog/constraints_tests_util.cpp \
                            harness.cpp \
                            catalog/constraints_test.cpp \
                            concurrency/transaction_tests_util.cpp
