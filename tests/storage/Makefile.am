## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# STORAGE
######################################################################

check_PROGRAMS += \
		tuple_test \
		tile_test \
		tile_group_test \
		data_table_test \
		tile_group_iterator_test

tuple_test_SOURCES = \
		storage/tuple_test.cpp

tile_test_SOURCES = \
		storage/tile_test.cpp

tile_group_test_SOURCES = \
		harness.cpp \
		storage/tile_group_test.cpp

data_table_test_SOURCES = \
		storage/data_table_test.cpp \
		executor/executor_tests_util.cpp \
		harness.cpp
		
tile_group_iterator_test_SOURCES = \
		storage/tile_group_iterator_test.cpp
