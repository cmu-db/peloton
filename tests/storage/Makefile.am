## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# STORAGE
######################################################################

check_PROGRAMS += tuple_test \
				  tile_test \
				  tile_group_test

tuple_test_SOURCES = storage/tuple_test.cpp

tuple_test_LDADD = $(peloton_tests_common_ld)

tile_test_SOURCES = storage/tile_test.cpp

tile_test_LDADD =  $(peloton_tests_common_ld)				  

tile_group_test_SOURCES = \
						  harness.cpp \
						  storage/tile_group_test.cpp

tile_group_test_LDADD = $(peloton_tests_common_ld)


