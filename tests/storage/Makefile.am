## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# STORAGE
######################################################################

check_PROGRAMS += tuple_test \
				  tile_test \
				  tile_group_test

storage_tests_common_ld = \
						  $(obj_DIR)/catalog/schema.o \
						  $(obj_DIR)/common/types.o \
						  $(obj_DIR)/common/value.o \
						  $(obj_DIR)/common/varlen.o \
						  $(obj_DIR)/common/pool.o \
						  $(obj_DIR)/storage/tuple.o


tuple_test_SOURCES = storage/tuple_test.cpp

tuple_test_LDADD = \
				   $(storage_tests_common_ld)


tile_test_SOURCES = storage/tile_test.cpp

tile_test_LDADD = \
				  $(storage_tests_common_ld) \
				  $(obj_DIR)/storage/tile.o				  

tile_group_test_SOURCES = \
						  harness.cpp \
						  storage/tile_group_test.cpp

tile_group_test_LDADD = \
						$(storage_tests_common_ld) \
						$(obj_DIR)/catalog/manager.o \
						$(obj_DIR)/concurrency/transaction_manager.o \
						$(obj_DIR)/concurrency/transaction.o \
						$(obj_DIR)/storage/tile.o \
						$(obj_DIR)/storage/tile_group.o \
						$(obj_DIR)/storage/tile_group_header.o \
						$(obj_DIR)/storage/tile_group_factory.o


