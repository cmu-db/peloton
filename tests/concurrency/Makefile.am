## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

check_PROGRAMS += \
		transaction_test

transaction_test_SOURCES = \
						   concurrency/transaction_test.cpp \
						   harness.cpp

transaction_test_LDADD = \
					$(obj_DIR)/catalog/manager.o \
					$(obj_DIR)/catalog/schema.o \
					$(obj_DIR)/common/types.o \
					$(obj_DIR)/common/value.o \
					$(obj_DIR)/common/varlen.o \
					$(obj_DIR)/common/pool.o \
					$(obj_DIR)/storage/tile_group.o \
					$(obj_DIR)/storage/tile_group_header.o \
					$(obj_DIR)/storage/tile.o \
					$(obj_DIR)/storage/tuple.o \
					$(obj_DIR)/concurrency/transaction_manager.o \
					$(obj_DIR)/concurrency/transaction.o

