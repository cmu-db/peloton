## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

check_PROGRAMS += sample_test \
				  logger_test \
				  value_test \
				  transaction_test

sample_test_SOURCES = common/sample_test.cpp

logger_test_SOURCES = common/logger_test.cpp

value_test_SOURCES = common/value_test.cpp

value_test_LDADD = \
				   $(obj_DIR)/common/value.o \
				   $(obj_DIR)/common/varlen.o \
				   $(obj_DIR)/common/types.o \
				   $(obj_DIR)/common/pool.o

transaction_test_SOURCES = \
						   common/transaction_test.cpp \
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
						 $(obj_DIR)/common/transaction.o

