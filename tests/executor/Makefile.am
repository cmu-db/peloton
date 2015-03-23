## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += logical_tile_test

logical_tile_test_SOURCES = executor/logical_tile_test.cpp
logical_tile_test_LDADD = \
						  $(obj_DIR)/common/types.o \
						  $(obj_DIR)/executor/logical_tile.o\
						  $(obj_DIR)/catalog/schema.o \
						  $(obj_DIR)/common/value.o \
						  $(obj_DIR)/common/varlen.o \
						  $(obj_DIR)/storage/tuple.o \
						  $(obj_DIR)/storage/tile.o \
						  $(obj_DIR)/storage/tile_group.o \
						  $(obj_DIR)/storage/tile_group_header.o
