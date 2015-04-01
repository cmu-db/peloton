## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# INDEX
######################################################################

check_PROGRAMS += index_test

index_test_SOURCES = index/index_test.cpp
index_test_LDADD = \
				   $(obj_DIR)/catalog/schema.o \
				   $(obj_DIR)/common/types.o \
				   $(obj_DIR)/common/value.o \
				   $(obj_DIR)/common/varlen.o \
				   $(obj_DIR)/storage/tuple.o \
				   $(obj_DIR)/storage/tile.o \
				   $(obj_DIR)/storage/tile_group.o \
				   $(obj_DIR)/index/index.o \
				   $(obj_DIR)/index/concurrent_btree.o \
				   $(obj_DIR)/index/btree_multimap_index.o \
				   $(obj_DIR)/index/index_factory.o

