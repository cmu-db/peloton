## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# INDEX
######################################################################

check_PROGRAMS += index_test

index_test_SOURCES = index/index_test.cpp

index_test_LDADD = \
				   $(obj_DIR)/catalog/libpeloton_la-schema.lo \
				   $(obj_DIR)/common/libpeloton_la-types.lo \
				   $(obj_DIR)/common/libpeloton_la-value.lo \
				   $(obj_DIR)/common/libpeloton_la-varlen.lo \
				   $(obj_DIR)/common/libpeloton_la-pool.lo \
				   $(obj_DIR)/storage/libpeloton_la-tuple.lo \
				   $(obj_DIR)/index/libpeloton_la-index.lo \
				   $(obj_DIR)/index/libpeloton_la-index_factory.lo \
				   $(obj_DIR)/index/libpeloton_la-concurrent_btree.lo \
				   $(obj_DIR)/index/libpeloton_la-btree_index.lo
