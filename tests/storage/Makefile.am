## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# STORAGE
######################################################################

check_PROGRAMS += tuple_test \
				  tile_test \
				  tile_group_test

storage_tests_common_ld = \
						  $(obj_DIR)/catalog/libpeloton_la-schema.lo \
						  $(obj_DIR)/catalog/libpeloton_la-catalog_object.lo \
						  $(obj_DIR)/catalog/libpeloton_la-catalog.lo \
						  $(obj_DIR)/catalog/libpeloton_la-column.lo \
						  $(obj_DIR)/catalog/libpeloton_la-constraint.lo \
						  $(obj_DIR)/common/libpeloton_la-types.lo \
						  $(obj_DIR)/common/libpeloton_la-value.lo \
						  $(obj_DIR)/common/libpeloton_la-varlen.lo \
						  $(obj_DIR)/common/libpeloton_la-pool.lo \
						  $(obj_DIR)/storage/libpeloton_la-tuple.lo


tuple_test_SOURCES = storage/tuple_test.cpp

tuple_test_LDADD = \
				   $(storage_tests_common_ld)


tile_test_SOURCES = storage/tile_test.cpp

tile_test_LDADD = \
				  $(storage_tests_common_ld) \
				  $(obj_DIR)/storage/libpeloton_la-tile.lo				  

tile_group_test_SOURCES = \
						  harness.cpp \
						  storage/tile_group_test.cpp

tile_group_test_LDADD = \
						$(storage_tests_common_ld) \
						$(obj_DIR)/catalog/libpeloton_la-manager.lo \
						$(obj_DIR)/concurrency/libpeloton_la-transaction_manager.lo \
						$(obj_DIR)/concurrency/libpeloton_la-transaction.lo \
						$(obj_DIR)/storage/libpeloton_la-tile.lo \
						$(obj_DIR)/storage/libpeloton_la-tile_group.lo \
						$(obj_DIR)/storage/libpeloton_la-tile_group_header.lo \
						$(obj_DIR)/storage/libpeloton_la-tile_group_factory.lo


