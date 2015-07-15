## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# CATALOG
######################################################################

check_PROGRAMS += catalog_test \
			manager_test \
			tuple_schema_test

catalog_tests_common_ld = \
						  $(obj_DIR)/catalog/libpeloton_la-manager.lo \
						  $(obj_DIR)/catalog/libpeloton_la-catalog_object.lo \
						  $(obj_DIR)/catalog/libpeloton_la-column.lo \
						  $(obj_DIR)/catalog/libpeloton_la-constraint.lo \
						  $(obj_DIR)/catalog/libpeloton_la-schema.lo \
						  $(obj_DIR)/common/libpeloton_la-types.lo \
						  $(obj_DIR)/common/libpeloton_la-value.lo \
						  $(obj_DIR)/common/libpeloton_la-varlen.lo \
						  $(obj_DIR)/common/libpeloton_la-pool.lo \
						  $(obj_DIR)/storage/libpeloton_la-tile_group.lo \
						  $(obj_DIR)/storage/libpeloton_la-tile_group_header.lo \
						  $(obj_DIR)/storage/libpeloton_la-tile_group_factory.lo \
						  $(obj_DIR)/storage/libpeloton_la-tile.lo \
						  $(obj_DIR)/storage/libpeloton_la-tuple.lo

catalog_test_SOURCES = \
					   catalog/catalog_test.cpp

catalog_test_LDADD =

manager_test_SOURCES = catalog/manager_test.cpp

manager_test_LDADD = \
					 $(catalog_tests_common_ld)

tuple_schema_test_SOURCES = catalog/tuple_schema_test.cpp

tuple_schema_test_LDADD = \
					      $(catalog_tests_common_ld)