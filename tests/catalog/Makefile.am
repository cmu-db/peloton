## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# CATALOG
######################################################################

check_PROGRAMS += catalog_test \
			manager_test \
			tuple_schema_test

catalog_tests_common_ld = \
						  $(obj_DIR)/catalog/manager.o \
						  $(obj_DIR)/catalog/schema.o \
						  $(obj_DIR)/common/types.o \
						  $(obj_DIR)/common/value.o \
						  $(obj_DIR)/common/varlen.o \
						  $(obj_DIR)/common/pool.o \
						  $(obj_DIR)/storage/tile_group.o \
						  $(obj_DIR)/storage/tile_group_header.o \
						  $(obj_DIR)/storage/tile_group_factory.o \
						  $(obj_DIR)/storage/tile.o \
						  $(obj_DIR)/storage/tuple.o

catalog_test_SOURCES = \
					   catalog/catalog_test.cpp

catalog_test_LDADD =

manager_test_SOURCES = catalog/manager_test.cpp

manager_test_LDADD = \
					 $(catalog_tests_common_ld)

tuple_schema_test_SOURCES = catalog/tuple_schema_test.cpp

tuple_schema_test_LDADD = \
						  $(obj_DIR)/catalog/schema.o \
						  $(obj_DIR)/common/types.o
