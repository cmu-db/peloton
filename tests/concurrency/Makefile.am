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
					$(obj_DIR)/catalog/libpeloton_la-manager.lo \
					$(obj_DIR)/catalog/libpeloton_la-schema.lo \
					$(obj_DIR)/common/libpeloton_la-types.lo \
					$(obj_DIR)/common/libpeloton_la-value.lo \
					$(obj_DIR)/common/libpeloton_la-varlen.lo \
					$(obj_DIR)/common/libpeloton_la-pool.lo \
					$(obj_DIR)/storage/libpeloton_la-tile_group.lo \
					$(obj_DIR)/storage/libpeloton_la-tile_group_header.lo \
					$(obj_DIR)/storage/libpeloton_la-tile.lo \
					$(obj_DIR)/storage/libpeloton_la-tuple.lo \
					$(obj_DIR)/concurrency/libpeloton_la-transaction_manager.lo \
					$(obj_DIR)/concurrency/libpeloton_la-transaction.lo

