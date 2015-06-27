## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += expression_test

expression_test_SOURCES = expression/expression_test.cpp

expression_test_LDADD = \
						$(obj_DIR)/catalog/libpeloton_la-schema.lo \
						$(obj_DIR)/common/libpeloton_la-types.lo \
						$(obj_DIR)/common/libpeloton_la-value.lo \
						$(obj_DIR)/common/libpeloton_la-varlen.lo \
          				$(obj_DIR)/common/libpeloton_la-pool.lo \
						$(obj_DIR)/storage/libpeloton_la-tuple.lo \
						$(obj_DIR)/expression/libpeloton_la-abstract_expression.lo \
						$(obj_DIR)/expression/libpeloton_la-expression_util.lo
