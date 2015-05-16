## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += expression_test

expression_test_SOURCES = expression/expression_test.cpp

expression_test_LDADD = \
						$(obj_DIR)/catalog/schema.o \
						$(obj_DIR)/common/types.o \
						$(obj_DIR)/common/value.o \
						$(obj_DIR)/common/varlen.o \
						$(obj_DIR)/storage/tuple.o \
						$(obj_DIR)/expression/abstract_expression.o \
						$(obj_DIR)/expression/expression_util.o
