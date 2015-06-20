## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# PARSER
######################################################################

check_PROGRAMS += parser_test

parser_test_SOURCES = parser/parser_test.cpp

parser_test_LDADD = \
		    $(obj_DIR)/common/types.o \
		    $(obj_DIR)/common/value.o \
		    $(obj_DIR)/common/varlen.o \
		    $(obj_DIR)/common/pool.o \
		    $(obj_DIR)/parser/parser.o \
		    $(obj_DIR)/../parser_bison.o \
		    $(obj_DIR)/../parser_flex.o \
		    $(obj_DIR)/parser/parser_utils.o \
		    $(obj_DIR)/parser/sql_statement.o \
		    $(obj_DIR)/parser/table_ref.o \
		    $(obj_DIR)/expression/abstract_expression.o \
		    $(obj_DIR)/expression/expression_util.o

