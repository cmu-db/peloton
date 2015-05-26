## Makefile.am -- Process this file with automake to produce Makefile.in
 
######################################################################
# PARSER
######################################################################

parser_FILES = \
			   parser_bison.ypp \
			   parser_flex.lpp \
			   parser/parser.cpp \
			   parser/parser_utils.cpp \
			   parser/sql_statement.cpp \
			   parser/table_ref.cpp
			   			   
AM_YFLAGS = -d

parser_INCLUDES = \
				  -I$(srcdir)/parser
