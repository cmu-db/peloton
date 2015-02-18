## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(nstore_INCLUDES)
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -fcolor-diagnostics 
AM_LDFLAGS  = $(PTHREAD_CFLAGS) 

bin_PROGRAMS = nstore

######################################################################
# N-STORE
######################################################################
nstore_SOURCES =  \
				  $(backend_FILES) \
				  $(catalog_FILES) \
				  $(common_FILES) \
				  $(executor_FILES) \
				  $(parser_FILES) \
				  $(storage_FILES)

######################################################################
# N-STORE INCLUDES
######################################################################
nstore_INCLUDES = \
				  -I$(srcdir)/backend \
				  -I$(srcdir)/catalog \
				  -I$(srcdir)/common \
				  -I$(srcdir)/parser \
				  -I$(srcdir)/parser/statements \
				  -I$(srcdir)/storage

######################################################################
# BACKEND
######################################################################
backend_FILES = \
				backend/nstore.cc \
				backend/tester.cc \
				backend/parser.cc 
######################################################################
# CATALOG
######################################################################
catalog_FILES = \
				catalog/abstract_tuple_schema.cpp \
				catalog/tuple_schema.cpp

######################################################################
# EXECUTOR
######################################################################
executor_FILES = 

######################################################################
# STORAGE
######################################################################
storage_FILES = \
				storage/abstract_tuple.cpp

######################################################################
# PARSER
######################################################################
parser_FILES = \
			   parser/bison_parser.cpp \
			   parser/destruct.cpp\
			   parser/Expr.cpp \
			   parser/flex_lexer.cpp \
			   parser/sqlhelper.cpp \
			   parser/SQLParser.cpp

