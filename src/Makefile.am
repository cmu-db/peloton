## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(nstore_INCLUDES) $(third_party_INCLUDES)
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -fcolor-diagnostics 
AM_LDFLAGS  = $(PTHREAD_CFLAGS) -lboost_system 

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
# THIRD-PARTY INCLUDES
######################################################################
third_party_INCLUDES = \
					   -I$(top_srcdir)/third_party 

######################################################################
# BACKEND
######################################################################
backend_FILES = \
				backend/nstore.cpp \
				backend/tester.cpp \
				backend/parser.cpp 
######################################################################
# CATALOG
######################################################################
catalog_FILES = \
				catalog/tuple_schema.cpp 

######################################################################
# COMMON
######################################################################
common_FILES = \
			   common/value.cpp \
			   common/types.cpp \
			   common/varlen.cpp  

######################################################################
# EXECUTOR
######################################################################
executor_FILES = 

######################################################################
# STORAGE
######################################################################
storage_FILES = \
				storage/tuple.cpp

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

