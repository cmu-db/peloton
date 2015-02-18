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
				  $(parser_FILES) 

######################################################################
# N-STORE INCLUDES
######################################################################
nstore_INCLUDES = \
				 -I$(srcdir)/backend \
				 -I$(srcdir)/parser \
				 -I$(srcdir)/parser/statements 

######################################################################
# BACKEND
######################################################################
backend_FILES = \
		  backend/nstore.cc \
		  backend/tester.cc \
		  backend/parser.cc 

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

