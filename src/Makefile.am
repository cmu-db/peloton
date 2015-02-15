## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(srcdir)/parser -I$(srcdir)/parser/statements 
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -fcolor-diagnostics 
AM_LDFLAGS  = $(PTHREAD_CFLAGS) 

bin_PROGRAMS = main

main_LDADD = libsql_parser.a

main_SOURCES = parse.cc \
			   func.cc

noinst_LIBRARIES = libsql_parser.a

libsql_parser_a_SOURCES =  parser/bison_parser.cpp \ 
                           parser/destruct.cpp \
						   parser/Expr.cpp \ 
						   parser/flex_lexer.cpp \ 
						   parser/sqlhelper.cpp \  
						   parser/SQLParser.cpp

