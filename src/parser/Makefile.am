## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(srcdir)/parser 
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -fcolor-diagnostics 
AM_LDFLAGS  = $(PTHREAD_CFLAGS) 

SUBDIRS = parser

bin_PROGRAMS = main

main_SOURCES = main.cc \
			   parser/bison_parser.cpp \
			   parser/destruct.cpp \
			   parser/Expr.cpp \
			   parser/flex_lexer.cpp \
			   parser/sqlhelper.cpp \
			   parser/SQLParser.cpp \
			   parse.cc \
			   func.cc
