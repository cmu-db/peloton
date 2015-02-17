## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(srcdir)/parser -I$(srcdir)/parser/statements 
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -fcolor-diagnostics 
AM_LDFLAGS  = $(PTHREAD_CFLAGS) -lrt

bin_PROGRAMS = nstore

nstore_SOURCES = \ 
			   	func.cc \
				nstore.cc \
				parse.cc \
			   	parser/bison_parser.cpp \ 
			   	parser/destruct.cpp \
				parser/Expr.cpp \ 
				parser/flex_lexer.cpp \ 
				parser/sqlhelper.cpp \  
				parser/SQLParser.cpp

