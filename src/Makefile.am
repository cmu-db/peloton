## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(srcdir)/common 
AM_CXXFLAGS = -Wall -Wextra -Werror -fcolor-diagnostics 
AM_LDFLAGS  = $(PTHREAD_CFLAGS)
 
bin_PROGRAMS = main

main_SOURCES = main.cc \
			   func.cc
