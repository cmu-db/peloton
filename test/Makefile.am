## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -isystem $(top_srcdir)/third_party/gtest/include -I$(top_srcdir)/src $(GTEST_CPPFLAGS)
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -fcolor-diagnostics $(GTEST_CXXFLAGS)  
AM_LDFLAGS = $(GTEST_LDFLAGS) $(GTEST_LIBS) -pthread \
			 $(top_builddir)/third_party/gtest/lib/libgtest_main.la \
			 $(top_builddir)/third_party/gtest/lib/libgtest.la

# TESTS -- Programs run automatically by "make check"

# check_PROGRAMS -- Programs built by "make check" but not necessarily run
TESTS = 
check_PROGRAMS = 

TESTS += func_test \
		 tuple_schema_test

check_PROGRAMS += func_test \
				  tuple_schema_test

func_test_SOURCES = func_test.cpp
func_test_LDADD = $(top_builddir)/src/backend/tester.o

tuple_schema_test_SOURCES = tuple_schema_test.cpp
tuple_schema_test_LDADD = $(top_builddir)/src/catalog/tuple_schema.o \
						  $(top_builddir)/src/common/types.o 
