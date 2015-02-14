## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -isystem $(top_srcdir)/third_party/gtest/include -I$(top_srcdir)/src $(GTEST_CPPFLAGS)
AM_CXXFLAGS = $(GTEST_CXXFLAGS)
AM_LDFLAGS = $(GTEST_LDFLAGS) $(GTEST_LIBS) \
	     $(top_srcdir)/third_party/gtest/lib/libgtest_main.la \
	     $(top_srcdir)/third_party/gtest/lib/libgtest.la

# TESTS -- Programs run automatically by "make check"

# check_PROGRAMS -- Programs built by "make check" but not necessarily run
TESTS = 
check_PROGRAMS = 

TESTS += func_test
check_PROGRAMS += func_test

func_test_SOURCES = func_test.cpp
func_test_LDADD = $(top_builddir)/src/func.o
