## Makefile.am -- Process this file with automake to produce Makefile.in

# Creates the "check-valgrind" target
@VALGRIND_CHECK_RULES@

@CODE_COVERAGE_RULES@
CODE_COVERAGE_IGNORE_PATTERN="/usr/*" "*/third_party/*" "*/test/*"

AM_CPPFLAGS = -isystem $(top_srcdir)/third_party/gtest/include -I$(top_srcdir)/src \
			  -I$(top_srcdir)/third_party \
			  $(GTEST_CPPFLAGS)  

AM_CXXFLAGS = -std=c++11 -O0 -g3 -Wall -Wextra -fno-strict-aliasing \
			  $(GTEST_CXXFLAGS) \
			  $(BOOST_CPPFLAGS) \
			  $(CODE_COVERAGE_CFLAGS) 

AM_LDFLAGS = $(GTEST_LDFLAGS) $(GTEST_LIBS) -pthread \
			 $(top_builddir)/third_party/gtest/lib/libgtest_main.la \
			 $(top_builddir)/third_party/gtest/lib/libgtest.la \
			 $(BOOST_LDFLAGS) \
			 $(BOOST_DATE_TIME_LIBS) \
			 $(BOOST_SYSTEM) \
			 $(BOOST_LOG_LIBS) \
			 $(BOOST_LOG_SETUP_LIBS) \
			 $(BOOST_THREAD_LIBS) \
			 $(CODE_COVERAGE_LDFLAGS) 

# TESTS -- Programs run automatically by "make check"
# check_PROGRAMS -- Programs built by "make check" but not necessarily run

check_PROGRAMS = func_test \
				 tuple_test \
				 tuple_schema_test \
				 tile_test \
				 tile_group_test \
				 logger_test

TESTS = $(check_PROGRAMS)

obj_DIR = $(top_builddir)/src

func_test_SOURCES = func_test.cpp
func_test_LDADD = \
				  $(obj_DIR)/backend/tester.o

tuple_test_SOURCES = tuple_test.cpp
tuple_test_LDADD = \
				   $(obj_DIR)/catalog/schema.o \
				   $(obj_DIR)/common/types.o \
				   $(obj_DIR)/common/value.o \
				   $(obj_DIR)/common/varlen.o \
				   $(obj_DIR)/storage/tuple.o  

tuple_schema_test_SOURCES = tuple_schema_test.cpp
tuple_schema_test_LDADD = \
						  $(obj_DIR)/catalog/schema.o \
						  $(obj_DIR)/common/types.o 

tile_test_SOURCES = tile_test.cpp
tile_test_LDADD = \
				  $(obj_DIR)/catalog/schema.o \
				  $(obj_DIR)/common/types.o \
				  $(obj_DIR)/common/value.o \
				  $(obj_DIR)/common/varlen.o \
				  $(obj_DIR)/storage/tuple.o \
				  $(obj_DIR)/storage/tile.o 

tile_group_test_SOURCES = tile_group_test.cpp
tile_group_test_LDADD = \
						$(obj_DIR)/catalog/schema.o \
						$(obj_DIR)/common/types.o \
						$(obj_DIR)/common/value.o \
						$(obj_DIR)/common/varlen.o \
						$(obj_DIR)/storage/tuple.o \
						$(obj_DIR)/storage/tile.o \
						$(obj_DIR)/storage/tile_group.o \
						$(obj_DIR)/storage/tile_group_header.o

logger_test_SOURCES = logger_test.cpp

