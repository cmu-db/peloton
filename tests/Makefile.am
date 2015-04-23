## Makefile.am -- Process this file with automake to produce Makefile.in

## Common flags
include $(top_srcdir)/common.am

GMOCK_DIR = third_party/gmock
GTEST_DIR = $(GMOCK_DIR)/gtest

GMOCK_SRCDIR = $(top_srcdir)/$(GMOCK_DIR)
GMOCK_BUILDDIR = $(top_builddir)/$(GMOCK_DIR)

GTEST_SRCDIR = $(top_srcdir)/$(GTEST_DIR)
GTEST_BUILDDIR = $(top_builddir)/$(GTEST_DIR)

AM_CPPFLAGS += -isystem $(GTEST_SRCDIR)/include \
               -isystem $(GMOCK_SRCDIR)/include \
               -I$(top_srcdir)/src \
               -I$(top_srcdir)/third_party \
               $(GTEST_CPPFLAGS)

AM_CXXFLAGS +=  $(GTEST_CXXFLAGS) -Wno-unused-but-set-variable

AM_LDFLAGS += $(GTEST_LDFLAGS) $(GTEST_LIBS) -pthread \
			  $(GTEST_BUILDDIR)/lib/libgtest.la \
			  $(GTEST_BUILDDIR)/lib/libgtest_main.la \
			  $(GMOCK_BUILDDIR)/lib/libgmock.la \
              $(GMOCK_BUILDDIR)/lib/libgmock_main.la \
			  -I$(top_srcdir)/third_party \
			  $(top_builddir)/src/libnstore.la \
			  $(TBB_LIBS)

# TESTS -- Programs run automatically by "make check"
# check_PROGRAMS -- Programs built by "make check" but not necessarily run
# this is populated by the makefiles in the subdirs
check_PROGRAMS =

######################################################################
# SUBDIRS
######################################################################

include $(srcdir)/catalog/Makefile.am
include $(srcdir)/common/Makefile.am
include $(srcdir)/executor/Makefile.am
include $(srcdir)/expression/Makefile.am
include $(srcdir)/index/Makefile.am
include $(srcdir)/parser/Makefile.am
include $(srcdir)/planner/Makefile.am
include $(srcdir)/scheduler/Makefile.am
include $(srcdir)/storage/Makefile.am

TESTS = $(check_PROGRAMS)
