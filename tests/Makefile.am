## Makefile.am -- Process this file with automake to produce Makefile.in
 
## Common flags
include $(top_srcdir)/common.am

GMOCK_DIR = third_party/gmock

GMOCK_SRCDIR = $(top_srcdir)/$(GMOCK_DIR)
GMOCK_BUILDDIR = $(top_builddir)/$(GMOCK_DIR)
 
# TESTS -- Programs run automatically by "make check"
# check_PROGRAMS -- Programs built by "make check" but not necessarily run
# this is populated by the makefiles in the subdirs
check_PROGRAMS =

AM_CPPFLAGS += \
			   -isystem $(GMOCK_SRCDIR) \
			   -I$(top_srcdir)/src \
			   -I$(top_srcdir)/third_party \
			   $(GTEST_CPPFLAGS)

AM_CXXFLAGS += $(GTEST_CXXFLAGS) -Wno-unused-but-set-variable

AM_LDFLAGS += $(GTEST_LDFLAGS) $(GTEST_LIBS) -pthread -static \
			  $(GMOCK_BUILDDIR)/libgmock.la \
			  $(GMOCK_BUILDDIR)/libgmock_main.la \
			  $(top_builddir)/src/libnstore.la \
			  $(TBB_LIBS)

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

