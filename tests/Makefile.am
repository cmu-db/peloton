## Makefile.am -- Process this file with automake to produce Makefile.in

## Common flags
include $(top_srcdir)/common.am

GMOCK_DIR = third_party/gmock

GMOCK_SRCDIR = $(top_srcdir)/$(GMOCK_DIR)
GMOCK_BUILDDIR = $(top_builddir)/$(GMOCK_DIR)

# TESTS -- Programs run automatically by "make check"
# check_PROGRAMS -- Programs built by "make check" but not necessarily run
# this is populated by the makefiles in the subdirs

check_SCRIPTS =
check_PROGRAMS =
TEST_EXTENSIONS =

AM_CPPFLAGS += \
	       -isystem $(GMOCK_SRCDIR) \
	       -I$(top_srcdir)/src/postgres/include \
	       -I$(top_srcdir)/src/postgres/backend	\
	       -I$(top_srcdir)/src \
	       -I$(top_srcdir)/third_party \
	       -I$(top_builddir)/src \
	       $(GTEST_CPPFLAGS)

AM_CXXFLAGS += $(GTEST_CXXFLAGS)

if BUILD_OS_IS_DARWIN
AM_LDFLAGS += $(GTEST_LDFLAGS) $(GTEST_LIBS) -static \
			  $(GMOCK_BUILDDIR)/libgmock.la \
			  $(GMOCK_BUILDDIR)/libgmock_main.la \
			  $(top_builddir)/src/libpeloton.la \
			  $(top_builddir)/src/libpelotonpg.la \
			  $(top_builddir)/src/libnanomsg.la \
			  $(TBB_LIBS) -lanl
else
AM_LDFLAGS += $(GTEST_LDFLAGS) $(GTEST_LIBS) -static -pthread \
			  $(GMOCK_BUILDDIR)/libgmock.la \
			  $(GMOCK_BUILDDIR)/libgmock_main.la \
			  $(top_builddir)/src/libpeloton.la \
			  $(top_builddir)/src/libpelotonpg.la \
			  $(top_builddir)/src/libnanomsg.la \
			  $(TBB_LIBS) -lanl
endif

######################################################################
# SUBDIRS
######################################################################

include $(srcdir)/brain/Makefile.am
include $(srcdir)/catalog/Makefile.am
include $(srcdir)/common/Makefile.am
include $(srcdir)/concurrency/Makefile.am
include $(srcdir)/executor/Makefile.am
include $(srcdir)/expression/Makefile.am
include $(srcdir)/index/Makefile.am
include $(srcdir)/logging/Makefile.am
include $(srcdir)/mfabric/Makefile.am
include $(srcdir)/planner/Makefile.am
include $(srcdir)/scheduler/Makefile.am
include $(srcdir)/storage/Makefile.am

include $(srcdir)/postgres/Makefile.am

TESTS = $(check_PROGRAMS) $(check_SCRIPTS)
LOG_COMPILER = $(srcdir)/test-driver.sh 

CLEANFILES = /tmp/*.peloton

check-build:
	$(MAKE) $(AM_MAKEFLAGS) $(check_PROGRAMS)

