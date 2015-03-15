## Makefile.am -- Process this file with automake to produce Makefile.in

## Common flags
include $(top_srcdir)/common.am

AM_CPPFLAGS += -isystem $(top_srcdir)/third_party/gtest/include \
			   -I$(top_srcdir)/src \
			   -I$(top_srcdir)/third_party \
			   $(GTEST_CPPFLAGS)  

AM_CXXFLAGS +=  $(GTEST_CXXFLAGS) 

AM_LDFLAGS += $(GTEST_LDFLAGS) $(GTEST_LIBS) -pthread \
			  $(top_builddir)/third_party/gtest/lib/libgtest_main.la \
			  $(top_builddir)/third_party/gtest/lib/libgtest.la 

# TESTS -- Programs run automatically by "make check"
# check_PROGRAMS -- Programs built by "make check" but not necessarily run
# this is populated by the makefiles in the subdirs

obj_DIR = $(top_builddir)/src
check_PROGRAMS =

######################################################################
# SUBDIRS
######################################################################
include $(srcdir)/catalog/Makefile.am
include $(srcdir)/common/Makefile.am
include $(srcdir)/storage/Makefile.am

TESTS = $(check_PROGRAMS)


