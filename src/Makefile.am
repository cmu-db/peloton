## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

## Common flags
include $(top_srcdir)/common.am

######################################################################
# SUBDIRS
######################################################################

include $(srcdir)/backend/brain/Makefile.am
include $(srcdir)/backend/bridge/Makefile.am
include $(srcdir)/backend/catalog/Makefile.am
include $(srcdir)/backend/common/Makefile.am
include $(srcdir)/backend/concurrency/Makefile.am
include $(srcdir)/backend/executor/Makefile.am
include $(srcdir)/backend/expression/Makefile.am
include $(srcdir)/backend/index/Makefile.am
include $(srcdir)/backend/main/Makefile.am
include $(srcdir)/backend/planner/Makefile.am
include $(srcdir)/backend/scheduler/Makefile.am
include $(srcdir)/backend/storage/Makefile.am

include $(srcdir)/postgres/backend/Makefile.am
include $(srcdir)/postgres/common/Makefile.am
include $(srcdir)/postgres/interfaces/Makefile.am
include $(srcdir)/postgres/port/Makefile.am
include $(srcdir)/postgres/timezone/Makefile.am

bin_peloton_PROGRAMS = peloton

bin_pelotondir = /usr/local/peloton/bin

lib_LTLIBRARIES += libpeloton.la

DEFAULT_INCLUDES =

######################################################################
# PELOTON LIBRARY
######################################################################

libpostgres_CXXFLAGS = $(AM_CXXFLAGS) \
			  -Wno-unused-variable -Wno-unused-but-set-variable \
			  -Wno-unused-parameter -Wno-missing-field-initializers \
			  -Wno-array-bounds -Wno-clobbered -Wno-strict-overflow

libpeloton_la_SOURCES = \
			$(brain_FILES)

## HACK: To make use of different include flags for peloton and postgres.

libpeloton_la_CPPFLAGS = $(AM_CPPFLAGS) \
						 -I$(srcdir)/postgres/include \
						 -I$(srcdir)/postgres/backend \
						 -I$(srcdir)/ \
						 -I. -I$(top_srcdir)/src -I.. \
						 $(third_party_INCLUDES)

libpeloton_la_CXXFLAGS = $(AM_CXXFLAGS) -msse4.2

libpeloton_la_LIBADD = libpostgres.la

libpostgres_la_SOURCES = \
			$(postgres_backend_FILES) \
			$(postgres_common_FILES) \
			$(postgres_port_FILES) \
			$(postgres_timezone_FILES)

libpostgres_la_CPPFLAGS = $(AM_CPPFLAGS) \
						 -I$(srcdir)/postgres/include \
						 -I$(srcdir)/postgres/backend \
						 -I$(srcdir)/ \
						 -I. -I$(top_srcdir)/src -I.. \
						 $(third_party_INCLUDES)

if BUILD_OS_IS_DARWIN
libpeloton_la_LDFLAGS = -lm -ldw
## if build os is not darwin
else
libpeloton_la_LDFLAGS = -lm -ldw
endif

######################################################################
# PELOTON
######################################################################

peloton_SOURCES = backend/main/main.cpp
peloton_LDFLAGS =
peloton_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS) 
peloton_LDADD = libpeloton.la $(TBB_LIBS)

######################################################################
# THIRD-PARTY INCLUDES
######################################################################

third_party_INCLUDES = -I$(top_srcdir)/third_party 
