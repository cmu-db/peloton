## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# COMMON
######################################################################

## Common flags
include $(top_srcdir)/common.am

######################################################################
# SUBDIRS
######################################################################

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
include $(srcdir)/postgres/bridge/Makefile.am
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

libpeloton_la_SOURCES = \
			$(backend_FILES) \
			$(bridge_FILES) \
			$(catalog_FILES) \
			$(common_FILES) \
			$(concurrency_FILES) \
			$(executor_FILES) \
			$(expression_FILES) \
			$(index_FILES) \
			$(planner_FILES) \
			$(scheduler_FILES) \
			$(storage_FILES) \
			$(postgres_backend_FILES) \
			$(postgres_bridge_FILES) \
			$(postgres_common_FILES) \
			$(postgres_port_FILES) \
			$(postgres_timezone_FILES)

## HACK: To make use of different include flags for peloton and postgres.

libpeloton_la_CPPFLAGS = -I$(srcdir)/postgres/include \
			 -I$(srcdir)/postgres/backend \
			 -I$(srcdir)/ \
			 $(third_party_INCLUDES)

libpeloton_la_CFLAGS = $(postgres_common_INCLUDES)
libpeloton_la_CXXFLAGS = -std=c++11 -I. -I$(top_srcdir)/src -I.. $(third_party_INCLUDES) -msse4.2 \
						 -fPIC -fpic -fpermissive 

if BUILD_OS_IS_DARWIN
libpeloton_la_LDFLAGS = -lm
libpeloton_la_CFLAGS  +=  -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement \
			  -Wendif-labels -Wmissing-format-attribute -Wformat-security \
			  -fno-strict-aliasing -fwrapv \
			  -fPIC -fpic -msse4.2
## if build os is not darwin
else
libpeloton_la_LDFLAGS = -lm
libpeloton_la_CFLAGS +=  -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement \
			 -Wendif-labels -Wmissing-format-attribute -Wformat-security \
			 -fno-strict-aliasing -fwrapv -fexcess-precision=standard \
			 -fPIC -fpic -msse4.2
endif

######################################################################
# PELOTON
######################################################################

peloton_SOURCES = backend/main/main.cpp
peloton_LDFLAGS =
peloton_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES)
peloton_LDADD = libpeloton.la $(TBB_LIBS)

######################################################################
# THIRD-PARTY INCLUDES
######################################################################

third_party_INCLUDES = -I$(top_srcdir)/third_party 
