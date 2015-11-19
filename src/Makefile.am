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
include $(srcdir)/backend/message/Makefile.am
include $(srcdir)/backend/planner/Makefile.am
include $(srcdir)/backend/storage/Makefile.am
include $(srcdir)/backend/logging/Makefile.am
include $(srcdir)/backend/benchmark/Makefile.am

include $(srcdir)/postgres/backend/Makefile.am
include $(srcdir)/postgres/common/Makefile.am
include $(srcdir)/postgres/interfaces/Makefile.am
include $(srcdir)/postgres/port/Makefile.am
include $(srcdir)/postgres/timezone/Makefile.am

include $(top_srcdir)/third_party/Makefile.am

bin_peloton_PROGRAMS = peloton hyadapt ycsb tpcc

bin_pelotondir = /usr/local/peloton/bin

# The order matters here

lib_LTLIBRARIES += libpeloton.la
lib_LTLIBRARIES += libpelotonpg.la

DEFAULT_INCLUDES =

######################################################################
# PELOTON LIBRARY
######################################################################

libpeloton_la_SOURCES = \
			$(expression_FILES) \
			$(brain_FILES) \
			$(catalog_FILES) \
			$(backend_FILES) \
			$(bridge_FILES) \
			$(common_FILES) \
			$(concurrency_FILES) \
			$(executor_FILES) \
			$(index_FILES) \
			$(message_FILES) \
			$(planner_FILES) \
			$(storage_FILES) \
			$(logging_FILES)

## Use different compiler flags for Peloton and pelotonpg.

######################################################################
# LIBPELOTON
######################################################################

libpeloton_la_CPPFLAGS = $(AM_CPPFLAGS) \
						 -I$(srcdir)/postgres/include \
						 -I$(srcdir)/postgres/backend \
						 -I$(srcdir)/ \
						 -I. -I$(top_srcdir)/src -I.. \
						 -I /opt/local/include \
						 $(third_party_INCLUDES)

libpeloton_la_CXXFLAGS = $(AM_CXXFLAGS)

libpeloton_la_LDFLAGS = -lm -lvmem

######################################################################
# LIBPELOTONPG
######################################################################

libpelotonpg_la_SOURCES = \
			$(postgres_backend_FILES) \
			$(postgres_common_FILES) \
			$(postgres_port_FILES) \
			$(postgres_timezone_FILES) \
			$(third_party_FILES)

libpelotonpg_la_CPPFLAGS = $(AM_CPPFLAGS) \
						 -I$(srcdir)/postgres/include \
						 -I$(srcdir)/postgres/backend \
						 -I$(srcdir)/ \
						 -I. -I$(top_srcdir)/src -I.. \
						 $(third_party_INCLUDES)

libpelotonpg_la_CXXFLAGS = $(AM_CXXFLAGS) \
			  -Wno-unused-variable -Wno-unused-but-set-variable \
			  -Wno-unused-parameter -Wno-missing-field-initializers \
			  -Wno-array-bounds -Wno-clobbered -Wno-strict-overflow \
			  -Wno-sign-compare -Wno-write-strings \
			  -msse4.2

######################################################################
# PELOTON
######################################################################

peloton_SOURCES = backend/main/main.cpp
peloton_LDFLAGS =
peloton_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)

peloton_LDADD = libpelotonpg.la libpeloton.la


