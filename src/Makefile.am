## Makefile.am -- Process this file with automake to produce Makefile.in

## Common flags
include $(top_srcdir)/common.am

######################################################################
# SUBDIRS
######################################################################

include $(srcdir)/backend/Makefile.am
include $(srcdir)/catalog/Makefile.am
include $(srcdir)/common/Makefile.am
include $(srcdir)/executor/Makefile.am
include $(srcdir)/expression/Makefile.am
include $(srcdir)/index/Makefile.am
include $(srcdir)/parser/Makefile.am
include $(srcdir)/planner/Makefile.am
include $(srcdir)/scheduler/Makefile.am
include $(srcdir)/storage/Makefile.am

include $(srcdir)/postgres/backend/Makefile.am
include $(srcdir)/postgres/bridge/Makefile.am
include $(srcdir)/postgres/common/Makefile.am
include $(srcdir)/postgres/port/Makefile.am
include $(srcdir)/postgres/timezone/Makefile.am

bin_PROGRAMS = peloton
lib_LTLIBRARIES= libpeloton.la libpostgres.la

DEFAULT_INCLUDES =

######################################################################
# PELOTON LIBRARY
######################################################################

libpeloton_la_SOURCES = \
                      $(backend_FILES) \
					  $(catalog_FILES) \
					  $(common_FILES) \
					  $(executor_FILES) \
					  $(expression_FILES) \
					  $(index_FILES) \
					  $(parser_FILES) \
					  $(planner_FILES) \
					  $(scheduler_FILES) \
					  $(storage_FILES)

libpeloton_la_LDFLAGS = $(TBB_LIBS) -pthread
libpeloton_la_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(third_party_INCLUDES)

######################################################################
# POSTGRES LIBRARY
######################################################################

libpostgres_la_SOURCES = \
					  $(postgres_backend_FILES) \
					  $(postgres_bridge_FILES) \
					  $(postgres_common_FILES) \
					  $(postgres_port_FILES) \
					  $(postgres_timezone_FILES)

libpostgres_la_LDFLAGS = -lrt -lcrypt -ldl -lm
libpostgres_la_CFLAGS =  -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement \
						 -Wendif-labels -Wmissing-format-attribute -Wformat-security \
						 -fno-strict-aliasing -fwrapv -fexcess-precision=standard \
						 -fPIC -fpic -msse4.2

libpostgres_la_CPPFLAGS = $(postgres_common_INCLUDES)

######################################################################
# PELOTON
######################################################################

peloton_SOURCES = backend/peloton.cpp
peloton_LDFLAGS = $(TBB_LIBS)
peloton_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) 
peloton_LDADD = libpeloton.la libpostgres.la

######################################################################
# THIRD-PARTY INCLUDES
######################################################################

third_party_INCLUDES = -I$(top_srcdir)/third_party 
