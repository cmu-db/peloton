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

bin_PROGRAMS = nstore
noinst_LTLIBRARIES= libnstore.la

######################################################################
# N-STORE SOURCES
######################################################################

common_SOURCES =  $(backend_FILES) \
				  $(catalog_FILES) \
				  $(common_FILES) \
				  $(executor_FILES) \
				  $(expression_FILES) \
				  $(index_FILES) \
				  $(parser_FILES) \
				  $(planner_FILES) \
				  $(scheduler_FILES) \
				  $(storage_FILES)

libnstore_la_SOURCES = $(common_SOURCES)

nstore_SOURCES = backend/nstore.cpp
nstore_LDFLAGS = $(AM_LDFLAGS)
nstore_LDADD = libnstore.la

######################################################################
# N-STORE INCLUDES
######################################################################

nstore_INCLUDES = $(backend_INCLUDES) \
				  $(catalog_INCLUDES) \
				  $(common_INCLUDES) \
				  $(executor_INCLUDES) \
				  $(expression_INCLUDES) \
				  $(index_INCLUDES) \
				  $(parser_INCLUDES) \
				  $(planner_INCLUDES) \
				  $(storage_INCLUDES) \
				  $(scheduler_INCLUDES) \
				  $(third_party_INCLUDES)

######################################################################
# THIRD-PARTY INCLUDES
######################################################################

third_party_INCLUDES = -I$(top_srcdir)/third_party 
