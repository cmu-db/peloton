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
include $(srcdir)/index/Makefile.am
include $(srcdir)/parser/Makefile.am
include $(srcdir)/planner/Makefile.am
include $(srcdir)/storage/Makefile.am

bin_PROGRAMS = nstore

######################################################################
# N-STORE SOURCES
######################################################################

nstore_SOURCES =  $(backend_FILES) \
				  $(catalog_FILES) \
				  $(common_FILES) \
				  $(executor_FILES) \
				  $(index_FILES) \
				  $(planner_FILES) \
				  $(parser_FILES) \
				  $(planner_FILES) \
				  $(storage_FILES)

######################################################################
# N-STORE INCLUDES
######################################################################

nstore_INCLUDES = $(backend_INCLUDES) \
				  $(catalog_INCLUDES) \
				  $(common_INCLUDES) \
				  $(executor_INCLUDES) \
				  $(index_INCLUDES) \
				  $(parser_INCLUDES) \
				  $(planner_INCLUDES) \
				  $(storage_INCLUDES) \
				  $(third_party_INCLUDES)

######################################################################
# THIRD-PARTY INCLUDES
######################################################################

third_party_INCLUDES = -I$(top_srcdir)/third_party 
