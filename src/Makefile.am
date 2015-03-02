## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(nstore_INCLUDES) 
AM_CXXFLAGS = -std=c++11 -O0 -g3 -Wall -Wextra -fcolor-diagnostics 
AM_LDFLAGS  = -lboost_system 

######################################################################
# SUBDIRS
######################################################################
include $(srcdir)/backend/Makefile.am
include $(srcdir)/catalog/Makefile.am
include $(srcdir)/common/Makefile.am
include $(srcdir)/executor/Makefile.am
include $(srcdir)/parser/Makefile.am
include $(srcdir)/planner/Makefile.am
include $(srcdir)/storage/Makefile.am

bin_PROGRAMS = nstore

######################################################################
# N-STORE
######################################################################

nstore_SOURCES =  $(backend_FILES) \
				  $(catalog_FILES) \
				  $(common_FILES) \
				  $(executor_FILES) \
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
				  $(parser_INCLUDES) \
				  $(planner_INCLUDES) \
				  $(storage_INCLUDES) \
				  $(third_party_INCLUDES)

######################################################################
# THIRD-PARTY INCLUDES
######################################################################

third_party_INCLUDES = \
					   -I$(top_srcdir)/third_party 
