## Makefile.am -- Process this file with automake to produce Makefile.in
AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS	 = -I m4

include $(top_srcdir)/doc/doxygen.am
include $(top_srcdir)/third_party/gmock/Makefile.am
include $(top_srcdir)/third_party/libnanomsg/Makefile.am

# subdirectories of the project 
SUBDIRS = \
		  src \
		  tests \
		  tools 

# add files in the distribution
EXTRA_DIST = bootstrap $(DX_CONFIG) doc 

clean-local:
	find . -type f -name "*.gcno" -delete; find . -type f -name "*.gcda" -delete
