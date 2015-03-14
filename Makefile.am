## Makefile.am -- Process this file with automake to produce Makefile.in
AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS	 = -I m4

include $(top_srcdir)/doc/doxygen.am

# subdirectories of the project 
SUBDIRS = \
		  src \
		  third_party \
		  tests

# add files in the distribution
EXTRA_DIST = bootstrap $(top_srcdir)/doc/doxygen.cfg
