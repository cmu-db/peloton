## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# POSTGRES_BRIDGE
######################################################################

check_PROGRAMS += bridge_test

bridge_test_SOURCES = postgres/bridge/bridge_test.cpp

bridge_test_LDADD = -L$(top_builddir)/src -lpostgres -lpq
