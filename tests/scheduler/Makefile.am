## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# SCHEDULER
######################################################################

check_PROGRAMS += scheduler_test

scheduler_test_SOURCES = scheduler/scheduler_test.cpp

scheduler_test_LDADD = \
					   $(obj_DIR)/catalog/libpeloton_la-manager.lo \
					   $(obj_DIR)/main/libpeloton_la-kernel.lo \
					   $(obj_DIR)/scheduler/libpeloton_la-scheduler.lo \
					   $(obj_DIR)/scheduler/libpeloton_la-traffic_cop.lo \
					   $(top_builddir)/src/libpeloton.la
