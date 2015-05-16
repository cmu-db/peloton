## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# SCHEDULER
######################################################################

check_PROGRAMS += scheduler_test

scheduler_test_SOURCES = scheduler/scheduler_test.cpp

scheduler_test_LDADD = \
					   $(obj_DIR)/catalog/manager.o \
					   $(obj_DIR)/backend/kernel.o \
					   $(obj_DIR)/scheduler/scheduler.o \
					   $(obj_DIR)/scheduler/traffic_cop.o \
					   $(top_builddir)/src/libnstore.la			   				   
