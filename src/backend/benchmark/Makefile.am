## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# BENCHMARK
######################################################################

######################################################################
# HYADAPT
######################################################################

hyadapt_SOURCES =  \
					backend/benchmark/hyadapt/hyadapt.cpp \
                    backend/benchmark/hyadapt/hyadapt_configuration.cpp \
                    backend/benchmark/hyadapt/hyadapt_workload.cpp \
                    backend/benchmark/hyadapt/hyadapt_loader.cpp

hyadapt_LDFLAGS =
hyadapt_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)  \
				   $(third_party_INCLUDES) \
				   -I$(srcdir)/backend/benchmark

hyadapt_LDADD = libpelotonpg.la libpeloton.la -lpthread

######################################################################
# LOGGER
######################################################################

logger_SOURCES =  \
					backend/benchmark/logger/logger.cpp \
                    backend/benchmark/logger/logger_configuration.cpp \
                    backend/benchmark/logger/logger_workload.cpp \
                    backend/benchmark/logger/logger_loader.cpp

logger_LDFLAGS =
logger_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)  \
				   $(third_party_INCLUDES) \
				   -I$(srcdir)/backend/benchmark

logger_LDADD = libpelotonpg.la libpeloton.la -lpthread

######################################################################
# YCSB
######################################################################

ycsb_SOURCES =  \
					backend/benchmark/ycsb/ycsb.cpp \
                    backend/benchmark/ycsb/ycsb_configuration.cpp \
                    backend/benchmark/ycsb/ycsb_workload.cpp \
                    backend/benchmark/ycsb/ycsb_loader.cpp

ycsb_LDFLAGS =
ycsb_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)  \
				   $(third_party_INCLUDES) \
				   -I$(srcdir)/backend/benchmark

ycsb_LDADD = libpelotonpg.la libpeloton.la -lpthread