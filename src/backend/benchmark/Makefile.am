## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# BENCHMARK
######################################################################

benchmark_common_cppflags = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)  \
            				$(third_party_INCLUDES) \
			            	-I$(srcdir)/backend/benchmark


benchmark_common_ldadd = libpelotonpg.la libpeloton.la -lpthread

######################################################################
# HYADAPT
######################################################################

hyadapt_SOURCES =  \
					backend/benchmark/hyadapt/hyadapt.cpp \
                    backend/benchmark/hyadapt/hyadapt_configuration.cpp \
                    backend/benchmark/hyadapt/hyadapt_workload.cpp \
                    backend/benchmark/hyadapt/hyadapt_loader.cpp

hyadapt_LDFLAGS =
hyadapt_CPPFLAGS = $(benchmark_common_cppflags)
hyadapt_LDADD = $(benchmark_common_ldadd)

######################################################################
# LOGGER
######################################################################

logger_SOURCES =  \
					backend/benchmark/logger/logger.cpp \
                    backend/benchmark/logger/logger_configuration.cpp \
                    backend/benchmark/logger/logger_workload.cpp \
                    backend/benchmark/logger/logger_loader.cpp \
                    backend/benchmark/ycsb/ycsb_configuration.cpp \
                    backend/benchmark/ycsb/ycsb_workload.cpp \
                    backend/benchmark/ycsb/ycsb_loader.cpp

logger_LDFLAGS =
logger_CPPFLAGS = $(benchmark_common_cppflags)
logger_LDADD = $(benchmark_common_ldadd)

######################################################################
# YCSB
######################################################################

ycsb_SOURCES =  \
					backend/benchmark/ycsb/ycsb.cpp \
                    backend/benchmark/ycsb/ycsb_configuration.cpp \
                    backend/benchmark/ycsb/ycsb_workload.cpp \
                    backend/benchmark/ycsb/ycsb_loader.cpp

ycsb_LDFLAGS =
ycsb_CPPFLAGS = $(benchmark_common_cppflags)
ycsb_LDADD = $(benchmark_common_ldadd)

######################################################################
# TPCC
######################################################################

tpcc_SOURCES =  \
					backend/benchmark/tpcc/tpcc.cpp \
                    backend/benchmark/tpcc/tpcc_configuration.cpp \
                    backend/benchmark/tpcc/tpcc_workload.cpp \
                    backend/benchmark/tpcc/tpcc_loader.cpp

tpcc_LDFLAGS =
tpcc_CPPFLAGS = $(benchmark_common_cppflags)
tpcc_LDADD = $(benchmark_common_ldadd)
