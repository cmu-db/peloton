## Makefile.am -- Process this file with automake to produce Makefile.in
 
######################################################################
# BENCHMARK
######################################################################

######################################################################
# HYADAPT
######################################################################
 
hyadapt_SOURCES =  \
					backend/benchmark/hyadapt/hyadapt.cpp \
                    backend/benchmark/hyadapt/configuration.cpp
                                        
hyadapt_LDFLAGS =
hyadapt_CPPFLAGS = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)  \
				   $(third_party_INCLUDES) \
				   -I$(srcdir)/backend/benchmark
 
hyadapt_LDADD = libpelotonpg.la libpeloton.la
