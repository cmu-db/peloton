## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# POSTGRES_INTERFACES
######################################################################

lib_LTLIBRARIES = libpq.la

libpq_la_SOURCES = \
				   postgres/interfaces/libpq/fe-print.cpp \
				   postgres/interfaces/libpq/fe-auth.cpp \
				   postgres/interfaces/libpq/fe-exec.cpp \
				   postgres/interfaces/libpq/pqexpbuffer.cpp \
				   postgres/interfaces/libpq/test/uri-regress.cpp \
				   postgres/interfaces/libpq/libpq-events.cpp \
				   postgres/interfaces/libpq/fe-secure.cpp \
				   postgres/interfaces/libpq/fe-misc.cpp \
				   postgres/interfaces/libpq/fe-protocol2.cpp \
				   postgres/interfaces/libpq/fe-connect.cpp \
				   postgres/interfaces/libpq/fe-protocol3.cpp \
				   postgres/interfaces/libpq/fe-lobj.cpp

libpq_la_CPPFLAGS = \
				   -I$(srcdir)/postgres/include \
				   -I$(srcdir)/postgres/interfaces/libpq \
				   -I$(srcdir)/postgres/backend \
				   -I$(srcdir)/postgres/port


libpq_la_CXXFLAGS = -Wno-write-strings

libpq_la_LDFLAGS = -lssl
