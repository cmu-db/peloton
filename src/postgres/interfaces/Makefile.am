## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# POSTGRES_INTERFACES
######################################################################

lib_LTLIBRARIES = libpq.la

libpq_la_SOURCES = \
				   postgres/interfaces/libpq/fe-print.c \
				   postgres/interfaces/libpq/fe-auth.c \
				   postgres/interfaces/libpq/fe-exec.c \
				   postgres/interfaces/libpq/pqexpbuffer.c \
				   postgres/interfaces/libpq/test/uri-regress.c \
				   postgres/interfaces/libpq/libpq-events.c \
				   postgres/interfaces/libpq/fe-secure.c \
				   postgres/interfaces/libpq/fe-misc.c \
				   postgres/interfaces/libpq/fe-protocol2.c \
				   postgres/interfaces/libpq/fe-connect.c \
				   postgres/interfaces/libpq/fe-protocol3.c \
				   postgres/interfaces/libpq/fe-lobj.c

libpq_la_CPPFLAGS = \
				   -I$(srcdir)/postgres/include \
				   -I$(srcdir)/postgres/interfaces/libpq \
				   -I$(srcdir)/postgres/backend \
				   -I$(srcdir)/postgres/port

				   
libpq_la_LDFLAGS = -lssl