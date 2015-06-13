## Makefile.am -- Process this file with automake to produce Makefile.in

## Common flags
include $(top_srcdir)/common.am

######################################################################
# SUBDIRS
######################################################################

bin_peloton_PROGRAMS = initdb pg_ctl psql \
			   createdb createuser dropdb dropuser             

bin_pelotondir = /usr/local/peloton/bin

pg_common_ldflags = -L$(top_builddir)/src -lpostgres -lpq -lrt -lm -lreadline

######################################################################
# INITDB
######################################################################

initdb_SOURCES = \
				 common/fe_memutils.c \
				 pg_initdb/findtimezone.c \
				 pg_initdb/initdb.c

initdb_CPPFLAGS = \
				  -DFRONTEND \
				  -I$(top_srcdir)/src/postgres/include \
				  -I$(top_srcdir)/src/postgres/timezone

initdb_LDFLAGS = $(pg_common_ldflags)

######################################################################
# PSQL
######################################################################

psql_SOURCES = \
			   common/dumputils.c \
			   common/fe_memutils.c \
			   common/keywords.c \
			   pg_psql/command.c \
			   pg_psql/common.c \
			   pg_psql/copy.c \
			   pg_psql/describe.c \
			   pg_psql/help.c \
			   pg_psql/input.c \
			   pg_psql/large_obj.c \
			   pg_psql/mainloop.c \
			   pg_psql/mbprint.c \
			   pg_psql/print.c \
			   pg_psql/prompt.c \
			   pg_psql/sql_help.c \
			   pg_psql/startup.c \
			   pg_psql/stringutils.c \
			   pg_psql/tab-complete.c \
			   pg_psql/variables.c

psql_CPPFLAGS = \
				-DFRONTEND \
				-I. \
				-I$(top_srcdir)/tools/common \
				-I$(top_srcdir)/src/postgres/include \
				-I$(top_srcdir)/src/postgres/interfaces/libpq

psql_LDFLAGS = $(pg_common_ldflags)

######################################################################
# PSQL & SCRIPTS
######################################################################

pg_scripts_CPPFLAGS = \
					-DFRONTEND \
				    -I$(top_srcdir)/tools/common \
					-I$(top_srcdir)/src/postgres/include \
					-I$(top_srcdir)/src/postgres/interfaces/libpq

pg_scripts_SOURCES = \
                    common/dumputils.c \
			        common/fe_memutils.c \
			        common/keywords.c

pg_ctl_SOURCES = \
                 $(pg_scripts_SOURCES) \
				 scripts/pg_ctl.c

pg_ctl_CPPFLAGS = $(pg_scripts_CPPFLAGS)

pg_ctl_LDFLAGS = $(pg_common_ldflags)

createdb_SOURCES = \
				   $(pg_scripts_SOURCES) \
				   scripts/common.c \
				   scripts/createdb.c

createdb_CPPFLAGS = $(pg_scripts_CPPFLAGS)

createdb_LDFLAGS = $(pg_common_ldflags)

createuser_SOURCES = \
					 $(pg_scripts_SOURCES) \
					 scripts/common.c \
					 scripts/createuser.c

createuser_CPPFLAGS = $(pg_scripts_CPPFLAGS)

createuser_LDFLAGS = $(pg_common_ldflags)

dropdb_SOURCES = \
				 $(pg_scripts_SOURCES) \
				 scripts/common.c \
				 scripts/dropdb.c

dropdb_CPPFLAGS = $(pg_scripts_CPPFLAGS)

dropdb_LDFLAGS = $(pg_common_ldflags)

dropuser_SOURCES = \
				   $(pg_scripts_SOURCES) \
				   scripts/common.c \
				   scripts/dropuser.c

dropuser_CPPFLAGS = $(pg_scripts_CPPFLAGS)

dropuser_LDFLAGS = $(pg_common_ldflags)

######################################################################
# PELOTON CONFIGURATION
######################################################################

install-data-hook:
	cp -rf $(top_srcdir)/tools/peloton /usr/local
	ln -sf $(top_builddir)/../src/peloton $(top_builddir)/tools/.libs/
