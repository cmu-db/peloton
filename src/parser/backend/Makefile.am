## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# POSTGRES_BACKEND
######################################################################

postgres_backend_FILES = \
						 postgres/backend/libpq/pqformat.cpp \
						 postgres/backend/utils/mb/encnames.cpp \
						 postgres/backend/utils/mb/mbutils.cpp \
						 postgres/backend/utils/mb/wchar.cpp \
						 postgres/backend/utils/mmgr/aset.cpp \
						 postgres/backend/utils/mmgr/mcxt.cpp \
						 postgres/backend/utils/error/assert.cpp \
						 postgres/backend/utils/error/elog.cpp \
						 postgres/backend/utils/init/globals.cpp \
						 postgres/backend/utils/adt/datum.cpp \
						 postgres/backend/utils/adt/name.cpp \
						 postgres/backend/utils/adt/expandeddatum.cpp \
						 postgres/backend/parser/gram.cpp \
						 postgres/backend/parser/parser.cpp \
						 postgres/backend/parser/keywords.cpp \
						 postgres/backend/parser/kwlookup.cpp \
						 postgres/backend/parser/scansup.cpp \
						 postgres/backend/nodes/bitmapset.cpp \
						 postgres/backend/nodes/copyfuncs.cpp \
						 postgres/backend/nodes/equalfuncs.cpp \
						 postgres/backend/nodes/nodeFuncs.cpp \
						 postgres/backend/nodes/makefuncs.cpp \
						 postgres/backend/nodes/value.cpp \
						 postgres/backend/nodes/list.cpp \
						 postgres/backend/lib/stringinfo.cpp

postgres_backend_INCLUDES = \
							-I$(srcdir)/postgres/include \
							-I$(srcdir)/postgres/backend

