root_dir := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

TARGET = pg_query
ARLIB = lib$(TARGET).a
PGDIR = $(root_dir)/postgres
PGDIRBZ2 = $(root_dir)/postgres.tar.bz2

PG_VERSION = 9.4.5

OBJS = pg_query.o \
pg_query_parse.o \
pg_query_normalize.o \
pg_polyfills.o

PGOBJS = $(PGDIR)/src/backend/utils/mb/wchar.o \
$(PGDIR)/src/backend/libpq/pqformat.o \
$(PGDIR)/src/backend/utils/mb/encnames.o \
$(PGDIR)/src/backend/utils/mb/mbutils.o \
$(PGDIR)/src/backend/utils/mmgr/mcxt.o \
$(PGDIR)/src/backend/utils/mmgr/aset.o \
$(PGDIR)/src/backend/utils/error/elog.o \
$(PGDIR)/src/backend/utils/error/assert.o \
$(PGDIR)/src/backend/utils/init/globals.o \
$(PGDIR)/src/backend/utils/adt/datum.o \
$(PGDIR)/src/backend/utils/adt/name.o \
$(PGDIR)/src/backend/parser/gram.o \
$(PGDIR)/src/backend/parser/parser.o \
$(PGDIR)/src/backend/parser/keywords.o \
$(PGDIR)/src/backend/parser/kwlookup.o \
$(PGDIR)/src/backend/parser/scansup.o \
$(PGDIR)/src/backend/nodes/bitmapset.o \
$(PGDIR)/src/backend/nodes/copyfuncs.o \
$(PGDIR)/src/backend/nodes/equalfuncs.o \
$(PGDIR)/src/backend/nodes/nodeFuncs.o \
$(PGDIR)/src/backend/nodes/makefuncs.o \
$(PGDIR)/src/backend/nodes/value.o \
$(PGDIR)/src/backend/nodes/list.o \
$(PGDIR)/src/backend/lib/stringinfo.o \
$(PGDIR)/src/port/qsort.o \
$(PGDIR)/src/common/psprintf.o

ALL_OBJS = $(OBJS) $(PGOBJS)

CFLAGS   = -I $(PGDIR)/src/include -I $(PGDIR)/src/timezone -O2 -Wall -Wmissing-prototypes -Wpointer-arith \
-Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute \
-Wformat-security -fno-strict-aliasing -fwrapv -fPIC -g
INCFLAGS = -I.
LIBPATH  = -L.

ifeq ($(JSON_OUTPUT_V2),1)
	CFLAGS += -D JSON_OUTPUT_V2
	OBJS += pg_query_json.o
else
	PGOBJS += $(PGDIR)/src/backend/nodes/outfuncs_json.o
endif

CLEANLIBS = $(ARLIB)
CLEANOBJS = *.o
CLEANFILES = $(PGDIRBZ2)

AR = ar rs
RM = rm -f
ECHO = echo

CC ?= cc

all: $(ARLIB)

clean:
	-@ $(RM) $(CLEANLIBS) $(CLEANOBJS) $(CLEANFILES) $(EXAMPLES)
	-@ $(RM) -r $(PGDIR)

.PHONY: all clean examples

$(PGDIR): $(PGDIRBZ2)
	tar -xjf $(PGDIRBZ2)
	mv $(root_dir)/postgresql-$(PG_VERSION) $(PGDIR)
	if [ "$$JSON_OUTPUT_V2" != "1" ] ; then \
		cd $(PGDIR) && patch -p1 < $(root_dir)/patches/01_output_nodes_as_json.patch ; \
	fi
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/02_parse_replacement_char.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/03_regenerate_bison_flex_files.patch
	cd $(PGDIR); CFLAGS=-fPIC ./configure -q --without-readline --without-zlib --enable-cassert --enable-debug
	cd $(PGDIR); make -C src/backend lib-recursive
	cd $(PGDIR); make -C src/backend/libpq pqformat.o
	cd $(PGDIR); make -C src/backend/utils/mb wchar.o encnames.o mbutils.o
	cd $(PGDIR); make -C src/backend/utils/mmgr mcxt.o aset.o
	cd $(PGDIR); make -C src/backend/utils/error elog.o assert.o
	cd $(PGDIR); make -C src/backend/utils/init globals.o
	cd $(PGDIR); make -C src/backend/utils/adt datum.o name.o
	cd $(PGDIR); make -C src/backend/parser gram.o parser.o keywords.o kwlookup.o scansup.o
	cd $(PGDIR); make -C src/backend/nodes bitmapset.o copyfuncs.o equalfuncs.o nodeFuncs.o makefuncs.o value.o list.o
	if [ "$$JSON_OUTPUT_V2" != "1" ] ; then \
		cd $(PGDIR) && make -C src/backend/nodes outfuncs_json.o ; \
	fi
	cd $(PGDIR); make -C src/backend/lib stringinfo.o
	cd $(PGDIR); make -C src/port qsort.o
	cd $(PGDIR); make -C src/common psprintf.o

$(PGDIRBZ2):
	curl -o $(PGDIRBZ2) https://ftp.postgresql.org/pub/source/v$(PG_VERSION)/postgresql-$(PG_VERSION).tar.bz2

.c.o: $(PGDIR)
	@$(ECHO) compiling $(<)
	@$(CC) $(INCFLAGS) $(CPPFLAGS) $(CFLAGS) -o $@ -c $<

$(ARLIB): $(PGDIR) $(OBJS) Makefile
	@$(AR) $@ $(ALL_OBJS)

EXAMPLES = examples/simple examples/normalize examples/simple_error examples/normalize_error

examples: $(EXAMPLES)
	examples/simple
	examples/normalize
	examples/simple_error
	examples/normalize_error

examples/simple: examples/simple.c $(ARLIB)
	$(CC) -I. -L. -o $@ -g examples/simple.c $(ARLIB)

examples/normalize: examples/normalize.c $(ARLIB)
	$(CC) -I. -L. -o $@ -g examples/normalize.c $(ARLIB)

examples/simple_error: examples/simple_error.c $(ARLIB)
	$(CC) -I. -L. -o $@ -g examples/simple_error.c $(ARLIB)

examples/normalize_error: examples/normalize_error.c $(ARLIB)
	$(CC) -I. -L. -o $@ -g examples/normalize_error.c $(ARLIB)
